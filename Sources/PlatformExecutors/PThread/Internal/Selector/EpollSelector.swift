//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift.org open source project
//
// Copyright (c) 2025 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
// See https://swift.org/CONTRIBUTORS.txt for the list of Swift project authors
//
//===----------------------------------------------------------------------===//
//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2017-2024 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

#if canImport(Glibc)
import Glibc
import CPlatformExecutors

/// A monotonic per-fd registration ID, mirrored from
/// `swift-nio`'s `SelectorRegistrationID` (see
/// `NIOPosix/SelectorGeneric.swift:409`).
///
/// `EpollSelector` allocates a fresh ID at each ``EpollSelector/registerIO(fd:)``
/// and writes it into the kernel-event payload alongside the file descriptor
/// (`epoll_event.data.u64`). On every event the dispatch loop validates
/// `event.registrationID == ioRegistrations[fd]?.registrationID` and discards
/// stale events for closed-and-reused descriptors. This is the load-bearing
/// invariant for fd-reuse safety; without it, an in-flight epoll event for a
/// closed fd can resume the wrong continuation when the kernel hands the same
/// integer back for a new socket.
///
/// Wraparound is acceptable — the disambiguation window is only in-flight
/// events between `deregisterIO(fd)` and the next `registerIO(fd) → same int`.
/// `.max` is reserved for the selector's internal eventfd / timerfds; the
/// allocator never hands it out for I/O fds.
@available(macOS 14.0, iOS 17.0, watchOS 10.0, tvOS 17.0, *)
struct SelectorRegistrationID: Hashable, Sendable {
  var rawValue: UInt32

  init(rawValue: UInt32) {
    self.rawValue = rawValue
  }

  /// The reserved ID for the selector's internal fds (eventfd, timerfds).
  static let reservedForInternalFDs = SelectorRegistrationID(rawValue: .max)
}

extension SelectorRegistrationID {
  /// Allocates the next ID, skipping `.max` (reserved).
  fileprivate static func nextID(_ counter: inout UInt32) -> SelectorRegistrationID {
    let issued = counter
    counter &+= 1
    if counter == UInt32.max {
      // .max is reserved for the selector's internal fds — skip past it.
      counter &+= 1
    }
    return SelectorRegistrationID(rawValue: issued)
  }
}

/// A selector that uses epoll for eventing
@available(macOS 14.0, iOS 17.0, watchOS 10.0, tvOS 17.0, *)
struct EpollSelector: ~Copyable {
  /// User data supports (un)packing into an `UInt64` because epoll has a user info field that we can attach which is
  /// up to 64 bits wide. We're using all of those 64 bits, 32 for a "registration ID" and 32 for the file handle.
  struct UserData {
    var registrationID: SelectorRegistrationID
    var fileHandle: CInt

    init(registrationID: SelectorRegistrationID, fileHandle: CInt) {
      assert(MemoryLayout<UInt64>.size == MemoryLayout<UserData>.size)
      self.registrationID = registrationID
      self.fileHandle = fileHandle
    }

    init(rawValue: UInt64) {
      let unpacked = IntegerBitPacking.unpackUInt32CInt(rawValue)
      self = .init(
        registrationID: SelectorRegistrationID(rawValue: unpacked.0),
        fileHandle: unpacked.1
      )
    }
  }

  /// A pending I/O submission against a registered fd.
  ///
  /// Submissions queue per-fd / per-direction in
  /// ``IORegistration/pendingReads`` / ``IORegistration/pendingWrites``.
  /// On readiness, the head of the matching queue is dequeued and its
  /// continuation is resumed via the public ``IOError`` channel.
  struct PendingIO {
    /// A monotonic per-`PThreadExecutor` submission id. Used by the
    /// task-cancellation path to find this exact submission inside its
    /// per-fd queue, even when several submissions share the fd/direction.
    var submissionID: UInt64
    var operation: LoweredIOOperation
    var continuation: UnsafeContinuation<IOOperationResult, IOError>
  }

  /// Per-fd registration state.
  ///
  /// `registrationID` is the fd-reuse disambiguator (see
  /// ``SelectorRegistrationID``). `pendingReads`/`pendingWrites` are FIFO
  /// queues — multiple concurrent reads (or writes) on the same fd
  /// complete in submission order, fixing the M1-S1.0/F3 single-slot
  /// overwrite hazard.
  struct IORegistration {
    var registrationID: SelectorRegistrationID
    var pendingReads: ContiguousArray<PendingIO> = []
    var pendingWrites: ContiguousArray<PendingIO> = []
  }

  /// The selector file descriptor.
  fileprivate var selectorFD: CInt
  /// The event file descriptor to wake the thread when a new job is enqueued.
  fileprivate let eventFD: CInt
  /// The monotonic timer file descriptor to back the suspending clock.
  fileprivate let monotonicTimerFD: CInt
  /// The boottime timer file descriptor to back the suspending clock.
  fileprivate let boottimeTimerFD: CInt
  /// The next continuous clock timer to avoid re-arming the timer if possible.
  fileprivate var nextMonotonicClockTimer: ContinuousClock.Instant?
  /// The next suspending clock timer to avoid re-arming the timer if possible.
  fileprivate var nextBoottimelockTimer: SuspendingClock.Instant?
  /// Registered I/O file descriptors and their pending operations.
  var ioRegistrations: [CInt: IORegistration] = [:]
  /// Allocator state for ``SelectorRegistrationID``.
  fileprivate var nextRegistrationIDCounter: UInt32 = 0

  init() throws {
    // We try! all of these since if the creation fails there is nothing we can do to recover.
    self.selectorFD = try! Epoll.epoll_create(size: 128)
    self.eventFD = try! EventFileDescriptor.makeEventFileDescriptor(
      initval: 0,
      flags: Int32(EventFileDescriptor.EFD_CLOEXEC | EventFileDescriptor.EFD_NONBLOCK)
    )
    self.monotonicTimerFD = try! TimerFileDescriptor.timerfd_create(
      clockId: CLOCK_MONOTONIC,
      flags: Int32(TimerFileDescriptor.TFD_CLOEXEC | TimerFileDescriptor.TFD_NONBLOCK)
    )
    self.boottimeTimerFD = try! TimerFileDescriptor.timerfd_create(
      clockId: CLOCK_BOOTTIME,
      flags: Int32(TimerFileDescriptor.TFD_CLOEXEC | TimerFileDescriptor.TFD_NONBLOCK)
    )

    var ev = Epoll.epoll_event()
    ev.events = Epoll.EPOLLERR | Epoll.EPOLLHUP | Epoll.EPOLLIN
    ev.data.u64 = UInt64(
      UserData(
        registrationID: .reservedForInternalFDs,
        fileHandle: self.eventFD
      )
    )
    try Epoll.epoll_ctl(
      epfd: self.selectorFD,
      op: Epoll.EPOLL_CTL_ADD,
      fd: self.eventFD,
      event: &ev
    )

    var monotonicTimerev = Epoll.epoll_event()
    monotonicTimerev.events = Epoll.EPOLLIN | Epoll.EPOLLERR | Epoll.EPOLLRDHUP
    monotonicTimerev.data.u64 = UInt64(
      UserData(
        registrationID: .reservedForInternalFDs,
        fileHandle: self.monotonicTimerFD
      )
    )
    try Epoll.epoll_ctl(
      epfd: self.selectorFD,
      op: Epoll.EPOLL_CTL_ADD,
      fd: self.monotonicTimerFD,
      event: &monotonicTimerev
    )

    var boottimeTimerev = Epoll.epoll_event()
    boottimeTimerev.events = Epoll.EPOLLIN | Epoll.EPOLLERR | Epoll.EPOLLRDHUP
    boottimeTimerev.data.u64 = UInt64(
      UserData(
        registrationID: .reservedForInternalFDs,
        fileHandle: self.boottimeTimerFD
      )
    )
    try Epoll.epoll_ctl(
      epfd: self.selectorFD,
      op: Epoll.EPOLL_CTL_ADD,
      fd: self.boottimeTimerFD,
      event: &boottimeTimerev
    )
  }

  deinit {
    // We try! all of the closes because close can only fail in the following ways:
    // - EINTR, which we eat in close
    // - EIO, which can only happen for on-disk files
    // - EBADF, which can't happen here because we would crash as EBADF is marked unacceptable
    // Therefore, we assert here that close will always succeed and if not, that's a bug we need to know
    // about.
    try! close(descriptor: self.boottimeTimerFD)
    try! close(descriptor: self.monotonicTimerFD)
    try! close(descriptor: self.eventFD)
    try! close(descriptor: self.selectorFD)
  }

  /// Blocks until the wakeup is called.
  mutating func whenReady(
    strategy: SelectorStrategy
  ) throws {
    // Handle up to 64 events: 3 internal + external I/O FDs
    let maxEvents = 64

    try withUnsafeTemporaryAllocation(of: Epoll.epoll_event.self, capacity: maxEvents) { eventsPointer in
      let readyEvents: Int
      switch strategy {
      case .now:
        readyEvents = Int(
          try Epoll.epoll_wait(
            epfd: self.selectorFD,
            events: eventsPointer.baseAddress!,
            maxevents: Int32(maxEvents),
            timeout: 0
          )
        )
      case .blockUntilTimeout(let continuousClockInstant, let suspendingClockInstant):
        // The continuous clock maps to the boottime clock
        func setTimer(instant: ContinuousClock.Instant) throws {
          var ts = itimerspec()
          ts.it_value = timespec(duration: ContinuousClock.now.duration(to: instant))
          try TimerFileDescriptor.timerfd_settime(fd: self.boottimeTimerFD, flags: 0, newValue: &ts, oldValue: nil)
        }
        // The suspending clock maps to the monotonic clock
        func setTimer(instant: SuspendingClock.Instant) throws {
          var ts = itimerspec()
          ts.it_value = timespec(duration: SuspendingClock.now.duration(to: instant))
          try TimerFileDescriptor.timerfd_settime(fd: self.monotonicTimerFD, flags: 0, newValue: &ts, oldValue: nil)
        }
        // Only call timerfd_settime if we're not already scheduled one that will cover it.
        if let continuousClockInstant {
          if let nextMonotonicClockTimer = self.nextMonotonicClockTimer {
            if continuousClockInstant < nextMonotonicClockTimer {
              try setTimer(instant: continuousClockInstant)
            }
          } else {
            try setTimer(instant: continuousClockInstant)
          }
        }

        // Only call timerfd_settime if we're not already scheduled one that will cover it.
        if let suspendingClockInstant {
          if let nextBoottimelockTimer = self.nextBoottimelockTimer {
            if suspendingClockInstant < nextBoottimelockTimer {
              try setTimer(instant: suspendingClockInstant)
            }
          } else {
            try setTimer(instant: suspendingClockInstant)
          }
        }
        fallthrough

      case .block:
        readyEvents = Int(
          try Epoll.epoll_wait(
            epfd: self.selectorFD,
            events: eventsPointer.baseAddress!,
            maxevents: Int32(maxEvents),
            timeout: -1  // Specifying -1 blocks until a file descriptor becomes ready
          )
        )
      }

      for i in 0..<readyEvents {
        let ev = eventsPointer[i]
        let epollUserData = UserData(rawValue: ev.data.u64)
        let fd = epollUserData.fileHandle
        let eventRegistrationID = epollUserData.registrationID
        switch fd {
        case self.eventFD:
          // Consume event
          var val = EventFileDescriptor.eventfd_t()
          _ = try EventFileDescriptor.eventfd_read(fd: self.eventFD, value: &val)
        case self.monotonicTimerFD:
          // Consume event
          var val: UInt64 = 0
          // We are not interested in the result
          _ = try! TimerFileDescriptor.timerfd_read(
            descriptor: self.monotonicTimerFD,
            pointer: &val,
            size: MemoryLayout.size(ofValue: val)
          )

          // Processed the earliest set timer so reset it.
          self.nextMonotonicClockTimer = nil
        case self.boottimeTimerFD:
          // Consume event
          var val: UInt64 = 0
          // We are not interested in the result
          _ = try! TimerFileDescriptor.timerfd_read(
            descriptor: self.boottimeTimerFD,
            pointer: &val,
            size: MemoryLayout.size(ofValue: val)
          )

          // Processed the earliest set timer so reset it.
          self.nextBoottimelockTimer = nil
        default:
          guard let registration = self.ioRegistrations[fd] else {
            // The fd was deregistered while events were in flight; ignore.
            continue
          }
          // fd-reuse safety: discard events for a stale generation.
          // See `SelectorRegistrationID` and SNW-0001 §"fd-reuse safety".
          guard registration.registrationID == eventRegistrationID else {
            continue
          }

          let isReadable = ev.events & Epoll.EPOLLIN != 0
            || ev.events & Epoll.EPOLLHUP != 0
            || ev.events & Epoll.EPOLLERR != 0
          let isWritable = ev.events & Epoll.EPOLLOUT != 0

          if isReadable {
            self.completeNextOperation(fd: fd, direction: .read)
          }
          if isWritable {
            self.completeNextOperation(fd: fd, direction: .write)
          }
        }
      }
    }
  }

  /// Wakes up the selector.
  func wakeup() throws {
    _ = try EventFileDescriptor.eventfd_write(fd: self.eventFD, value: 1)
  }

  // MARK: - I/O operation submission

  /// Registers a file descriptor for I/O event monitoring.
  ///
  /// Allocates a fresh ``SelectorRegistrationID`` for this `(fd,
  /// generation)` and writes it into the kernel-event payload. Subsequent
  /// `EPOLL_CTL_MOD` calls for this fd carry the same ID until
  /// ``deregisterIO(fd:)`` issues a new generation.
  mutating func registerIO(fd: CInt) throws {
    let id = SelectorRegistrationID.nextID(&self.nextRegistrationIDCounter)
    var ev = Epoll.epoll_event()
    ev.events = Epoll.EPOLLET
    ev.data.u64 = UInt64(UserData(registrationID: id, fileHandle: fd))
    try Epoll.epoll_ctl(
      epfd: self.selectorFD,
      op: Epoll.EPOLL_CTL_ADD,
      fd: fd,
      event: &ev
    )
    ioRegistrations[fd] = IORegistration(registrationID: id)
  }

  /// Submits an I/O operation for completion.
  ///
  /// Appends the submission to the per-fd / per-direction queue and re-arms
  /// the epoll interest mask to include the relevant filter (`EPOLLIN` for
  /// read/accept/close-drain, `EPOLLOUT` for write/connect). When the fd
  /// becomes ready, ``whenReady(strategy:)`` pops the head of the matching
  /// queue and invokes the completion syscall on the executor thread,
  /// resuming the continuation with the result.
  ///
  /// Fail-by-resume: if the `epoll_ctl(MOD)` fails, the entry is dequeued
  /// and the continuation resumes with the typed POSIX error rather than
  /// trapping at the call site (M1-S1.0/F4).
  mutating func submit(
    _ operation: LoweredIOOperation,
    submissionID: UInt64,
    continuation: UnsafeContinuation<IOOperationResult, IOError>
  ) {
    let fd = operation.fileHandle
    let direction = operation.direction

    guard var registration = ioRegistrations[fd] else {
      // Submitted against a non-registered fd. Surface a typed POSIX error.
      continuation.resume(throwing: .posix(errno: EBADF, operation: operation.kind))
      return
    }

    let pending = PendingIO(
      submissionID: submissionID,
      operation: operation,
      continuation: continuation
    )

    switch direction {
    case .read:
      registration.pendingReads.append(pending)
    case .write:
      registration.pendingWrites.append(pending)
    }
    ioRegistrations[fd] = registration

    do {
      try self.armInterestMask(fd: fd, registration: registration)
    } catch let err as POSIXError {
      // Fail-by-resume — pop the entry we just pushed and resume with the
      // typed error instead of trapping inside the continuation closure.
      if var rollback = ioRegistrations[fd] {
        switch direction {
        case .read:
          if !rollback.pendingReads.isEmpty {
            _ = rollback.pendingReads.removeLast()
          }
        case .write:
          if !rollback.pendingWrites.isEmpty {
            _ = rollback.pendingWrites.removeLast()
          }
        }
        ioRegistrations[fd] = rollback
      }
      continuation.resume(throwing: .posix(errno: err.errnoCode, operation: operation.kind))
    } catch {
      continuation.resume(throwing: .backend(error))
    }
  }

  /// Cancels a pending submission identified by `submissionID`.
  ///
  /// Walks the per-fd queue in `direction`, removes the matching entry,
  /// and resumes its continuation with
  /// ``IOError/cancelled(transferred:)``. Returns `true` if a matching
  /// entry was found and resumed; `false` if the submission had already
  /// completed (lost the cancel race).
  @discardableResult
  mutating func cancelSubmission(
    fd: CInt,
    direction: PendingDirection,
    submissionID: UInt64
  ) -> Bool {
    guard var registration = ioRegistrations[fd] else { return false }
    let removed: PendingIO?
    switch direction {
    case .read:
      if let idx = registration.pendingReads.firstIndex(where: { $0.submissionID == submissionID }) {
        removed = registration.pendingReads.remove(at: idx)
      } else {
        removed = nil
      }
    case .write:
      if let idx = registration.pendingWrites.firstIndex(where: { $0.submissionID == submissionID }) {
        removed = registration.pendingWrites.remove(at: idx)
      } else {
        removed = nil
      }
    }
    ioRegistrations[fd] = registration
    if let removed {
      removed.continuation.resume(throwing: .cancelled(transferred: 0))
      return true
    }
    return false
  }

  /// Builds and applies the epoll interest mask reflecting `registration`'s
  /// current per-fd queues. `EPOLLIN` is set if `pendingReads` is non-empty,
  /// `EPOLLOUT` if `pendingWrites` is non-empty.
  private mutating func armInterestMask(
    fd: CInt,
    registration: IORegistration
  ) throws {
    var ev = Epoll.epoll_event()
    ev.events = Epoll.EPOLLET | Epoll.EPOLLHUP | Epoll.EPOLLERR
    if !registration.pendingReads.isEmpty {
      ev.events |= Epoll.EPOLLIN
    }
    if !registration.pendingWrites.isEmpty {
      ev.events |= Epoll.EPOLLOUT
    }
    ev.data.u64 = UInt64(
      UserData(registrationID: registration.registrationID, fileHandle: fd)
    )
    try Epoll.epoll_ctl(
      epfd: self.selectorFD,
      op: Epoll.EPOLL_CTL_MOD,
      fd: fd,
      event: &ev
    )
  }

  /// Deregisters a file descriptor from I/O monitoring and drains its
  /// pending submissions.
  ///
  /// Every queued submission resumes with
  /// ``IOError/fileHandleClosed(operation:)`` carrying the operation's
  /// kind. After deregistration the next `registerIO(fd:)` for the same
  /// integer (e.g. after `close(2)` and a fresh `socket(2)` returns the
  /// reused fd) gets a new ``SelectorRegistrationID``, so any stale epoll
  /// events that arrive for the old generation are silently dropped by the
  /// dispatch loop.
  mutating func deregisterIO(fd: CInt) {
    if let registration = ioRegistrations.removeValue(forKey: fd) {
      for pending in registration.pendingReads {
        pending.continuation.resume(
          throwing: .fileHandleClosed(operation: pending.operation.kind)
        )
      }
      for pending in registration.pendingWrites {
        pending.continuation.resume(
          throwing: .fileHandleClosed(operation: pending.operation.kind)
        )
      }
    }
    var ev = Epoll.epoll_event()
    _ = try? Epoll.epoll_ctl(
      epfd: self.selectorFD,
      op: Epoll.EPOLL_CTL_DEL,
      fd: fd,
      event: &ev
    )
  }

  // MARK: - Operation completion (performs syscall on readiness)

  /// Pops the head submission from the appropriate per-fd queue and runs
  /// its completion syscall. Re-arms the interest mask if the queue still
  /// has remaining entries.
  private mutating func completeNextOperation(fd: CInt, direction: PendingDirection) {
    guard var registration = ioRegistrations[fd] else { return }
    let pending: PendingIO?
    switch direction {
    case .read:
      pending = registration.pendingReads.isEmpty ? nil : registration.pendingReads.removeFirst()
    case .write:
      pending = registration.pendingWrites.isEmpty ? nil : registration.pendingWrites.removeFirst()
    }
    ioRegistrations[fd] = registration

    guard let pending else { return }

    switch direction {
    case .read:
      self.completeReadOperation(pending)
    case .write:
      self.completeWriteOperation(pending)
    }

    // Re-arm if the queue still has more work; otherwise the next submit
    // will arm the mask itself.
    if let updated = ioRegistrations[fd] {
      let stillHasReads = !updated.pendingReads.isEmpty
      let stillHasWrites = !updated.pendingWrites.isEmpty
      if stillHasReads || stillHasWrites {
        // Best-effort re-arm. If this fails the next event loop iteration
        // simply doesn't deliver the event; submits land on the same path
        // and would re-arm then.
        _ = try? self.armInterestMask(fd: fd, registration: updated)
      }
    }
  }

  private func completeReadOperation(_ pending: PendingIO) {
    switch pending.operation {
    case .read(let fd, let buffer):
      while true {
        let bytesRead = Glibc.read(fd, buffer.baseAddress, buffer.count)
        if bytesRead >= 0 {
          pending.continuation.resume(returning: .read(bytesRead: bytesRead))
          return
        }
        if errno == EINTR { continue }
        pending.continuation.resume(throwing: .posix(errno: errno, operation: .read))
        return
      }
    case .accept(let fd):
      while true {
        var addr = sockaddr_storage()
        var addrLen = socklen_t(MemoryLayout<sockaddr_storage>.size)
        let result = withUnsafeMutablePointer(to: &addr) { addrPtr in
          addrPtr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sa in
            Glibc.accept(fd, sa, &addrLen)
          }
        }
        if result >= 0 {
          let flags = fcntl(result, F_GETFL)
          _ = fcntl(result, F_SETFL, flags | O_NONBLOCK)
          pending.continuation.resume(
            returning: .accept(acceptedFileHandle: result, peer: addr, peerLength: addrLen)
          )
          return
        }
        if errno == EINTR { continue }
        pending.continuation.resume(throwing: .posix(errno: errno, operation: .accept))
        return
      }
    case .close(let fd):
      // Close is "drain pending then issue close" — by the time we get
      // here the queue drain already happened in `deregisterIO`. We just
      // run the syscall and resume.
      let result = Glibc.close(fd)
      if result == 0 {
        pending.continuation.resume(returning: .close)
      } else {
        pending.continuation.resume(throwing: .posix(errno: errno, operation: .close))
      }
    default:
      pending.continuation.resume(throwing: .unsupportedOperation(operation: pending.operation.kind))
    }
  }

  private func completeWriteOperation(_ pending: PendingIO) {
    switch pending.operation {
    case .write(let fd, let buffer, let offset):
      while true {
        let remaining = buffer.count - offset
        let written = unsafe Glibc.write(
          fd, buffer.baseAddress! + offset, remaining
        )
        if written >= 0 {
          pending.continuation.resume(returning: .write(bytesWritten: written))
          return
        }
        if errno == EINTR { continue }
        pending.continuation.resume(throwing: .posix(errno: errno, operation: .write))
        return
      }
    case .connect(let fd, _, _):
      var soError: CInt = 0
      var soErrorLen = socklen_t(MemoryLayout<CInt>.size)
      _ = getsockopt(fd, SOL_SOCKET, SO_ERROR, &soError, &soErrorLen)
      if soError == 0 {
        pending.continuation.resume(returning: .connect)
      } else {
        pending.continuation.resume(throwing: .posix(errno: soError, operation: .connect))
      }
    default:
      pending.continuation.resume(throwing: .unsupportedOperation(operation: pending.operation.kind))
    }
  }

  @inline(never)
  internal static func eventfd_write(fd: CInt, value: UInt64) throws -> CInt {
    return try retryingSyscall(blocking: false) {
      CPlatformExecutors.eventfd_write(fd, value)
    }.result
  }
}

internal enum Epoll {
  internal typealias epoll_event = CPlatformExecutors.epoll_event

  internal static let EPOLL_CTL_ADD: CInt = numericCast(CPlatformExecutors.EPOLL_CTL_ADD)
  internal static let EPOLL_CTL_MOD: CInt = numericCast(CPlatformExecutors.EPOLL_CTL_MOD)
  internal static let EPOLL_CTL_DEL: CInt = numericCast(CPlatformExecutors.EPOLL_CTL_DEL)

  #if os(Android)
  internal static let EPOLLIN: CUnsignedInt = 1  //numericCast(EPOLLIN)
  internal static let EPOLLOUT: CUnsignedInt = 4  //numericCast(EPOLLOUT)
  internal static let EPOLLERR: CUnsignedInt = 8  // numericCast(EPOLLERR)
  internal static let EPOLLRDHUP: CUnsignedInt = 8192  //numericCast(EPOLLRDHUP)
  internal static let EPOLLHUP: CUnsignedInt = 16  //numericCast(EPOLLHUP)
  internal static let EPOLLET: CUnsignedInt = 2_147_483_648  //numericCast(EPOLLET)
  #elseif canImport(Musl)
  internal static let EPOLLIN: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLIN)
  internal static let EPOLLOUT: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLOUT)
  internal static let EPOLLERR: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLERR)
  internal static let EPOLLRDHUP: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLRDHUP)
  internal static let EPOLLHUP: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLHUP)
  internal static let EPOLLET: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLET)
  #else
  internal static let EPOLLIN: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLIN.rawValue)
  internal static let EPOLLOUT: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLOUT.rawValue)
  internal static let EPOLLERR: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLERR.rawValue)
  internal static let EPOLLRDHUP: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLRDHUP.rawValue)
  internal static let EPOLLHUP: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLHUP.rawValue)
  internal static let EPOLLET: CUnsignedInt = numericCast(CPlatformExecutors.EPOLLET.rawValue)
  #endif

  internal static let ENOENT: CUnsignedInt = numericCast(CPlatformExecutors.ENOENT)

  @inline(never)
  internal static func epoll_create(size: CInt) throws -> CInt {
    return try retryingSyscall(blocking: false) {
      CPlatformExecutors.epoll_create(size)
    }.result
  }

  @inline(never)
  @discardableResult
  internal static func epoll_ctl(
    epfd: CInt,
    op: CInt,
    fd: CInt,
    event: UnsafeMutablePointer<epoll_event>
  ) throws -> CInt {
    return try retryingSyscall(blocking: false) {
      CPlatformExecutors.epoll_ctl(epfd, op, fd, event)
    }.result
  }

  @inline(never)
  internal static func epoll_wait(
    epfd: CInt,
    events: UnsafeMutablePointer<epoll_event>,
    maxevents: CInt,
    timeout: CInt
  ) throws -> CInt {
    return try retryingSyscall(blocking: false) {
      CPlatformExecutors.epoll_wait(epfd, events, maxevents, timeout)
    }.result
  }
}

private struct EpollFilterSet: OptionSet, Equatable {
  typealias RawValue = UInt8

  let rawValue: RawValue

  static let _none = EpollFilterSet([])
  static let hangup = EpollFilterSet(rawValue: 1 << 0)
  static let readHangup = EpollFilterSet(rawValue: 1 << 1)
  static let input = EpollFilterSet(rawValue: 1 << 2)
  static let output = EpollFilterSet(rawValue: 1 << 3)
  static let error = EpollFilterSet(rawValue: 1 << 4)

  init(rawValue: RawValue) {
    self.rawValue = rawValue
  }
}

extension UInt64 {
  init(_ epollUserData: EpollSelector.UserData) {
    let fd = epollUserData.fileHandle
    assert(fd >= 0, "\(fd) is not a valid file descriptor")
    self = IntegerBitPacking.packUInt32CInt(epollUserData.registrationID.rawValue, fd)
  }
}
#endif
