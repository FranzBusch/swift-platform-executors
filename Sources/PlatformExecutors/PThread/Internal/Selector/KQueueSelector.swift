//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift.org open source project
//
// Copyright (c) 2025-2026 Apple Inc. and the Swift project authors
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

#if canImport(Darwin)
import Darwin

// `kevent` is both a function and a type in Darwin. Save the function under a
// private alias before the `kevent` type-name shadows it inside `withUnsafePointer(to:)`.
private let sysKevent = kevent

/// A monotonic per-fd registration ID, mirrored from
/// `swift-nio`'s `SelectorRegistrationID` (see
/// `NIOPosix/SelectorGeneric.swift:409`).
///
/// `KQueueSelector` allocates a fresh ID at each ``KQueueSelector/registerIO(fd:)``
/// and writes it into the kernel-event payload as `kevent.udata`. On every
/// event the dispatch loop validates `event.registrationID ==
/// ioRegistrations[fd]?.registrationID` and discards stale events for
/// closed-and-reused descriptors. Load-bearing for fd-reuse safety; without
/// it, an in-flight kqueue event for a closed fd can resume the wrong
/// continuation when the kernel hands the same integer back for a fresh
/// socket. Mirrors `EpollSelector.SelectorRegistrationID` (ENGINEERING K1).
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
struct SelectorRegistrationID: Hashable, Sendable {
  var rawValue: UInt32

  init(rawValue: UInt32) {
    self.rawValue = rawValue
  }

  /// The reserved ID for the selector's internal slots (EVFILT_USER ident 0,
  /// EVFILT_TIMER idents 1/2). The allocator never hands `.max` out for I/O fds.
  static let reservedForInternalFDs = SelectorRegistrationID(rawValue: .max)
}

@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
extension SelectorRegistrationID {
  /// Allocates the next ID, skipping `.max` (reserved).
  fileprivate static func nextID(_ counter: inout UInt32) -> SelectorRegistrationID {
    let issued = counter
    counter &+= 1
    if counter == UInt32.max {
      counter &+= 1
    }
    return SelectorRegistrationID(rawValue: issued)
  }
}

/// A selector that uses kqueue for eventing.
///
/// Mirrors the structure of ``EpollSelector`` (S1.7) so the executor I/O
/// path behaves identically across Linux and Darwin: per-fd FIFO queues
/// (K2), monotonic registration IDs validated on dispatch (K1),
/// fail-by-resume on submission failure (K3), closure-scoped pooled buffer
/// leases (K4 — handled by the `PThreadExecutor` layer above the selector).
///
/// Sockets register as **edge-triggered** (`EV_CLEAR`) to mirror
/// `EpollSelector`'s `EPOLLET`. NIO's kqueue selector uses level-triggered
/// (`EV_ADD`/`EV_DELETE` only) for sockets, but our project chose ET on
/// epoll for K-series semantics; the kqueue selector matches that choice
/// for behavioral parity within the project. A fresh `EV_ADD` against an
/// already-armed `EV_CLEAR` filter re-evaluates against current state, so
/// re-arming on the dispatch path with remaining queued work behaves the
/// same as `EPOLL_CTL_MOD`.
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
struct KQueueSelector: ~Copyable {

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
  /// `registrationID` is the fd-reuse disambiguator. `pendingReads` /
  /// `pendingWrites` are FIFO queues — multiple concurrent reads (or writes)
  /// on the same fd complete in submission order, fixing the
  /// M1-S1.0/F3 single-slot overwrite hazard.
  struct IORegistration {
    var registrationID: SelectorRegistrationID
    var pendingReads: ContiguousArray<PendingIO> = []
    var pendingWrites: ContiguousArray<PendingIO> = []
  }

  /// The selector file descriptor.
  fileprivate var selectorFD: CInt
  /// The next continuous clock timer to avoid re-arming the timer if possible.
  fileprivate var nextContinuousClockTimer: ContinuousClock.Instant?
  /// The next suspending clock timer to avoid re-arming the timer if possible.
  fileprivate var nextSuspendingClockTimer: SuspendingClock.Instant?
  /// Registered I/O file descriptors and their pending operations.
  var ioRegistrations: [CInt: IORegistration] = [:]
  /// Allocator state for ``SelectorRegistrationID``.
  fileprivate var nextRegistrationIDCounter: UInt32 = 0

  init() throws {
    // K3 does not apply to init-time selector creation: there is no caller
    // continuation to fail-by-resume into, and a process that cannot create
    // its own kqueue cannot run. Mirror `EpollSelector.init`'s pattern.
    self.selectorFD = try! Self.kqueue()

    var event = Darwin.kevent()
    event.ident = 0
    event.filter = Int16(EVFILT_USER)
    event.fflags = UInt32(NOTE_FFNOP)
    event.data = 0
    event.udata = nil
    event.flags = UInt16(EV_ADD | EV_ENABLE | EV_CLEAR)
    try withUnsafeMutablePointer(to: &event) { ptr in
      try Self.kqueueApplyEventChangeSet(
        selectorFD: selectorFD,
        keventBuffer: UnsafeMutableBufferPointer(start: ptr, count: 1)
      )
    }
  }

  deinit {
    // We try! the close because close can only fail in the following ways:
    // - EINTR, which we eat in close
    // - EIO, which can only happen for on-disk files
    // - EBADF, which can't happen here because we would crash as EBADF is marked unacceptable
    try! close(descriptor: self.selectorFD)
  }

  @inline(never)
  fileprivate static func kqueue() throws -> CInt {
    return try retryingSyscall(blocking: false) {
      Darwin.kqueue()
    }.result
  }

  @inline(never)
  @discardableResult
  fileprivate static func kevent(
    kq: CInt,
    changelist: UnsafePointer<kevent>?,
    nchanges: CInt,
    eventlist: UnsafeMutablePointer<kevent>?,
    nevents: CInt,
    timeout: UnsafePointer<Darwin.timespec>?
  ) throws -> CInt {
    return try retryingSyscall(blocking: false) {
      sysKevent(kq, changelist, nchanges, eventlist, nevents, timeout)
    }.result
  }

  /// Apply a kqueue changeset by calling the `kevent` function with the `kevent`s supplied in `keventBuffer`.
  fileprivate static func kqueueApplyEventChangeSet(
    selectorFD: CInt,
    keventBuffer: UnsafeMutableBufferPointer<kevent>
  ) throws {
    guard keventBuffer.count > 0 else {
      return
    }
    do {
      try Self.kevent(
        kq: selectorFD,
        changelist: keventBuffer.baseAddress!,
        nchanges: CInt(keventBuffer.count),
        eventlist: nil,
        nevents: 0,
        timeout: nil
      )
    } catch let err as POSIXError {
      if err.errnoCode == EINTR {
        // See https://www.freebsd.org/cgi/man.cgi?query=kqueue&sektion=2
        // When kevent() call fails with EINTR error, all changes in the changelist have been applied.
        return
      }
      throw err
    }
  }

  private static func toKQueueTimeSpec(strategy: SelectorStrategy) -> timespec? {
    switch strategy {
    case .block:
      return nil
    case .blockUntilTimeout:
      // Timer events are scheduled via EVFILT_TIMER on idents 1/2; block indefinitely
      // here and let the kernel deliver the timer event.
      return nil
    case .now:
      return timespec(tv_sec: 0, tv_nsec: 0)
    }
  }

  /// Blocks until the wakeup is called or an event is ready.
  mutating func whenReady(
    strategy: SelectorStrategy
  ) throws {
    try self.setupTimers(strategy: strategy)

    let timespec = Self.toKQueueTimeSpec(strategy: strategy)

    // Handle up to 64 events per wake — internal slots (EVFILT_USER, two
    // timers) plus external I/O readiness. Mirror `EpollSelector` capacity.
    let maxEvents = 64
    try withUnsafeTemporaryAllocation(of: Darwin.kevent.self, capacity: maxEvents) { eventsPointer in
      let readyEvents = try timespec.withUnsafeOptionalPointer { ts in
        Int(
          try Self.kevent(
            kq: self.selectorFD,
            changelist: nil,
            nchanges: 0,
            eventlist: eventsPointer.baseAddress!,
            nevents: CInt(maxEvents),
            timeout: ts
          )
        )
      }

      for i in 0..<readyEvents {
        let ev = eventsPointer[i]
        let filter = Int32(ev.filter)

        // EV_ERROR is delivered as an eventlist entry on a per-change failure
        // (kqueue returns errors on the eventlist, not as a thrown errno from
        // the changelist-only `kqueueApplyEventChangeSet`). Surface it as a
        // per-direction failure on the head of the matching queue (K3 in the
        // dispatch path, mirror of `EpollSelector.submit`'s rollback arm).
        if Int32(ev.flags) & EV_ERROR != 0 && ev.data != 0 {
          let fd = CInt(ev.ident)
          let dir: PendingDirection
          switch filter {
          case EVFILT_READ: dir = .read
          case EVFILT_WRITE: dir = .write
          default:
            // Unexpected filter on EV_ERROR — best-effort: skip.
            continue
          }
          self.failHeadOfQueue(
            fd: fd,
            direction: dir,
            errno: CInt(truncatingIfNeeded: ev.data)
          )
          continue
        }

        switch filter {
        case EVFILT_USER:
          // Wakeup event (ident 0). Nothing to consume — EV_CLEAR auto-resets.
          continue
        case EVFILT_TIMER:
          switch Int(ev.ident) {
          case 1:
            self.nextContinuousClockTimer = nil
          case 2:
            self.nextSuspendingClockTimer = nil
          default:
            // Unknown timer ident — ignore rather than trap; we never register
            // arbitrary timer idents, but a future change might.
            continue
          }
        case EVFILT_READ, EVFILT_WRITE:
          let fd = CInt(ev.ident)
          let eventRegistrationID = SelectorRegistrationID(
            rawValue: UInt32(truncatingIfNeeded: UInt(bitPattern: ev.udata))
          )
          guard let registration = self.ioRegistrations[fd] else {
            // The fd was deregistered while events were in flight; ignore.
            continue
          }
          // K1 fd-reuse safety: discard events for a stale generation.
          guard registration.registrationID == eventRegistrationID else {
            continue
          }
          let direction: PendingDirection = filter == EVFILT_READ ? .read : .write
          self.completeNextOperation(fd: fd, direction: direction)
        default:
          // We only register EVFILT_USER, EVFILT_TIMER, EVFILT_READ, EVFILT_WRITE.
          // Any other filter is a bug somewhere; skip rather than trap so the
          // run loop survives.
          continue
        }
      }
    }
  }

  /// Set up kqueue timers for the given strategy.
  private mutating func setupTimers(strategy: SelectorStrategy) throws {
    guard case .blockUntilTimeout(let continuousClockInstant, let suspendingClockInstant) = strategy else {
      return
    }

    if let continuousClockInstant {
      let shouldSetTimer: Bool
      if let nextContinuousClockTimer = self.nextContinuousClockTimer {
        shouldSetTimer = continuousClockInstant < nextContinuousClockTimer
      } else {
        shouldSetTimer = true
      }
      if shouldSetTimer {
        let duration = ContinuousClock.now.duration(to: continuousClockInstant)
        let nanoseconds =
          Int(duration.components.seconds) * 1_000_000_000 + Int(duration.components.attoseconds / 1_000_000_000)
        try self.setTimer(ident: 1, nanoseconds: nanoseconds)
        self.nextContinuousClockTimer = continuousClockInstant
      }
    }

    if let suspendingClockInstant {
      let shouldSetTimer: Bool
      if let nextSuspendingClockTimer = self.nextSuspendingClockTimer {
        shouldSetTimer = suspendingClockInstant < nextSuspendingClockTimer
      } else {
        shouldSetTimer = true
      }
      if shouldSetTimer {
        let duration = SuspendingClock.now.duration(to: suspendingClockInstant)
        let nanoseconds =
          Int(duration.components.seconds) * 1_000_000_000 + Int(duration.components.attoseconds / 1_000_000_000)
        try self.setTimer(ident: 2, nanoseconds: nanoseconds)
        self.nextSuspendingClockTimer = suspendingClockInstant
      }
    }
  }

  /// Set a kqueue timer for the given ident.
  private func setTimer(ident: Int, nanoseconds: Int) throws {
    var event = Darwin.kevent()
    event.ident = UInt(ident)
    event.filter = Int16(EVFILT_TIMER)
    event.flags = UInt16(EV_ADD | EV_ENABLE | EV_ONESHOT)
    event.fflags = UInt32(NOTE_NSECONDS)
    event.data = nanoseconds
    event.udata = nil

    try withUnsafeMutablePointer(to: &event) { ptr in
      try Self.kqueueApplyEventChangeSet(
        selectorFD: self.selectorFD,
        keventBuffer: UnsafeMutableBufferPointer(start: ptr, count: 1)
      )
    }
  }

  /// Wakes up the selector.
  func wakeup() throws {
    var event = Darwin.kevent()
    event.ident = 0
    event.filter = Int16(EVFILT_USER)
    event.fflags = UInt32(NOTE_TRIGGER | NOTE_FFNOP)
    event.data = 0
    event.udata = nil
    event.flags = 0
    try withUnsafeMutablePointer(to: &event) { ptr in
      try Self.kqueueApplyEventChangeSet(
        selectorFD: self.selectorFD,
        keventBuffer: UnsafeMutableBufferPointer(start: ptr, count: 1)
      )
    }
  }

  // MARK: - I/O registration

  /// Registers a file descriptor for I/O event monitoring.
  ///
  /// Allocates a fresh ``SelectorRegistrationID`` for this `(fd, generation)`
  /// and stores an empty per-direction queue. No `kevent` call is issued
  /// here — interest is only armed on the first ``submit(_:submissionID:continuation:)``.
  /// Mirrors `EpollSelector.registerIO`.
  mutating func registerIO(fd: CInt) throws {
    let id = SelectorRegistrationID.nextID(&self.nextRegistrationIDCounter)
    self.ioRegistrations[fd] = IORegistration(registrationID: id)
  }

  /// Deregisters a file descriptor and drains its pending submissions.
  ///
  /// Each queued submission resumes with
  /// ``IOError/fileHandleClosed(operation:)``. Best-effort EV_DELETE on
  /// both `EVFILT_READ` and `EVFILT_WRITE` (`ENOENT` is expected when a
  /// direction was never armed). After deregistration, the next
  /// `registerIO(fd:)` for the same integer (e.g. after `close(2)` and a
  /// fresh `socket(2)` returns the reused fd) gets a new
  /// ``SelectorRegistrationID``, so any stale kqueue events that arrive for
  /// the old generation are silently dropped by the dispatch loop.
  mutating func deregisterIO(fd: CInt) {
    if let registration = self.ioRegistrations.removeValue(forKey: fd) {
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

    // Best-effort EV_DELETE on both filters. ENOENT is expected when a
    // direction was never armed; the `try?` swallows it.
    var changes = ContiguousArray<Darwin.kevent>(repeating: Darwin.kevent(), count: 2)
    changes[0].ident = UInt(fd)
    changes[0].filter = Int16(EVFILT_READ)
    changes[0].flags = UInt16(EV_DELETE)
    changes[1].ident = UInt(fd)
    changes[1].filter = Int16(EVFILT_WRITE)
    changes[1].flags = UInt16(EV_DELETE)
    changes.withUnsafeMutableBufferPointer { buf in
      _ = try? Self.kqueueApplyEventChangeSet(
        selectorFD: self.selectorFD,
        keventBuffer: buf
      )
    }
  }

  // MARK: - I/O operation submission

  /// Submits an I/O operation for completion.
  ///
  /// Appends the submission to the per-fd / per-direction queue and arms
  /// the appropriate filter (`EVFILT_READ` for read/accept, `EVFILT_WRITE`
  /// for write/connect) with `EV_ADD | EV_ENABLE | EV_CLEAR`. When the fd
  /// becomes ready, ``whenReady(strategy:)`` pops the head of the matching
  /// queue and invokes the completion syscall on the executor thread,
  /// resuming the continuation with the result.
  ///
  /// `.close` is special-cased: it bypasses the kqueue arm and synchronously
  /// runs `close(2)`. Registering a readiness wait on a soon-to-close fd
  /// risks an `EV_EOF` burst interleaving with the close completion, and
  /// `close(2)` is non-blocking on a TCP socket so there is nothing to wait
  /// for. The caller is expected to have already called ``deregisterIO(fd:)``
  /// (the `PThreadExecutor.close` path does so).
  ///
  /// Fail-by-resume (K3): if `kevent` fails the entry is popped and the
  /// continuation resumes with the typed POSIX error rather than trapping
  /// at the call site.
  mutating func submit(
    _ operation: LoweredIOOperation,
    submissionID: UInt64,
    continuation: UnsafeContinuation<IOOperationResult, IOError>
  ) {
    let fd = operation.fileHandle

    // Special-case `.close`: synchronous, no kqueue arm.
    if case .close = operation {
      let result = Darwin.close(fd)
      if result == 0 {
        continuation.resume(returning: .close)
      } else {
        continuation.resume(throwing: .posix(errno: errno, operation: .close))
      }
      return
    }

    let direction = operation.direction

    guard var registration = ioRegistrations[fd] else {
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
      try self.armFilter(
        fd: fd,
        direction: direction,
        registrationID: registration.registrationID
      )
    } catch let err as POSIXError {
      // K3 fail-by-resume — pop the entry we just pushed and resume.
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
  /// and resumes its continuation with ``IOError/cancelled(transferred:)``.
  /// Returns `true` if a matching entry was found and resumed; `false` if
  /// the submission had already completed (lost the cancel race).
  ///
  /// Does not EV_DELETE on cancel — the next submit re-arms naturally, and
  /// kqueue tolerates a re-EV_ADD on an already-armed filter.
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

  /// Arms a single direction's filter with `EV_ADD | EV_ENABLE | EV_CLEAR`.
  /// Re-arming an already-armed filter is idempotent — the kernel re-evaluates
  /// against current readiness and re-delivers an initial edge if data is
  /// buffered. Mirrors `EpollSelector.armInterestMask`'s role.
  private mutating func armFilter(
    fd: CInt,
    direction: PendingDirection,
    registrationID: SelectorRegistrationID
  ) throws {
    var ev = Darwin.kevent()
    ev.ident = UInt(fd)
    switch direction {
    case .read:
      ev.filter = Int16(EVFILT_READ)
    case .write:
      ev.filter = Int16(EVFILT_WRITE)
    }
    ev.flags = UInt16(EV_ADD | EV_ENABLE | EV_CLEAR)
    ev.fflags = 0
    ev.data = 0
    ev.udata = UnsafeMutableRawPointer(bitPattern: UInt(registrationID.rawValue))
    try withUnsafeMutablePointer(to: &ev) { ptr in
      try Self.kqueueApplyEventChangeSet(
        selectorFD: self.selectorFD,
        keventBuffer: UnsafeMutableBufferPointer(start: ptr, count: 1)
      )
    }
  }

  // MARK: - Operation completion (performs syscall on readiness)

  /// Pops the head submission from the appropriate per-fd queue and runs
  /// its completion syscall. Re-arms the filter (a fresh `EV_ADD` re-evaluates
  /// against current state under `EV_CLEAR`) if the queue still has more
  /// work; otherwise leaves the filter armed but quiescent until the next
  /// edge or the next submit.
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

    // Re-arm if the queue still has more work in this direction; under EV_CLEAR
    // the kernel only re-fires on a new edge, but a fresh EV_ADD re-evaluates
    // current readiness and delivers an initial edge if data remains buffered.
    if let updated = ioRegistrations[fd] {
      let stillHasWork: Bool
      switch direction {
      case .read: stillHasWork = !updated.pendingReads.isEmpty
      case .write: stillHasWork = !updated.pendingWrites.isEmpty
      }
      if stillHasWork {
        _ = try? self.armFilter(
          fd: fd,
          direction: direction,
          registrationID: updated.registrationID
        )
      }
    }
  }

  /// Pops the head of the matching queue and resumes the continuation with
  /// a typed POSIX error. Used by the EV_ERROR dispatch arm to surface
  /// per-change kqueue failures to the submitter rather than the run loop.
  private mutating func failHeadOfQueue(
    fd: CInt,
    direction: PendingDirection,
    errno: CInt
  ) {
    guard var registration = ioRegistrations[fd] else { return }
    let removed: PendingIO?
    switch direction {
    case .read:
      removed = registration.pendingReads.isEmpty ? nil : registration.pendingReads.removeFirst()
    case .write:
      removed = registration.pendingWrites.isEmpty ? nil : registration.pendingWrites.removeFirst()
    }
    ioRegistrations[fd] = registration
    if let removed {
      removed.continuation.resume(throwing: .posix(errno: errno, operation: removed.operation.kind))
    }
  }

  private func completeReadOperation(_ pending: PendingIO) {
    switch pending.operation {
    case .read(let fd, let buffer):
      while true {
        let bytesRead = Darwin.read(fd, buffer.baseAddress, buffer.count)
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
            Darwin.accept(fd, sa, &addrLen)
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
    default:
      // .close is handled in `submit`'s shortcut path; .write/.connect should
      // never reach the read completion arm. This is a contract violation if
      // it does; surface as `unsupportedOperation` so the caller sees a typed
      // error rather than a hung continuation.
      pending.continuation.resume(throwing: .unsupportedOperation(operation: pending.operation.kind))
    }
  }

  private func completeWriteOperation(_ pending: PendingIO) {
    switch pending.operation {
    case .write(let fd, let buffer, let offset):
      while true {
        let remaining = buffer.count - offset
        let written = unsafe Darwin.write(
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
}

extension Optional {
  fileprivate func withUnsafeOptionalPointer<T>(
    _ body: (UnsafePointer<Wrapped>?) throws -> T
  ) rethrows -> T {
    guard var this = self else {
      return try body(nil)
    }
    return try withUnsafePointer(to: &this) { x in
      try body(x)
    }
  }
}
#endif
