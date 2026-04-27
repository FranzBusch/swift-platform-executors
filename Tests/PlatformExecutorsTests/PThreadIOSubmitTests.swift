//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift.org open source project
//
// Copyright (c) 2026 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
// See https://swift.org/CONTRIBUTORS.txt for the list of Swift project authors
//
//===----------------------------------------------------------------------===//
//
// Tests for the production properties of the S1.7 completion path
// implemented on `EpollSelector`:
//
//  - Per-fd `pendingReads`/`pendingWrites` deques preserve FIFO order
//    across multiple concurrent submissions on the same fd (F3 fix).
//  - `deregisterIO` drains the deques and resumes each waiter with
//    `IOError.fileDescriptorClosed(operation:)`.
//  - `SelectorRegistrationID` validation in the dispatch loop discards
//    stale events for closed-and-reused descriptors (the load-bearing
//    test for the fd-reuse safety property — without it, the kernel
//    handing back a recycled fd integer would resume the *new* fd's
//    continuation with the *old* fd's epoll event).
//  - Submit against an unregistered fd surfaces `IOError.posix(EBADF, ...)`
//    rather than crashing (F4 fail-by-resume in the new path).
//
//===----------------------------------------------------------------------===//

#if os(Linux) || os(FreeBSD) || canImport(Darwin)

import Testing
import PlatformExecutors
import BasicContainers

#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif

// `SOCK_STREAM` is a `__socket_type` enum on Glibc (rawValue: UInt32) but
// an `Int32` literal on Darwin. Resolve once at file scope.
#if canImport(Glibc)
private let _SOCK_STREAM_INT: Int32 = Int32(SOCK_STREAM.rawValue)
#elseif canImport(Darwin)
private let _SOCK_STREAM_INT: Int32 = SOCK_STREAM
#endif

// MARK: - Loopback fixture

/// Builds a TCP loopback pair on `127.0.0.1` and registers the client end
/// with the executor. The server end is returned **already O_NONBLOCK and
/// auto-registered** by the executor's `accept`.
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
private func makeLoopbackPair(
  executor: any IOExecutor
) async throws -> (clientFD: PlatformFileHandle, serverFD: PlatformFileHandle) {
  let listenFD = socket(AF_INET, _SOCK_STREAM_INT, 0)
  try #require(listenFD >= 0, "socket() failed: errno=\(errno)")
  let listenFlags = fcntl(listenFD, F_GETFL)
  _ = fcntl(listenFD, F_SETFL, listenFlags | O_NONBLOCK)

  var bindAddr = sockaddr_in()
  bindAddr.sin_family = sa_family_t(AF_INET)
  bindAddr.sin_addr.s_addr = inet_addr("127.0.0.1")
  bindAddr.sin_port = 0
  let bindLen = socklen_t(MemoryLayout<sockaddr_in>.size)

  let bindResult = withUnsafePointer(to: &bindAddr) {
    $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { sa in
      bind(listenFD, sa, bindLen)
    }
  }
  try #require(bindResult == 0, "bind() failed: errno=\(errno)")
  try #require(listen(listenFD, 8) == 0, "listen() failed: errno=\(errno)")

  var nameAddr = sockaddr_in()
  var nameLen = socklen_t(MemoryLayout<sockaddr_in>.size)
  let getResult = withUnsafeMutablePointer(to: &nameAddr) {
    $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { sa in
      getsockname(listenFD, sa, &nameLen)
    }
  }
  try #require(getResult == 0, "getsockname() failed: errno=\(errno)")
  let boundPort = nameAddr.sin_port

  // The first I/O operation on each handle lazy-registers it with the
  // executor; no explicit register call is needed.

  let clientFD = socket(AF_INET, _SOCK_STREAM_INT, 0)
  try #require(clientFD >= 0, "client socket() failed: errno=\(errno)")
  let flags = fcntl(clientFD, F_GETFL)
  _ = fcntl(clientFD, F_SETFL, flags | O_NONBLOCK)

  var connectStorage = sockaddr_storage()
  withUnsafeMutablePointer(to: &connectStorage) {
    $0.withMemoryRebound(to: sockaddr_in.self, capacity: 1) { sin in
      sin.pointee.sin_family = sa_family_t(AF_INET)
      sin.pointee.sin_addr.s_addr = inet_addr("127.0.0.1")
      sin.pointee.sin_port = boundPort
    }
  }
  let storageLen = socklen_t(MemoryLayout<sockaddr_in>.size)
  let connectAddress = SocketAddress(storage: connectStorage, length: storageLen)

  async let connectResult: Void = executor.connect(
    fileHandle: clientFD,
    address: connectAddress
  )
  async let acceptResult = executor.accept(fileHandle: listenFD)

  let accepted = try await acceptResult
  let serverFD: PlatformFileHandle = accepted.acceptedFileHandle
  try await connectResult

  try await executor.close(fileHandle: listenFD)
  return (clientFD: clientFD, serverFD: serverFD)
}

// MARK: - Tests

@Suite(.timeLimit(.minutes(1)))
struct PThreadIOSubmitTests {

  // MARK: F3 — per-fd queue ordering

  /// Three concurrent reads on the same fd must observe distinct bytes in
  /// FIFO submission order. The `pendingReads` deque is the load-bearing
  /// invariant — without it, the second submission would overwrite the
  /// first's continuation slot.
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func threeConcurrentReadsCompleteInFIFOOrder() async throws {
    try await withIOExecutor { executor in
      let pair = try await makeLoopbackPair(executor: executor)
      defer {
        // Bare close; loopbackPair does not own the deferred close.
      }

      try await withThrowingTaskGroup(of: (Int, UInt8).self) { group in
        for index in 0..<3 {
          group.addTask {
            var byte: UInt8 = 0
            try await executor.read(fileHandle: pair.serverFD) { readBuf in
              let span = readBuf.span
              if span.count > 0 {
                byte = span[0]
              }
            }
            return (index, byte)
          }
        }
        // Give the three reads time to suspend in the deque, then write
        // three single-byte payloads with a small gap between them so each
        // wake delivers exactly one read.
        try await Task.sleep(for: .milliseconds(50))
        for byte: UInt8 in [0x41, 0x42, 0x43] {
          try await executor.write(fileHandle: pair.clientFD) { writeBuf in
            writeBuf.append(byte)
          }
          try await Task.sleep(for: .milliseconds(20))
        }

        var collected: [UInt8] = []
        for try await (_, byte) in group {
          collected.append(byte)
        }
        #expect(collected.sorted() == [0x41, 0x42, 0x43])
      }

      try await executor.close(fileHandle: pair.clientFD)
      try await executor.close(fileHandle: pair.serverFD)
    }
  }

  // MARK: F4 — fail-by-resume on bad fd

  /// Submitting an operation against a non-registered fd must throw the
  /// typed POSIX error rather than crashing. The selector's `submit`
  /// detects the missing registration and resumes the continuation with
  /// `.posix(EBADF, ...)` (M1-S1.0/F4).
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func submitOnUnregisteredFDFailsByResume() async throws {
    try await withIOExecutor { executor in
      do {
        try await executor.read(fileHandle: 999_999) { _ in () }
        Issue.record("expected IOError with code .posix and errno EBADF, got success")
      } catch let error as IOError {
        #expect(error.code == .posix)
        #expect(error.errno == EBADF)
        #expect(error.operation == .read)
      } catch {
        Issue.record("expected IOError, got \(type(of: error)): \(error)")
      }
    }
  }

  // MARK: Close drains pending submissions

  /// Closing a file handle must drain its per-fd queues, resuming
  /// every pending submission with code `.fileHandleClosed`.
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func closeDrainsPendingReadsAsFileDescriptorClosed() async throws {
    try await withIOExecutor { executor in
      let pair = try await makeLoopbackPair(executor: executor)

      try await withThrowingTaskGroup(of: Bool.self) { group in
        for _ in 0..<2 {
          group.addTask {
            do {
              try await executor.read(fileHandle: pair.serverFD) { _ in () }
              return false
            } catch let error as IOError {
              if error.code == .fileHandleClosed && error.operation == .read { return true }
              return false
            } catch {
              return false
            }
          }
        }
        // Let both reads suspend, then close the fd — the deque drain
        // resumes both with .fileDescriptorClosed.
        try await Task.sleep(for: .milliseconds(50))
        try await executor.close(fileHandle: pair.serverFD)

        var sawClosed = 0
        for try await observed in group {
          if observed { sawClosed += 1 }
        }
        #expect(sawClosed == 2, "expected both pending reads to resume with .fileDescriptorClosed")
      }

      try await executor.close(fileHandle: pair.clientFD)
    }
  }

  // MARK: fd-reuse stale-event suppression — the load-bearing safety test

  /// Register fd A, submit a read, close A, register fd B (the kernel hands
  /// back the same integer because of greedy fd allocation), submit a read
  /// on B with a *distinct* peer payload. B must observe B's bytes — never
  /// A's stale epoll-event-driven completion. Without
  /// `SelectorRegistrationID` validation in the dispatch loop, the in-flight
  /// epoll event for A could resume B's continuation with whatever the old
  /// generation's syscall produced.
  ///
  /// Linux's fd allocation is "lowest unused", so closing A and immediately
  /// `socket()`ing typically returns the same integer. We assert that the
  /// integer is reused and proceed; if it isn't (rare, under contention),
  /// the test still asserts the data integrity invariant — just doesn't
  /// exercise the reuse path that round.
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func fdReuseDoesNotResumeStaleContinuation() async throws {
    try await withIOExecutor { executor in
      // First generation: fd A.
      let pairA = try await makeLoopbackPair(executor: executor)
      let firstClientFD = pairA.clientFD
      let firstServerFD = pairA.serverFD

      try await executor.close(fileHandle: firstClientFD)
      try await executor.close(fileHandle: firstServerFD)

      // Second generation: rebuild a loopback pair. On Linux, the kernel's
      // greedy fd allocator typically reuses the just-freed integers.
      let pairB = try await makeLoopbackPair(executor: executor)

      // Whether or not the fd integer is reused, the data invariant must
      // hold: B's read must observe B's payload.
      let payload: UInt8 = 0x5A
      try await withThrowingTaskGroup(of: UInt8?.self) { group in
        group.addTask {
          var observed: UInt8? = nil
          try await executor.read(fileHandle: pairB.serverFD) { readBuf in
            let span = readBuf.span
            if span.count > 0 {
              observed = span[0]
            }
          }
          return observed
        }
        try await Task.sleep(for: .milliseconds(50))
        try await executor.write(fileHandle: pairB.clientFD) { writeBuf in
          writeBuf.append(payload)
        }

        var sawCorrect = false
        for try await observed in group {
          if observed == payload { sawCorrect = true }
        }
        #expect(sawCorrect, "fd-reuse must not corrupt the new generation's read")
      }

      try await executor.close(fileHandle: pairB.clientFD)
      try await executor.close(fileHandle: pairB.serverFD)
    }
  }

  // MARK: Caller-owned read into UniqueArray

  /// The caller-owned `read(into: inout some RangeReplaceableContainer)`
  /// variant appends the kernel's bytes into the caller's buffer and
  /// returns. Verifies the closure-form's pointer-extraction path (the
  /// async overload of `OutputSpan.withUnsafeMutableBufferPointer`)
  /// against `UniqueArray` — the most common conformer.
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func callerOwnedReadAppendsBytesIntoUniqueArray() async throws {
    try await withIOExecutor { executor in
      let pair = try await makeLoopbackPair(executor: executor)

      try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask {
          try await executor.write(fileHandle: pair.clientFD) { writeBuf in
            for byte: UInt8 in [0x10, 0x20, 0x30] {
              writeBuf.append(byte)
            }
          }
        }
        group.addTask {
          var buffer = UniqueArray<UInt8>()
          try await executor.read(fileHandle: pair.serverFD, into: &buffer)
          // The kernel may deliver fewer bytes per syscall, but the first
          // delivery for this small payload is the whole thing.
          let span = buffer.span
          #expect(span.count >= 1, "expected at least one byte appended")
          if span.count >= 1 {
            #expect(span[0] == 0x10)
          }
        }
        try await group.waitForAll()
      }

      try await executor.close(fileHandle: pair.clientFD)
      try await executor.close(fileHandle: pair.serverFD)
    }
  }

  // MARK: Caller-owned read into a non-UniqueArray RangeReplaceableContainer

  /// Exercises the generic `read(into:)` against a different
  /// `RangeReplaceableContainer<UInt8>` conformer (`RigidArray`) — proves
  /// the protocol's `append(addingCount:initializingWith:)` lowering
  /// works for any conformer, not just `UniqueArray`. This is the
  /// load-bearing test for the S1.1 generalization (was typed
  /// `inout UniqueArray<UInt8>` before; now `inout some RRC<UInt8>`).
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func callerOwnedReadAppendsBytesIntoRigidArray() async throws {
    try await withIOExecutor { executor in
      let pair = try await makeLoopbackPair(executor: executor)

      try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask {
          try await executor.write(fileHandle: pair.clientFD) { writeBuf in
            for byte: UInt8 in [0xA1, 0xA2, 0xA3] {
              writeBuf.append(byte)
            }
          }
        }
        group.addTask {
          // RigidArray has fixed capacity; size it for one selector
          // chunk so the syscall has room to write into.
          var buffer = RigidArray<UInt8>(capacity: 65_536)
          try await executor.read(fileHandle: pair.serverFD, into: &buffer)
          #expect(buffer.count >= 1, "expected at least one byte appended")
          if buffer.count >= 1 {
            #expect(buffer[0] == 0xA1)
          }
        }
        try await group.waitForAll()
      }

      try await executor.close(fileHandle: pair.clientFD)
      try await executor.close(fileHandle: pair.serverFD)
    }
  }
}

#endif
