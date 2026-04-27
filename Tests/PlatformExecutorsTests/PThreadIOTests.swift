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

#if os(Linux) || os(FreeBSD) || canImport(Darwin)

import Testing
import PlatformExecutors

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

@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
private func makeLoopbackPair(
  executor: any IOExecutor
) async throws -> (clientFD: PlatformFileHandle, serverFD: PlatformFileHandle) {
  // Listener socket bound to 127.0.0.1:0 (kernel-assigned ephemeral port).
  let listenFD = socket(AF_INET, _SOCK_STREAM_INT, 0)
  try #require(listenFD >= 0, "socket() failed: errno=\(errno)")

  // SO_REUSEADDR + non-blocking + register: accept's submission requires
  // the listening fd to be registered with the executor first. Otherwise
  // the selector resumes with .posix(EBADF, .accept) per the new contract.
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
  try #require(listen(listenFD, 1) == 0, "listen() failed: errno=\(errno)")

  // Read back the kernel-chosen port.
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

  // Client socket: set O_NONBLOCK so the executor's single thread never
  // blocks in a recv/send syscall.
  let clientFD = socket(AF_INET, _SOCK_STREAM_INT, 0)
  try #require(clientFD >= 0, "client socket() failed: errno=\(errno)")
  let flags = fcntl(clientFD, F_GETFL)
  _ = fcntl(clientFD, F_SETFL, flags | O_NONBLOCK)

  // Build a sockaddr_storage carrying the IPv4 connect address.
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

  // Connect and accept concurrently. The executor's accept auto-O_NONBLOCKs
  // and registers the resulting fd.
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

@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
private func withLoopbackPair<R: Sendable>(
  executor: any IOExecutor,
  _ body: (PlatformFileHandle, PlatformFileHandle) async throws -> R
) async throws -> R {
  let pair = try await makeLoopbackPair(executor: executor)
  let result: Result<R, any Error>
  do {
    result = .success(try await body(pair.clientFD, pair.serverFD))
  } catch {
    result = .failure(error)
  }
  try? await executor.close(fileHandle: pair.clientFD)
  try? await executor.close(fileHandle: pair.serverFD)
  return try result.get()
}

// MARK: - Tests

@Suite(.timeLimit(.minutes(1)))
struct PThreadIOTests {

  // MARK: Happy path

  /// Round-trips a small payload through the closure-form `read` and
  /// `write`, exercising connect, accept, the callee-owned pooled buffer
  /// for both directions, and EOF on close.
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func roundtripSmallPayload() async throws {
    try await withIOExecutor { executor in
      try await withLoopbackPair(executor: executor) { clientFD, serverFD in
        let payload: [UInt8] = [0x68, 0x65, 0x6C, 0x6C, 0x6F]  // "hello"

        try await withThrowingTaskGroup(of: Void.self) { group in
          group.addTask {
            try await executor.write(fileHandle: clientFD) { writeBuf in
              for byte in payload {
                writeBuf.append(byte)
              }
            }
          }
          group.addTask {
            var collected: [UInt8] = []
            while collected.count < payload.count {
              try await executor.read(fileHandle: serverFD) { readBuf in
                let span = readBuf.span
                for i in 0..<span.count {
                  collected.append(span[i])
                }
              }
            }
            #expect(collected == payload)
          }
          try await group.waitForAll()
        }
      }
    }
  }

  // MARK: EOF

  /// When the peer closes its end, the next `read` resumes with an empty
  /// buffer — the EOF signal callers depend on.
  ///
  /// Uses bare `makeLoopbackPair` rather than `withLoopbackPair` because the
  /// test intentionally closes one half before the read; the helper would
  /// double-close.
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func readReturnsEmptySpanOnEOF() async throws {
    try await withIOExecutor { executor in
      let pair = try await makeLoopbackPair(executor: executor)
      try await executor.close(fileHandle: pair.clientFD)

      let sawEOF: Bool = try await executor.read(
        fileHandle: pair.serverFD
      ) { readBuf in
        readBuf.count == 0
      }
      #expect(sawEOF, "expected empty buffer on peer close")

      try await executor.close(fileHandle: pair.serverFD)
    }
  }

  // MARK: Error mapping

  /// `read` on a bad fd surfaces `IOError.posix(errno: EBADF, operation: .read)`,
  /// covering the non-`EAGAIN` error branch in `read(body:)`. The selector
  /// rejects the submission immediately because the fd was never registered.
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func readOnBadFDSurfacesEBADF() async throws {
    try await withIOExecutor { executor in
      do {
        try await executor.read(fileHandle: -1) { _ in () }
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

  // MARK: Cancellation

  /// A `read` suspended on an idle socket throws `IOError.cancelled` when
  /// its parent task is cancelled — the new submit path's
  /// `withTaskCancellationHandler` → `cancelPendingIO` flow.
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func readCancellationThrowsCancellationError() async throws {
    try await withIOExecutor { executor in
      try await withLoopbackPair(executor: executor) { _, serverFD in
        try await withThrowingTaskGroup(of: Bool.self) { group in
          group.addTask {
            do {
              try await executor.read(fileHandle: serverFD) { _ in () }
              return false
            } catch let error as IOError {
              if error.code == .cancelled { return true }
              return false
            } catch {
              return false
            }
          }
          // Let the read suspend on epoll before cancelling.
          try await Task.sleep(for: .milliseconds(50))
          group.cancelAll()
          var sawCancellation = false
          for try await result in group {
            if result { sawCancellation = true }
          }
          #expect(sawCancellation, "expected IOError.cancelled from cancelled read")
        }

        // Confirm no global poison: a fresh op on a different fd in the
        // same executor still works.
        try await withLoopbackPair(executor: executor) { c, s in
          try await withThrowingTaskGroup(of: Void.self) { g in
            g.addTask {
              try await executor.write(fileHandle: c) { writeBuf in
                writeBuf.append(0x41)  // 'A'
              }
            }
            g.addTask {
              try await executor.read(fileHandle: s) { readBuf in
                #expect(readBuf.count == 1)
              }
            }
            try await g.waitForAll()
          }
        }
      }
    }
  }

  // MARK: F9 — edge-triggered drain

  /// A bulk write that exceeds the executor's pool buffer must be drained
  /// across multiple `read` calls. Each `read(body:)` performs a single
  /// `recv` syscall (PThreadExecutor.swift:704–758, review M-c), so the
  /// reader must re-arm via `EPOLL_CTL_MOD` to pull subsequent bytes —
  /// the F9 ET-drain semantics.
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func largeWriteDrainsAcrossMultipleReads() async throws {
    try await withIOExecutor { executor in
      try await withLoopbackPair(executor: executor) { clientFD, serverFD in
        let chunkSize = 16 * 1024
        let chunks = 8
        let totalBytes = chunkSize * chunks  // 128 KiB

        try await withThrowingTaskGroup(of: Int.self) { group in
          group.addTask {
            for chunkIndex in 0..<chunks {
              try await executor.write(fileHandle: clientFD) { writeBuf in
                for byteIndex in 0..<chunkSize {
                  writeBuf.append(UInt8((chunkIndex + byteIndex) & 0xFF))
                }
              }
            }
            return -1  // sentinel: writer done
          }
          group.addTask {
            var collected = 0
            while collected < totalBytes {
              try await executor.read(fileHandle: serverFD) { readBuf in
                collected += readBuf.count
              }
            }
            return collected
          }

          var readerTotal = 0
          for try await result in group {
            if result >= 0 {
              readerTotal = result
            }
          }
          #expect(readerTotal == totalBytes)
        }
      }
    }
  }

  // MARK: F3 — multi-waiter (now enabled after S1.7's per-fd deque)

  /// Two concurrent readers on the same fd must each make progress.
  /// Before S1.7, the second submission overwrote the first's
  /// continuation slot in `IORegistration` (review F3) — the first read
  /// leaked and hung forever. The S1.7 per-fd `pendingReads`/`pendingWrites`
  /// deque fixes this; both reads must observe their own bytes.
  @Test
  @available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
  func multiWaiterDoesNotOverwriteContinuation() async throws {
    try await withIOExecutor { executor in
      try await withLoopbackPair(executor: executor) { clientFD, serverFD in
        try await withThrowingTaskGroup(of: Bool.self) { group in
          group.addTask {
            var saw = false
            try await executor.read(fileHandle: serverFD) { readBuf in
              saw = readBuf.count > 0
            }
            return saw
          }
          group.addTask {
            var saw = false
            try await executor.read(fileHandle: serverFD) { readBuf in
              saw = readBuf.count > 0
            }
            return saw
          }
          // Give both reads time to suspend, then send two payloads with a
          // pause between them so the second flush is a separate epoll wake.
          try await Task.sleep(for: .milliseconds(50))
          try await executor.write(fileHandle: clientFD) { writeBuf in
            writeBuf.append(0x41)
          }
          try await Task.sleep(for: .milliseconds(20))
          try await executor.write(fileHandle: clientFD) { writeBuf in
            writeBuf.append(0x42)
          }

          var bothResumed = true
          for try await sawData in group {
            bothResumed = bothResumed && sawData
          }
          #expect(bothResumed, "both concurrent readers must observe data")
        }
      }
    }
  }
}

#endif
