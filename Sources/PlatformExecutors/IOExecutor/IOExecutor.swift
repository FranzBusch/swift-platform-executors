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

#if canImport(Glibc) || canImport(Darwin)

public import ContainersPreview
public import BasicContainers

/// A completion-based I/O executor.
///
/// An `IOExecutor` performs asynchronous I/O on platform-native file handles
/// and resumes the calling task with the result. The same call site targets
/// epoll on Linux, kqueue on macOS, and other completion engines such as
/// io_uring or IOCP without changing shape: the executor schedules the
/// underlying readiness wait or completion syscall on its dedicated event-loop
/// thread.
///
/// Implementations register a file handle with the kernel selector on first
/// use and deregister it when ``close(fileHandle:)`` is called. The protocol
/// exposes typed methods rather than a single submission entry point so that
/// pooled-buffer reads and writes can take a closure-form lease whose
/// lifetime is bound to the call.
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
public protocol IOExecutor: TaskExecutor {
  /// Reads bytes from a file handle into a callee-owned pool buffer.
  ///
  /// Acquires a buffer from the executor's internal pool, issues the read
  /// syscall, and passes the filled buffer to `body`. The buffer returns to
  /// the pool when `body` returns or throws. The buffer is empty on
  /// end-of-file.
  ///
  /// On cancellation the operation throws ``IOError`` with code
  /// ``IOError/Code/cancelled`` and the buffer returns to the pool. Partial
  /// bytes that were already in the pool buffer are not surfaced to the
  /// closure; use ``read(fileHandle:into:)`` instead when partial transfers
  /// matter.
  ///
  /// - Parameters:
  ///   - fileHandle: The platform-native file handle to read from.
  ///   - body: A closure that consumes the filled buffer and returns a value.
  /// - Returns: The value produced by `body`.
  /// - Throws: ``IOError`` on syscall failure, file-handle closure, or
  ///   cancellation.
  func read<Return: ~Copyable>(
    fileHandle: PlatformFileHandle,
    body: (inout UniqueArray<UInt8>) async throws -> sending Return
  ) async throws(IOError) -> sending Return

  /// Reads bytes from a file handle into the caller's buffer.
  ///
  /// Appends bytes into the free capacity of `buffer`. Appends zero bytes on
  /// end-of-file; callers detect EOF by snapshotting `buffer.count` before
  /// the call and comparing on return.
  ///
  /// On cancellation the operation throws ``IOError`` with code
  /// ``IOError/Code/cancelled``; bytes the kernel transferred before the
  /// cancellation are already in `buffer` and the error's
  /// ``IOError/transferred`` property reports their count.
  ///
  /// - Parameters:
  ///   - fileHandle: The platform-native file handle to read from.
  ///   - buffer: The buffer to append into. Any conforming
  ///     `RangeReplaceableContainer<UInt8>` works, including
  ///     `UniqueArray`, `RigidArray`, and the deque variants.
  /// - Throws: ``IOError`` on syscall failure, file-handle closure, or
  ///   cancellation.
  func read<Buffer: RangeReplaceableContainer<UInt8> & ~Copyable>(
    fileHandle: PlatformFileHandle,
    into buffer: inout Buffer
  ) async throws(IOError) where Buffer.Element: ~Copyable

  /// Writes the contents of a buffer to a file handle.
  ///
  /// Loops internally on partial writes until every byte has been delivered
  /// or the operation throws.
  ///
  /// - Parameters:
  ///   - fileHandle: The platform-native file handle to write to.
  ///   - buffer: The buffer whose bytes are written. Any conforming
  ///     `RangeReplaceableContainer<UInt8>` works.
  /// - Throws: ``IOError`` on syscall failure, file-handle closure, or
  ///   cancellation.
  func write<Buffer: RangeReplaceableContainer<UInt8> & ~Copyable>(
    fileHandle: PlatformFileHandle,
    from buffer: borrowing Buffer
  ) async throws(IOError) where Buffer.Element: ~Copyable

  /// Writes bytes to a file handle using a callee-owned pool buffer.
  ///
  /// Acquires a pooled buffer, hands it to `body` for filling, then writes
  /// the filled prefix to the file handle. A body that fills only part of
  /// the buffer's capacity has only that prefix written. The buffer returns
  /// to the pool when this method returns or throws.
  ///
  /// - Parameters:
  ///   - fileHandle: The platform-native file handle to write to.
  ///   - body: A closure that fills the pool buffer.
  /// - Throws: ``IOError`` on syscall failure, file-handle closure, or
  ///   cancellation.
  func write(
    fileHandle: PlatformFileHandle,
    body: (inout UniqueArray<UInt8>) async throws -> Void
  ) async throws(IOError)

  /// Accepts a connection on a listening socket.
  ///
  /// The accepted file handle is set to non-blocking mode and registered
  /// with the executor before this method returns.
  ///
  /// - Parameter fileHandle: The listening socket's file handle.
  /// - Returns: The accepted file handle and the peer's socket address. The
  ///   `acceptedFileHandle:` label distinguishes the new handle from the
  ///   listening handle at the call site.
  /// - Throws: ``IOError`` on syscall failure, file-handle closure, or
  ///   cancellation.
  func accept(
    fileHandle: PlatformFileHandle
  ) async throws(IOError) -> (acceptedFileHandle: PlatformFileHandle, peer: SocketAddress)

  /// Connects a socket to the given address.
  ///
  /// Performs a non-blocking `connect(2)` and waits for the socket to
  /// become writable. On success the socket is connected and ready for
  /// I/O.
  ///
  /// - Parameters:
  ///   - fileHandle: The client socket's file handle.
  ///   - address: The destination address to connect to.
  /// - Throws: ``IOError`` on syscall failure, file-handle closure, or
  ///   cancellation.
  func connect(
    fileHandle: PlatformFileHandle,
    address: SocketAddress
  ) async throws(IOError)

  /// Closes a file handle.
  ///
  /// Drains the file handle's in-flight operations first: each one resumes
  /// with ``IOError`` of code ``IOError/Code/fileHandleClosed``. After
  /// `close` returns, further submissions on the handle throw the same
  /// error.
  ///
  /// - Parameter fileHandle: The file handle to close.
  /// - Throws: ``IOError`` if the underlying `close(2)` syscall fails with
  ///   anything other than `EINTR`.
  func close(
    fileHandle: PlatformFileHandle
  ) async throws(IOError)
}

#endif
