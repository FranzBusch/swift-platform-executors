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

#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif

/// Internal lowered representation of an I/O operation submitted to a selector.
///
/// The public ``IOExecutor`` surface uses typed methods; each method lowers
/// its parameters into one of these cases inside its borrow scope, then hands
/// the lowered value to the selector. The selector dispatches via a single
/// `switch` per backend, mapping each case to its native primitive
/// (`epoll_ctl` + syscall, `kevent` + syscall, an io_uring SQE, …).
///
/// Buffer pointers are extracted from the caller's borrowed
/// `Span`/`MutableSpan`/`UniqueArray` and remain valid for the entire
/// `submit` call: the borrow is held by the calling coroutine frame across
/// the await, so the pointer is dereferenceable when the selector issues the
/// syscall on the executor thread.
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
package enum LoweredIOOperation {
    /// Read into the caller-supplied buffer. Pointer + capacity bytes.
    case read(fd: CInt, buffer: UnsafeMutableRawBufferPointer)

    /// Write from the caller-supplied buffer at `offset`. Loops at the
    /// `PThreadExecutor` level on partial writes; the selector returns
    /// per-syscall byte counts.
    case write(fd: CInt, buffer: UnsafeRawBufferPointer, offset: Int)

    /// Accept a connection on a listening socket. The accepted descriptor is
    /// auto-registered before the result is returned to the caller.
    case accept(fd: CInt)

    /// Connect a socket to an address. The address is owned by the caller
    /// (passed by value through ``IOExecutor/connect(fileDescriptor:address:)``)
    /// and lives for the duration of the call.
    case connect(fd: CInt, address: sockaddr_storage, addressLength: socklen_t)

    /// Close a file handle. Drains the handle's pending operations before
    /// issuing the actual `close(2)`.
    case close(fd: CInt)

    /// The file handle the operation is targeting.
    var fileHandle: CInt {
        switch self {
        case .read(let fd, _): return fd
        case .write(let fd, _, _): return fd
        case .accept(let fd): return fd
        case .connect(let fd, _, _): return fd
        case .close(let fd): return fd
        }
    }

    /// The operation kind, for ``IOError`` diagnostic tagging.
    var kind: IOOperationKind {
        switch self {
        case .read: return .read
        case .write: return .write
        case .accept: return .accept
        case .connect: return .connect
        case .close: return .close
        }
    }

    /// Whether this operation waits on read-readiness (vs. write-readiness)
    /// in a readiness-shaped backend like epoll.
    var direction: PendingDirection {
        switch self {
        case .read, .accept, .close: return .read
        case .write, .connect: return .write
        }
    }
}

/// Direction of a per-fd pending-operation queue: reads/accepts on one side,
/// writes/connects on the other.
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
package enum PendingDirection {
    case read
    case write
}

/// Internal result of a completed I/O operation.
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
package enum IOOperationResult {
    /// Bytes read; `0` signals EOF.
    case read(bytesRead: Int)

    /// Bytes written by this submission; the caller may need to loop on
    /// partial writes if it requires full delivery.
    case write(bytesWritten: Int)

    /// A connection was accepted: the new file handle and its peer address.
    case accept(acceptedFileHandle: CInt, peer: sockaddr_storage, peerLength: socklen_t)

    /// A `connect` completed.
    case connect

    /// A `close` completed.
    case close
}
