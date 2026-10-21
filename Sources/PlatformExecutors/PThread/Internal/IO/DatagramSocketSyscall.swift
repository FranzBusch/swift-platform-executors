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

#if ExperimentalIO
#if os(Linux) || os(Android) || os(FreeBSD) || canImport(Darwin)
#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

// The static methods below shadow the C functions of the same name,
// so we have to alias them here.
private let sysSendMessage = unsafe sendmsg
private let sysReceiveMessage = unsafe recvmsg

/// The non-blocking datagram socket syscalls that back the UDP operations.
///
/// Creating, binding and closing a socket is shared with ``StreamSocketSyscall``.
enum DatagramSocketSyscall {
  /// Sends the datagram described by the given message.
  ///
  /// - Parameters:
  ///   - fileDescriptor: The file descriptor of the socket.
  ///   - message: The message to send. Its name holds the address to send to.
  /// - Returns: The number of bytes sent, or `nil` if sending would have blocked.
  static func sendMessage(
    _ fileDescriptor: CInt,
    _ message: UnsafePointer<msghdr>
  ) throws(IOError) -> Int? {
    let result = try retryingIOSyscall(blocking: true) {
      unsafe sysSendMessage(fileDescriptor, message, 0)
    }
    guard case .processed(let count) = result else {
      return nil
    }
    return count
  }

  /// Receives a datagram into the given message.
  ///
  /// - Parameters:
  ///   - fileDescriptor: The file descriptor of the socket.
  ///   - message: The message to receive into. The address of the sender is written into its name.
  /// - Returns: The number of bytes received, or `nil` if receiving would have blocked.
  static func receiveMessage(
    _ fileDescriptor: CInt,
    _ message: UnsafeMutablePointer<msghdr>
  ) throws(IOError) -> Int? {
    let result = try retryingIOSyscall(blocking: true) {
      unsafe sysReceiveMessage(fileDescriptor, message, 0)
    }
    guard case .processed(let count) = result else {
      return nil
    }
    return count
  }
}
#endif
#endif
