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
@_spi(ExperimentalScheduling) @_spi(ConcurrencyExecutors) @_spi(ExperimentalCustomExecutors) import _Concurrency

#if canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#elseif canImport(Darwin)
import Darwin
#endif

/// The storage that a datagram operation lets the kernel read and write while it is in flight.
@unsafe private struct DatagramMessage {
  var header = unsafe msghdr()
  var vector = unsafe iovec()
  var address = sockaddr_storage()

  /// Points the message at the given buffer, and at the address in its own storage.
  ///
  /// - Parameters:
  ///   - message: The location of the message. It must not move while the operation is in flight.
  ///   - buffer: The buffer that holds or receives the bytes of the datagram.
  ///   - addressLength: The length of the address, or the capacity for receiving one.
  /// - Returns: The header to submit to the kernel.
  static func configure(
    _ message: UnsafeMutablePointer<DatagramMessage>,
    buffer: UnsafeMutableRawBufferPointer,
    addressLength: socklen_t
  ) -> UnsafeMutablePointer<msghdr> {
    let base = UnsafeMutableRawPointer(message)
    let vector = unsafe base.advanced(by: MemoryLayout<DatagramMessage>.offset(of: \.vector)!)
    let address = unsafe base.advanced(by: MemoryLayout<DatagramMessage>.offset(of: \.address)!)
    unsafe message.pointee.vector.iov_base = buffer.baseAddress
    unsafe message.pointee.vector.iov_len = buffer.count
    // Some platforms reject a message that has a name of length zero, so it has no name instead.
    unsafe message.pointee.header.msg_name = addressLength == 0 ? nil : address
    unsafe message.pointee.header.msg_namelen = addressLength
    unsafe message.pointee.header.msg_iov = vector.assumingMemoryBound(to: iovec.self)
    unsafe message.pointee.header.msg_iovlen = 1
    return unsafe base.advanced(by: MemoryLayout<DatagramMessage>.offset(of: \.header)!)
      .assumingMemoryBound(to: msghdr.self)
  }
}

// MARK: - UDP socket handles

@available(anyAppleOS 27.0, *)
extension PThreadExecutor {
  /// A UDP socket of a ``PThreadExecutor``.
  package typealias UDPSocket = PThreadUDPSocket
}

// MARK: - UDP socket operations

@available(anyAppleOS 27.0, *)
extension PThreadExecutor: UDPSocketOperationScheduler {
  package func bind(to address: SocketAddress) async throws(IOError) -> UDPSocket {
    let socket = try StreamSocketSyscall.makeSocket(family: address.family, kind: .datagram)
    do {
      try StreamSocketSyscall.bind(socket, to: address)
    } catch {
      try? StreamSocketSyscall.close(socket)
      throw error
    }
    return UDPSocket(fileDescriptor: socket)
  }

  package func connectUDP(to address: SocketAddress) async throws(IOError) -> UDPSocket {
    let socket = try StreamSocketSyscall.makeSocket(family: address.family, kind: .datagram)
    do {
      // Connecting a datagram socket only records the peer, so it completes right away.
      try unsafe address.withUnsafeSockAddress { (sockAddr, length) throws(IOError) -> Void in
        _ = try unsafe StreamSocketSyscall.connect(socket, to: sockAddr, addressLength: length)
      }
    } catch {
      try? StreamSocketSyscall.close(socket)
      throw error
    }
    return UDPSocket(fileDescriptor: socket)
  }

  package func receive(
    socket: borrowing UDPSocket,
    into buffer: inout OutputRawSpan
  ) async throws(IOError) -> (count: Int, sender: SocketAddress) {
    let (count, sender) = try await self.receiveDatagram(
      on: socket.fileDescriptor,
      into: &buffer,
      addressCapacity: socklen_t(MemoryLayout<sockaddr_storage>.size)
    )
    guard let sender = SocketAddress(sender) else {
      throw IOError(errnoCode: EAFNOSUPPORT)
    }
    return (count, sender)
  }

  package func read(socket: borrowing UDPSocket, into buffer: inout OutputRawSpan) async throws(IOError) -> Int {
    try await self.receiveDatagram(on: socket.fileDescriptor, into: &buffer, addressCapacity: 0).count
  }

  package func send(
    socket: borrowing UDPSocket,
    from buffer: RawSpan,
    to address: SocketAddress
  ) async throws(IOError) -> Int {
    try await self.sendDatagram(on: socket.fileDescriptor, from: buffer, to: address)
  }

  package func write(socket: borrowing UDPSocket, from buffer: RawSpan) async throws(IOError) -> Int {
    try await self.sendDatagram(on: socket.fileDescriptor, from: buffer, to: nil)
  }

  /// Receives one datagram into the free capacity of the buffer.
  ///
  /// - Parameters:
  ///   - fileDescriptor: The file descriptor of the socket.
  ///   - buffer: The buffer to receive into.
  ///   - addressCapacity: The capacity for the address of the sender, which is zero if it is not wanted.
  private func receiveDatagram(
    on fileDescriptor: CInt,
    into buffer: inout OutputRawSpan,
    addressCapacity: socklen_t
  ) async throws(IOError) -> (count: Int, sender: sockaddr_storage) {
    // The bytes are received into the free capacity of the buffer, which stays
    // alive while we are suspended since we hold the access to it.
    let freeCapacity = unsafe buffer.withUnsafeMutableBytes { bytes, initializedCount in
      unsafe UnsafeMutableRawBufferPointer(rebasing: bytes[initializedCount...])
    }

    // The kernel writes the message while we are suspended, so it lives in
    // storage that outlives the operation.
    let (count, sender) = try await unsafe withTemporaryAllocation(of: DatagramMessage.self, capacity: 1) {
      storage async throws(IOError) -> (Int, sockaddr_storage) in
      unsafe storage.append(DatagramMessage())
      var messages = unsafe storage.mutableSpan
      let message = messages.withUnsafeMutableBufferPointer { $0.baseAddress! }
      let header = unsafe DatagramMessage.configure(
        message,
        buffer: freeCapacity,
        addressLength: addressCapacity
      )
      let count = try await unsafe self.perform(
        .receiveMessage(socket: fileDescriptor, message: header)
      ) { continuation in
        .byteCount(continuation)
      }
      return unsafe (count, message.pointee.address)
    }

    // The kernel wrote into the free capacity of the buffer so we have to
    // update the initialized count now.
    unsafe buffer.withUnsafeMutableBytes { _, initializedCount in
      initializedCount += count
    }
    return (count, sender)
  }

  /// Sends the buffer as one datagram.
  ///
  /// - Parameters:
  ///   - fileDescriptor: The file descriptor of the socket.
  ///   - buffer: The bytes of the datagram.
  ///   - address: The address to send to, or `nil` to send to the peer that the socket is connected to.
  private func sendDatagram(
    on fileDescriptor: CInt,
    from buffer: RawSpan,
    to address: SocketAddress?
  ) async throws(IOError) -> Int {
    let bytes = buffer.withUnsafeBytes { unsafe $0 }

    return try await unsafe withTemporaryAllocation(of: DatagramMessage.self, capacity: 1) {
      storage async throws(IOError) -> Int in
      unsafe storage.append(DatagramMessage())
      var messages = unsafe storage.mutableSpan
      let message = messages.withUnsafeMutableBufferPointer { $0.baseAddress! }
      let addressLength = unsafe address?.write(into: &message.pointee.address) ?? 0
      let header = unsafe DatagramMessage.configure(
        message,
        buffer: UnsafeMutableRawBufferPointer(mutating: bytes),
        addressLength: addressLength
      )
      return try await unsafe self.perform(
        .sendMessage(socket: fileDescriptor, message: header)
      ) { continuation in
        .byteCount(continuation)
      }
    }
  }

  package func close(socket: consuming UDPSocket) async throws(IOError) {
    let fileDescriptor = socket.takeFileDescriptor()
    try await unsafe self.perform(.close(fileDescriptor: fileDescriptor)) { continuation in
      .void(continuation)
    }
  }

  /// Returns the address that the socket is bound to.
  ///
  /// This is how a socket that was bound to port zero reports the port the platform picked for it.
  ///
  /// - Parameter socket: The socket to return the address of.
  /// - Returns: The address that the socket is bound to.
  package func localAddress(of socket: borrowing UDPSocket) throws(IOError) -> SocketAddress {
    try StreamSocketSyscall.localAddress(socket.fileDescriptor)
  }
}
#endif
#endif
