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
/// A UDP socket.
///
/// A socket is either bound to a local address with ``bind(to:)`` or connected to a peer with
/// ``connect(to:)``:
///
/// - A bound socket sends datagrams to, and receives datagrams from, any peer with ``send(from:to:)``
///   and ``receive(into:)``.
/// - A connected socket only exchanges datagrams with its peer, and the platform drops the datagrams of
///   everyone else. It uses ``write(from:)`` and ``read(into:)``.
///
/// A socket uniquely owns the underlying platform resource, so it cannot be copied. It must be closed
/// explicitly with ``close()`` since closing is an asynchronous operation, and destroying a socket without
/// closing it traps.
///
/// ```swift
/// var socket = try await UDPSocket.bind(to: address)
/// let (count, sender) = try await socket.receive(into: &datagram)
/// try await socket.send(from: reply, to: sender)
/// try await socket.close()
/// ```
@available(anyAppleOS 27.0, *)
public struct UDPSocket: ~Copyable, Sendable {
  /// The scheduler that services the socket.
  private let scheduler: any UDPSocketOperationScheduler

  /// The scheduler's handle of the socket, or `nil` once the socket is closed.
  private var socket: (any Sendable)?

  private init<Scheduler: UDPSocketOperationScheduler>(
    scheduler: Scheduler,
    socket: Scheduler.UDPSocket
  ) {
    self.scheduler = scheduler
    self.socket = socket
  }

  /// Resolves the scheduler that services new sockets.
  private static func resolveScheduler() -> any UDPSocketOperationScheduler {
    guard
      let scheduler = OperationSchedulerPreference.resolve(as: (any UDPSocketOperationScheduler).self)
    else {
      fatalError(
        "No operation scheduler for UDP sockets found. Use withOperationScheduler or a task executor preference."
      )
    }
    return scheduler
  }

  /// Creates a new socket and binds it to the given address.
  ///
  /// - Parameter address: The address to bind to. Binding to port zero lets the platform pick a free port,
  ///   which ``localAddress`` reports afterwards.
  /// - Returns: The bound socket.
  public static func bind(to address: SocketAddress) async throws(IOError) -> UDPSocket {
    try await Self.bind(to: address, on: Self.resolveScheduler())
  }

  private static func bind<Scheduler: UDPSocketOperationScheduler>(
    to address: SocketAddress,
    on scheduler: Scheduler
  ) async throws(IOError) -> UDPSocket {
    let socket = try await scheduler.bind(to: address)
    return UDPSocket(scheduler: scheduler, socket: socket)
  }

  /// Creates a new socket and connects it to the given address.
  ///
  /// The platform picks the local address of the socket, which ``localAddress`` reports afterwards. Connecting
  /// does not send anything, so it succeeds even if nobody listens at the address. A datagram that the peer
  /// refuses can instead make a later ``read(into:)`` fail.
  ///
  /// - Parameter address: The address of the peer.
  /// - Returns: The connected socket.
  public static func connect(to address: SocketAddress) async throws(IOError) -> UDPSocket {
    try await Self.connect(to: address, on: Self.resolveScheduler())
  }

  private static func connect<Scheduler: UDPSocketOperationScheduler>(
    to address: SocketAddress,
    on scheduler: Scheduler
  ) async throws(IOError) -> UDPSocket {
    let socket = try await scheduler.connectUDP(to: address)
    return UDPSocket(scheduler: scheduler, socket: socket)
  }

  /// The address that the socket is bound to locally.
  public var localAddress: SocketAddress {
    get throws(IOError) {
      try Self.localAddress(socket: self.socket, on: self.scheduler)
    }
  }

  private static func localAddress<Scheduler: UDPSocketOperationScheduler>(
    socket: (any Sendable)?,
    on scheduler: Scheduler
  ) throws(IOError) -> SocketAddress {
    try scheduler.localAddress(of: Self.socket(socket, of: scheduler))
  }

  /// Receives one datagram into the free capacity of the given buffer.
  ///
  /// The bytes are appended to the buffer. A datagram that is larger than the free capacity of the buffer is
  /// truncated, and its remaining bytes are discarded.
  ///
  /// - Parameter buffer: The buffer to receive into.
  /// - Returns: The number of bytes received and the address of the sender.
  @discardableResult
  public mutating func receive(
    into buffer: inout OutputRawSpan
  ) async throws(IOError) -> (count: Int, sender: SocketAddress) {
    try await Self.receive(into: &buffer, socket: self.socket, on: self.scheduler)
  }

  private static func receive<Scheduler: UDPSocketOperationScheduler>(
    into buffer: inout OutputRawSpan,
    socket: (any Sendable)?,
    on scheduler: Scheduler
  ) async throws(IOError) -> (count: Int, sender: SocketAddress) {
    try await scheduler.receive(socket: Self.socket(socket, of: scheduler), into: &buffer)
  }

  /// Sends the bytes of the given buffer as one datagram to the given address.
  ///
  /// - Parameters:
  ///   - buffer: The bytes of the datagram.
  ///   - address: The address to send the datagram to.
  public mutating func send(from buffer: RawSpan, to address: SocketAddress) async throws(IOError) {
    try await Self.send(from: buffer, to: address, socket: self.socket, on: self.scheduler)
  }

  private static func send<Scheduler: UDPSocketOperationScheduler>(
    from buffer: RawSpan,
    to address: SocketAddress,
    socket: (any Sendable)?,
    on scheduler: Scheduler
  ) async throws(IOError) {
    // A datagram is sent as a whole or not at all.
    _ = try await scheduler.send(socket: Self.socket(socket, of: scheduler), from: buffer, to: address)
  }

  /// Reads one datagram from the peer of a connected socket into the free capacity of the given buffer.
  ///
  /// The bytes are appended to the buffer. A datagram that is larger than the free capacity of the buffer is
  /// truncated, and its remaining bytes are discarded.
  ///
  /// - Important: This is only meant for a socket that was created with ``connect(to:)``. On a socket that
  ///   is not connected the platform fails the operation or picks the sender arbitrarily.
  ///
  /// - Parameter buffer: The buffer to read into.
  /// - Returns: The number of bytes received.
  @discardableResult
  public mutating func read(into buffer: inout OutputRawSpan) async throws(IOError) -> Int {
    try await Self.read(into: &buffer, socket: self.socket, on: self.scheduler)
  }

  private static func read<Scheduler: UDPSocketOperationScheduler>(
    into buffer: inout OutputRawSpan,
    socket: (any Sendable)?,
    on scheduler: Scheduler
  ) async throws(IOError) -> Int {
    try await scheduler.read(socket: Self.socket(socket, of: scheduler), into: &buffer)
  }

  /// Sends the bytes of the given buffer as one datagram to the peer of a connected socket.
  ///
  /// - Important: This is only meant for a socket that was created with ``connect(to:)``. The platform
  ///   fails the operation on a socket that is not connected.
  ///
  /// - Parameter buffer: The bytes of the datagram.
  public mutating func write(from buffer: RawSpan) async throws(IOError) {
    try await Self.write(from: buffer, socket: self.socket, on: self.scheduler)
  }

  private static func write<Scheduler: UDPSocketOperationScheduler>(
    from buffer: RawSpan,
    socket: (any Sendable)?,
    on scheduler: Scheduler
  ) async throws(IOError) {
    // A datagram is sent as a whole or not at all.
    _ = try await scheduler.write(socket: Self.socket(socket, of: scheduler), from: buffer)
  }

  /// Closes the socket.
  public consuming func close() async throws(IOError) {
    try await Self.close(self.socket.take(), on: self.scheduler)
  }

  private static func close<Scheduler: UDPSocketOperationScheduler>(
    _ socket: (any Sendable)?,
    on scheduler: Scheduler
  ) async throws(IOError) {
    let socket = socket as! Scheduler.UDPSocket
    try await withTaskCancellationShield { () async throws(IOError) -> Void in
      try await scheduler.close(socket: socket)
    }
  }

  /// Returns the handle of the socket as the handle type of the given scheduler.
  ///
  /// - Parameters:
  ///   - socket: The erased handle of the socket.
  ///   - scheduler: The scheduler that services the socket.
  private static func socket<Scheduler: UDPSocketOperationScheduler>(
    _ socket: (any Sendable)?,
    of scheduler: Scheduler
  ) -> Scheduler.UDPSocket {
    // The handle was created by this scheduler, and it is only `nil` once
    // the socket is closed, which consumes it. So the cast cannot fail.
    socket as! Scheduler.UDPSocket
  }

  deinit {
    if self.socket != nil {
      fatalError("A UDPSocket was destroyed without being closed.")
    }
  }
}

// MARK: - Receiving and sending concurrently

@available(anyAppleOS 27.0, *)
extension UDPSocket {
  /// The receiving direction of a socket, lent out by ``UDPSocket/withSplit(_:)``.
  public struct ReceiveHalf: ~Copyable, ~Escapable, Sendable {
    /// The scheduler that services the socket.
    private let scheduler: any UDPSocketOperationScheduler

    /// The scheduler's handle of the socket.
    private let socket: (any Sendable)?

    @_lifetime(borrow owner)
    fileprivate init(_ owner: borrowing UDPSocket) {
      self.scheduler = owner.scheduler
      self.socket = owner.socket
    }

    /// Receives one datagram into the free capacity of the given buffer.
    ///
    /// The bytes are appended to the buffer. A datagram that is larger than the free capacity of the buffer
    /// is truncated, and its remaining bytes are discarded.
    ///
    /// - Parameter buffer: The buffer to receive into.
    /// - Returns: The number of bytes received and the address of the sender.
    @discardableResult
    public mutating func receive(
      into buffer: inout OutputRawSpan
    ) async throws(IOError) -> (count: Int, sender: SocketAddress) {
      try await UDPSocket.receive(into: &buffer, socket: self.socket, on: self.scheduler)
    }

    /// Reads one datagram from the peer of a connected socket into the free capacity of the given buffer.
    ///
    /// - Important: This is only meant for a socket that was created with ``UDPSocket/connect(to:)``.
    ///
    /// - Parameter buffer: The buffer to read into.
    /// - Returns: The number of bytes received.
    @discardableResult
    public mutating func read(into buffer: inout OutputRawSpan) async throws(IOError) -> Int {
      try await UDPSocket.read(into: &buffer, socket: self.socket, on: self.scheduler)
    }
  }

  /// The sending direction of a socket, lent out by ``UDPSocket/withSplit(_:)``.
  public struct SendHalf: ~Copyable, ~Escapable, Sendable {
    /// The scheduler that services the socket.
    private let scheduler: any UDPSocketOperationScheduler

    /// The scheduler's handle of the socket.
    private let socket: (any Sendable)?

    @_lifetime(borrow owner)
    fileprivate init(_ owner: borrowing UDPSocket) {
      self.scheduler = owner.scheduler
      self.socket = owner.socket
    }

    /// Sends the bytes of the given buffer as one datagram to the given address.
    ///
    /// - Parameters:
    ///   - buffer: The bytes of the datagram.
    ///   - address: The address to send the datagram to.
    public mutating func send(from buffer: RawSpan, to address: SocketAddress) async throws(IOError) {
      try await UDPSocket.send(from: buffer, to: address, socket: self.socket, on: self.scheduler)
    }

    /// Sends the bytes of the given buffer as one datagram to the peer of a connected socket.
    ///
    /// - Important: This is only meant for a socket that was created with ``UDPSocket/connect(to:)``.
    ///
    /// - Parameter buffer: The bytes of the datagram.
    public mutating func write(from buffer: RawSpan) async throws(IOError) {
      try await UDPSocket.write(from: buffer, socket: self.socket, on: self.scheduler)
    }
  }

  /// Lends the receiving and the sending direction of the socket to the given closure.
  ///
  /// The halves can be used concurrently allowing protocols to send and receive independently. Their
  /// methods are `mutating`, so that every half is only used by one task at a time.
  ///
  /// ```swift
  /// try await socket.withSplit { receiveHalf, sendHalf in
  ///   try await withConcurrently {
  ///     try await receiveReplies(from: &receiveHalf)
  ///   } _: {
  ///     try await sendHalf.send(from: request, to: address)
  ///   }
  /// }
  /// ```
  ///
  /// - Important: The halves cannot escape the closure, so the socket keeps owning the platform resource
  ///   and must still be closed afterwards.
  ///
  /// - Parameter body: The closure that the halves are lent to.
  /// - Returns: The value that the closure returned.
  public nonisolated(nonsending) func withSplit<Return, Failure: Error>(
    _ body: nonisolated(nonsending) (inout ReceiveHalf, inout SendHalf) async throws(Failure) -> Return
  ) async throws(Failure) -> Return {
    var receiveHalf = ReceiveHalf(self)
    var sendHalf = SendHalf(self)
    return try await body(&receiveHalf, &sendHalf)
  }
}
#endif
