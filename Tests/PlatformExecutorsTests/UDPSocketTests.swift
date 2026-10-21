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
#if os(Linux) || os(FreeBSD) || canImport(Darwin)

import Testing
@_spi(ExperimentalScheduling) @_spi(ConcurrencyExecutors) @_spi(ExperimentalCustomExecutors) import _Concurrency
@_spi(ExperimentalScheduling) @_spi(ConcurrencyExecutors) @_spi(ExperimentalCustomExecutors) import PlatformExecutors

@Suite
struct UDPSocketTests {
  @available(anyAppleOS 27.0, *)
  private static let loopback = SocketAddress.v4(SocketAddress.V4(address: .loopback, port: 0))

  @Test(arguments: PThreadIOBackend.tested)
  @available(anyAppleOS 27.0, *)
  func bindReportsThePickedPort(ioBackend: PThreadIOBackend) async throws {
    try await PThreadExecutor.withExecutor(name: "UDPSocketTest", ioBackend: ioBackend) { executor in
      try await withOperationScheduler(executor) {
        let socket = try await UDPSocket.bind(to: Self.loopback)
        let address = try socket.localAddress
        try await socket.close()

        guard case .v4(let v4) = address else {
          Issue.record("Expected an IPv4 address but got \(address)")
          return
        }
        #expect(v4.address == .loopback)
        #expect(v4.port != 0)
      }
    }
  }

  @Test(arguments: PThreadIOBackend.tested)
  @available(anyAppleOS 27.0, *)
  func exchangeWithOperationScheduler(ioBackend: PThreadIOBackend) async throws {
    try await PThreadExecutor.withExecutor(name: "UDPSocketTest", ioBackend: ioBackend) { executor in
      try await withOperationScheduler(executor) {
        try await Self.exchange()
      }
    }
  }

  @Test(arguments: PThreadIOBackend.tested, [1, 4])
  @available(anyAppleOS 27.0, *)
  func exchangeWithPThreadTaskExecutorPreference(ioBackend: PThreadIOBackend, poolSize: Int) async throws {
    try await PThreadTaskExecutor.withExecutor(name: "UDPSocketTest", poolSize: poolSize, ioBackend: ioBackend) {
      executor in
      try await withTaskExecutorPreference(executor) {
        try await Self.exchange()
      }
    }
  }

  @Test(arguments: PThreadIOBackend.tested)
  @available(anyAppleOS 27.0, *)
  func receiveTruncatesALargerDatagram(ioBackend: PThreadIOBackend) async throws {
    try await PThreadExecutor.withExecutor(name: "UDPSocketTest", ioBackend: ioBackend) { executor in
      try await withOperationScheduler(executor) {
        var receiver = try await UDPSocket.bind(to: Self.loopback)
        var sender = try await UDPSocket.bind(to: Self.loopback)

        let payload = Array("0123456789".utf8)
        try await sender.send(payload, to: receiver.localAddress)
        let (datagram, _) = try await receiver.receive(maxCount: 4)
        #expect(datagram == Array("0123".utf8))

        try await sender.close()
        try await receiver.close()
      }
    }
  }

  @Test(arguments: PThreadIOBackend.tested, [1, 4])
  @available(anyAppleOS 27.0, *)
  func cancellingAPendingReceiveResumesWithCancelledAndKeepsTheSocket(
    ioBackend: PThreadIOBackend,
    poolSize: Int
  ) async throws {
    try await PThreadTaskExecutor.withExecutor(name: "UDPSocketTest", poolSize: poolSize, ioBackend: ioBackend) {
      executor in
      try await withTaskExecutorPreference(executor) {
        let payload = Array("Sent after the receive was cancelled".utf8)
        let (cancelled, cancelledContinuation) = AsyncStream.makeStream(of: Void.self)
        let (ready, readyContinuation) = AsyncStream.makeStream(of: Void.self)

        try await withThrowingTaskGroup { group in
          // A noncopyable value cannot be consumed out of a closure capture, so the
          // socket is moved into the child task through an optional.
          let bound = try await UDPSocket.bind(to: Self.loopback)
          let address = try bound.localAddress
          var receiverBox: UDPSocket? = bound
          group.addTask {
            var receiver = receiverBox.take()!
            readyContinuation.yield()

            // Nothing was sent yet, so the receive waits until the task is cancelled.
            let error = await #expect(throws: IOError.self) {
              _ = try await receiver.receive(maxCount: 1024)
            }
            #expect(error?.code == .cancelled)
            cancelledContinuation.yield()

            // The socket is still usable after the cancelled receive.
            let (datagram, _) = try await withTaskCancellationShield { () async throws(IOError) in
              try await receiver.receive(maxCount: 1024)
            }
            #expect(datagram == payload)
            try await receiver.close()
          }

          for await _ in ready {
            break
          }
          group.cancelAll()
          for await _ in cancelled {
            break
          }

          var sender = try await UDPSocket.bind(to: Self.loopback)
          try await sender.send(payload, to: address)
          try await sender.close()
          try await group.waitForAll()
        }
      }
    }
  }

  @Test(arguments: PThreadIOBackend.tested, [1, 4])
  @available(anyAppleOS 27.0, *)
  func connectedSocketExchangesWithItsPeer(ioBackend: PThreadIOBackend, poolSize: Int) async throws {
    try await PThreadTaskExecutor.withExecutor(name: "UDPSocketTest", poolSize: poolSize, ioBackend: ioBackend) {
      executor in
      try await withTaskExecutorPreference(executor) {
        var server = try await UDPSocket.bind(to: Self.loopback)
        var client = try await UDPSocket.connect(to: server.localAddress)

        try await client.write(Array("ping".utf8))
        let (request, requestSender) = try await server.receive(maxCount: 1024)
        #expect(request == Array("ping".utf8))
        #expect(requestSender == (try client.localAddress))

        try await server.send(Array("pong".utf8), to: requestSender)
        let reply = try await client.read(maxCount: 1024)
        #expect(reply == Array("pong".utf8))

        try await client.close()
        try await server.close()
      }
    }
  }

  @Test(arguments: PThreadIOBackend.tested, [1, 4])
  @available(anyAppleOS 27.0, *)
  func withSplitReceivesWhileSending(ioBackend: PThreadIOBackend, poolSize: Int) async throws {
    try await PThreadTaskExecutor.withExecutor(name: "UDPSocketTest", poolSize: poolSize, ioBackend: ioBackend) {
      executor in
      try await withTaskExecutorPreference(executor) {
        try await withThrowingTaskGroup { group in
          let bound = try await UDPSocket.bind(to: Self.loopback)
          let serverAddress = try bound.localAddress
          var serverBox: UDPSocket? = bound

          // The server echoes one datagram.
          group.addTask {
            var server = serverBox.take()!
            let (datagram, sender) = try await server.receive(maxCount: 1024)
            try await server.send(datagram, to: sender)
            try await server.close()
          }

          let client = try await UDPSocket.connect(to: serverAddress)
          let payload = Array("Echo me".utf8)
          let echoed = try await client.withSplit { receiveHalf, sendHalf in
            let (echoed, _) = try await withConcurrently {
              try await receiveHalf.read(maxCount: 1024)
            } _: {
              try await sendHalf.write(payload)
            }
            return echoed
          }
          try await client.close()
          try await group.waitForAll()
          #expect(echoed == payload)
        }
      }
    }
  }

  /// Sends a datagram to a socket, which sends a reply to the address it received the datagram from.
  @available(anyAppleOS 27.0, *)
  private static func exchange() async throws {
    var server = try await UDPSocket.bind(to: Self.loopback)
    var client = try await UDPSocket.bind(to: Self.loopback)
    let serverAddress = try server.localAddress
    let clientAddress = try client.localAddress

    try await client.send(Array("ping".utf8), to: serverAddress)
    let (request, requestSender) = try await server.receive(maxCount: 1024)
    #expect(request == Array("ping".utf8))
    #expect(requestSender == clientAddress)

    try await server.send(Array("pong".utf8), to: requestSender)
    let (reply, replySender) = try await client.receive(maxCount: 1024)
    #expect(reply == Array("pong".utf8))
    #expect(replySender == serverAddress)

    try await client.close()
    try await server.close()
  }
}

@available(anyAppleOS 27.0, *)
extension UDPSocket {
  /// Receives one datagram of at most `maxCount` bytes.
  fileprivate mutating func receive(maxCount: Int) async throws(IOError) -> ([UInt8], SocketAddress) {
    // The buffer has to stay alive while the receive is in flight so we heap alloc here.
    let buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: maxCount, alignment: 1)
    defer {
      unsafe buffer.deallocate()
    }

    var output = unsafe OutputRawSpan(buffer: buffer, initializedCount: 0)
    let (count, sender) = try await self.receive(into: &output)
    _ = unsafe output.finalize(for: buffer)
    return unsafe (Array(UnsafeRawBufferPointer(rebasing: buffer[..<count])), sender)
  }

  /// Reads one datagram of at most `maxCount` bytes from the peer of a connected socket.
  fileprivate mutating func read(maxCount: Int) async throws(IOError) -> [UInt8] {
    let buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: maxCount, alignment: 1)
    defer {
      unsafe buffer.deallocate()
    }

    var output = unsafe OutputRawSpan(buffer: buffer, initializedCount: 0)
    let count = try await self.read(into: &output)
    _ = unsafe output.finalize(for: buffer)
    return unsafe Array(UnsafeRawBufferPointer(rebasing: buffer[..<count]))
  }

  /// Writes the bytes as one datagram to the peer of a connected socket.
  fileprivate mutating func write(_ bytes: [UInt8]) async throws(IOError) {
    let buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: bytes.count, alignment: 1)
    defer {
      unsafe buffer.deallocate()
    }
    unsafe buffer.copyBytes(from: bytes)
    try await unsafe self.write(from: RawSpan(_unsafeBytes: UnsafeRawBufferPointer(buffer)))
  }

  /// Sends the bytes as one datagram.
  fileprivate mutating func send(_ bytes: [UInt8], to address: SocketAddress) async throws(IOError) {
    // The buffer has to stay alive while the send is in flight so we heap alloc here.
    let buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: bytes.count, alignment: 1)
    defer {
      unsafe buffer.deallocate()
    }
    unsafe buffer.copyBytes(from: bytes)
    try await unsafe self.send(from: RawSpan(_unsafeBytes: UnsafeRawBufferPointer(buffer)), to: address)
  }
}
@available(anyAppleOS 27.0, *)
extension UDPSocket.ReceiveHalf {
  /// Reads one datagram of at most `maxCount` bytes.
  fileprivate mutating func read(maxCount: Int) async throws(IOError) -> [UInt8] {
    let buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: maxCount, alignment: 1)
    defer {
      unsafe buffer.deallocate()
    }

    var output = unsafe OutputRawSpan(buffer: buffer, initializedCount: 0)
    let count = try await self.read(into: &output)
    _ = unsafe output.finalize(for: buffer)
    return unsafe Array(UnsafeRawBufferPointer(rebasing: buffer[..<count]))
  }
}

@available(anyAppleOS 27.0, *)
extension UDPSocket.SendHalf {
  /// Writes the bytes as one datagram.
  fileprivate mutating func write(_ bytes: [UInt8]) async throws(IOError) {
    let buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: bytes.count, alignment: 1)
    defer {
      unsafe buffer.deallocate()
    }
    unsafe buffer.copyBytes(from: bytes)
    try await unsafe self.write(from: RawSpan(_unsafeBytes: UnsafeRawBufferPointer(buffer)))
  }
}
#endif
#endif
