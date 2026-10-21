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

import Benchmark
import PlatformExecutors

/// The size of every datagram.
private let datagramSize = 512

/// The number of datagrams that the client sends before it waits for the server to acknowledge them.
///
/// The loopback interface drops datagrams when the receive buffer of the server is full, so the client
/// cannot send unboundedly many. The burst has to fit into the receive buffer.
private let burstSize = 32

/// Sends bursts of datagrams from a client to a server over the loopback interface.
///
/// The client sends a burst of datagrams without waiting, and the server acknowledges the burst with one
/// datagram once it received all of it. This measures the throughput of sending and receiving datagrams
/// with an operation per datagram, instead of the latency of a round trip.
///
/// - Parameters:
///   - numberOfBursts: The number of bursts that the client sends.
///   - ioBackend: The I/O backend that the executor's threads use.
///   - benchmark: The benchmark to measure.
@available(anyAppleOS 27.0, *)
func runUDPBurst(numberOfBursts: Int, ioBackend: PThreadIOBackend, benchmark: Benchmark) async throws {
  try await PThreadTaskExecutor.withExecutor(name: "UDPBurst", poolSize: 1, ioBackend: ioBackend) { executor in
    try await withTaskExecutorPreference(executor) {
      benchmark.startMeasurement()
      defer {
        benchmark.stopMeasurement()
      }

      try await withThrowingTaskGroup { group in
        var server = try await UDPSocket.bind(to: .v4(SocketAddress.V4(address: .loopback, port: 0)))
        let address = try server.localAddress
        group.addTask {
          try await runClient(connectingTo: address, numberOfBursts: numberOfBursts)
        }

        try await runServer(socket: &server, numberOfBursts: numberOfBursts)
        try await server.close()

        try await group.waitForAll()
      }
    }
  }
}

@available(anyAppleOS 27.0, *)
private func runClient(connectingTo address: SocketAddress, numberOfBursts: Int) async throws {
  var socket = try await UDPSocket.connect(to: address)

  let datagram = [UInt8](repeating: 0, count: datagramSize)
  let buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: datagramSize, alignment: 1)
  defer {
    buffer.deallocate()
  }

  for _ in 0..<numberOfBursts {
    for _ in 0..<burstSize {
      try await socket.write(from: datagram.span.bytes)
    }
    var output = OutputRawSpan(buffer: buffer, initializedCount: 0)
    try await socket.read(into: &output)
    _ = output.finalize(for: buffer)
  }

  try await socket.close()
}

@available(anyAppleOS 27.0, *)
private func runServer(socket: inout UDPSocket, numberOfBursts: Int) async throws {
  let buffer = UnsafeMutableRawBufferPointer.allocate(byteCount: datagramSize, alignment: 1)
  defer {
    buffer.deallocate()
  }

  for _ in 0..<numberOfBursts {
    var sender: SocketAddress?
    for _ in 0..<burstSize {
      var output = OutputRawSpan(buffer: buffer, initializedCount: 0)
      sender = try await socket.receive(into: &output).sender
      _ = output.finalize(for: buffer)
    }
    try await socket.send(from: RawSpan(_unsafeBytes: UnsafeRawBufferPointer(rebasing: buffer[..<1])), to: sender!)
  }
}
