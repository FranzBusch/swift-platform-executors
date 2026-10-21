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

#if ExperimentalAsyncStreaming
#if os(Linux) || os(FreeBSD) || canImport(Darwin)

import AsyncStreaming
import BasicContainers
import Testing
@_spi(ExperimentalScheduling) @_spi(ConcurrencyExecutors) @_spi(ExperimentalCustomExecutors) import _Concurrency
@_spi(ExperimentalScheduling) @_spi(ConcurrencyExecutors) @_spi(ExperimentalCustomExecutors) import PlatformExecutors

@Suite
struct TCPAsyncStreamingTests {
  @available(anyAppleOS 27.0, *)
  private static let loopback = SocketAddress.v4(SocketAddress.V4(address: .loopback, port: 0))

  /// Runs `client` against a server that echoes everything until the client shut down its write direction.
  @available(anyAppleOS 27.0, *)
  private static func withEchoServer<Result: Sendable>(
    client: @escaping @Sendable (SocketAddress) async throws -> Result
  ) async throws -> Result {
    try await PThreadExecutor.withExecutor(name: "TCPAsyncStreamingTest") { executor in
      try await withOperationScheduler(executor) {
        let listener = try await TCPListener.bind(to: Self.loopback)
        let address = try listener.localAddress

        let result = try await withThrowingTaskGroup(of: Result.self) { group in
          group.addTask {
            try await client(address)
          }

          let server = try await listener.accept()
          try await server.withSplit { readHalf, writeHalf in
            var finished = false
            while !finished {
              let chunk: [UInt8] = try await readHalf.read { buffer, finalElement in
                if finalElement != nil {
                  finished = true
                }
                var chunk = [UInt8]()
                for index in 0..<buffer.count {
                  chunk.append(buffer[index])
                }
                return chunk
              }
              if !chunk.isEmpty {
                try await writeHalf.write { buffer in
                  for byte in chunk {
                    buffer.append(byte)
                  }
                }
              }
            }
            try await writeHalf.shutdown()
          }
          try await server.close()

          return try await group.next()!
        }

        try await listener.close()
        return result
      }
    }
  }

  @Test
  @available(anyAppleOS 27.0, *)
  func echoThroughAsyncReaderAndAsyncWriter() async throws {
    let echoed = try await Self.withEchoServer { address in
      let connection = try await TCPConnection.connect(to: address)
      let echoed = try await connection.withSplit { readHalf, writeHalf in
        try await writeHalf.write { buffer in
          for byte in "hello".utf8 {
            buffer.append(byte)
          }
        }
        var echoed = [UInt8]()
        while echoed.count < 5 {
          try await readHalf.read { buffer, finalElement in
            #expect(finalElement == nil)
            for index in 0..<buffer.count {
              echoed.append(buffer[index])
            }
          }
        }
        try await writeHalf.shutdown()
        return echoed
      }
      try await connection.close()
      return echoed
    }
    #expect(String(decoding: echoed, as: UTF8.self) == "hello")
  }

  @Test
  @available(anyAppleOS 27.0, *)
  func readDeliversEndOfStreamAsFinalElementWithEmptyBuffer() async throws {
    let (count, sawFinalElement) = try await Self.withEchoServer { address in
      let connection = try await TCPConnection.connect(to: address)
      let result = try await connection.withSplit { readHalf, writeHalf in
        // The server echoes, and then closes its side once it saw the end of our stream.
        try await writeHalf.shutdown()
        return try await readHalf.read { buffer, finalElement in
          (buffer.count, finalElement != nil)
        }
      }
      try await connection.close()
      return result
    }
    #expect(count == 0)
    #expect(sawFinalElement)
  }

  @Test
  @available(anyAppleOS 27.0, *)
  func roundTripsMoreThanTheScratchBufferConcurrently() async throws {
    let payload = (0..<(1024 * 1024)).map { UInt8(truncatingIfNeeded: $0 &* 31) }
    let echoed = try await Self.withEchoServer { address in
      let connection = try await TCPConnection.connect(to: address)
      let echoed = try await connection.withSplit { readHalf, writeHalf in
        let (echoed, _) = try await withConcurrently {
          var echoed = [UInt8]()
          var finished = false
          while !finished {
            try await readHalf.read { buffer, finalElement in
              for index in 0..<buffer.count {
                echoed.append(buffer[index])
              }
              if finalElement != nil {
                finished = true
              }
            }
          }
          return echoed
        } _: {
          // Write in chunks, so that the writer's buffer is reused between writes.
          var offset = 0
          while offset < payload.count {
            let end = min(offset + 100_000, payload.count)
            try await writeHalf.write { buffer in
              for index in offset..<end {
                buffer.append(payload[index])
              }
            }
            offset = end
          }
          try await writeHalf.shutdown()
        }
        return echoed
      }
      try await connection.close()
      return echoed
    }
    #expect(echoed.count == payload.count)
    #expect(echoed == payload)
  }

  @Test
  @available(anyAppleOS 27.0, *)
  func callerAsyncReaderAndWriterRoundTrip() async throws {
    let echoed = try await Self.withEchoServer { address in
      let connection = try await TCPConnection.connect(to: address)
      let echoed = try await connection.withSplit { readHalf, writeHalf in
        let (echoed, _) = try await withConcurrently {
          var echoed = UniqueArray<UInt8>()
          while try await readHalf.read(into: &echoed) == nil {}
          var result = [UInt8]()
          for index in 0..<echoed.count {
            result.append(echoed[index])
          }
          return result
        } _: {
          var buffer = UniqueArray<UInt8>()
          for byte in "caller managed".utf8 {
            buffer.append(byte)
          }
          try await writeHalf.write(buffer: &buffer)
          let isEmpty = buffer.isEmpty
          #expect(isEmpty)
          try await writeHalf.shutdown()
        }
        return echoed
      }
      try await connection.close()
      return echoed
    }
    #expect(String(decoding: echoed, as: UTF8.self) == "caller managed")
  }

  @Test
  @available(anyAppleOS 27.0, *)
  func finishingAnOwnedWriteHalfEndsTheStreamOfThePeer() async throws {
    let (echoed, sawFinalElement) = try await Self.withEchoServer { address in
      let connection = try await TCPConnection.connect(to: address)
      let result = try await connection.withOwnedSplit { readHalf, writeHalf in
        var readHalf = readHalf
        var writeHalf = writeHalf
        try await writeHalf.write { buffer in
          for byte in "bye".utf8 {
            buffer.append(byte)
          }
        }
        // The halves are owned, so the writer can be finished, which shuts down the write direction. The echo server
        // then sees the end of our stream and ends its own.
        try await writeHalf.finish(finalElement: ())

        var echoed = [UInt8]()
        var sawFinalElement = false
        while !sawFinalElement {
          try await readHalf.read { buffer, finalElement in
            for index in 0..<buffer.count {
              echoed.append(buffer[index])
            }
            sawFinalElement = finalElement != nil
          }
        }
        return (echoed, sawFinalElement)
      }
      try await connection.close()
      return result
    }
    #expect(String(decoding: echoed, as: UTF8.self) == "bye")
    #expect(sawFinalElement)
  }
}

#endif
#endif
