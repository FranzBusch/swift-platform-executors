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
public import AsyncStreaming
public import BasicContainers
public import ContainersPreview

/// Heap memory that stays alive while a read is in flight.
final class IOScratchBuffer: @unchecked Sendable {
  static let defaultCapacity = 16 * 1024

  let pointer: UnsafeMutableRawBufferPointer

  init(capacity: Int = IOScratchBuffer.defaultCapacity) {
    self.pointer = .allocate(byteCount: capacity, alignment: 16)
  }

  deinit {
    self.pointer.deallocate()
  }
}

/// The state that the `AsyncReader` conformance of ``TCPConnection/ReadHalf`` keeps between reads.
struct AsyncReadState: ~Copyable, Sendable {
  /// Lazily allocated, since most halves are read through ``TCPConnection/ReadHalf/read(into:)``.
  var scratch: IOScratchBuffer?

  /// Handed to the `body` of a read and reused by the next read.
  var buffer: UniqueArray<UInt8>?

  init() {
    self.scratch = nil
    self.buffer = nil
  }
}

/// The state that the `AsyncWriter` conformance of ``TCPConnection/WriteHalf`` keeps between writes.
struct AsyncWriteState: ~Copyable, Sendable {
  /// Handed to the `body` of a write and reused by the next write.
  var buffer: UniqueArray<UInt8>?

  init() {
    self.buffer = nil
  }
}

// MARK: - Reading

@available(anyAppleOS 27.0, *)
extension TCPConnection.ReadHalf {
  /// Reads once into the scratch memory and returns the number of bytes read.
  fileprivate mutating func readIntoScratch() async throws(IOError) -> (IOScratchBuffer, Int) {
    let scratch: IOScratchBuffer
    if let existing = self.asyncReadState.scratch {
      scratch = existing
    } else {
      scratch = IOScratchBuffer()
      self.asyncReadState.scratch = scratch
    }
    let pointer = unsafe scratch.pointer
    var output = unsafe OutputRawSpan(buffer: pointer, initializedCount: 0)
    let count: Int
    do throws(IOError) {
      count = try await self.read(into: &output)
    } catch {
      _ = unsafe output.finalize(for: pointer)
      throw error
    }
    _ = unsafe output.finalize(for: pointer)
    return (scratch, count)
  }
}

@available(anyAppleOS 27.0, *)
extension TCPConnection.ReadHalf: AsyncReader {
  public typealias ReadElement = UInt8
  public typealias Buffer = UniqueArray<UInt8>
  public typealias ReadFailure = IOError
  public typealias FinalElement = Void

  public mutating func read<Return: ~Copyable, Failure: Error>(
    body: (inout UniqueArray<UInt8>, consuming Void?) async throws(Failure) -> Return
  ) async throws(EitherError<IOError, Failure>) -> Return {
    let scratch: IOScratchBuffer
    let count: Int
    do throws(IOError) {
      (scratch, count) = try await self.readIntoScratch()
    } catch {
      throw .first(error)
    }

    var buffer = self.asyncReadState.buffer.take() ?? UniqueArray<UInt8>()
    buffer.removeAll(keepingCapacity: true)
    if count > 0 {
      let bytes = UnsafeRawBufferPointer(rebasing: scratch.pointer[..<count])
      buffer.append(addingCount: count) { output in
        for byte in bytes {
          output.append(byte)
        }
      }
    }

    do throws(Failure) {
      let result =
        if count == 0 {
          try await body(&buffer, ())
        } else {
          try await body(&buffer, nil)
        }
      self.asyncReadState.buffer = consume buffer
      return result
    } catch {
      self.asyncReadState.buffer = consume buffer
      throw .second(error)
    }
  }
}

@available(anyAppleOS 27.0, *)
extension TCPConnection.ReadHalf: CallerAsyncReader {
  public mutating func read<Buffer: RangeReplaceableContainer<UInt8> & ~Copyable>(
    into buffer: inout Buffer
  ) async throws(IOError) -> Void? where Buffer.Element: ~Copyable {
    let (scratch, count) = try await self.readIntoScratch()
    if count == 0 {
      return ()
    }
    for index in 0..<count {
      buffer.append(scratch.pointer.load(fromByteOffset: index, as: UInt8.self))
    }
    return nil
  }
}

// MARK: - Writing

@available(anyAppleOS 27.0, *)
extension TCPConnection.WriteHalf: AsyncWriter {
  public typealias WriteElement = UInt8
  public typealias Buffer = UniqueArray<UInt8>
  public typealias WriteFailure = IOError
  public typealias FinalElement = Void

  public mutating func write<Return: ~Copyable, Failure: Error>(
    _ body: (inout UniqueArray<UInt8>) async throws(Failure) -> Return
  ) async throws(EitherError<IOError, Failure>) -> Return {
    var buffer = self.asyncWriteState.buffer.take() ?? UniqueArray<UInt8>()
    buffer.removeAll(keepingCapacity: true)

    let result: Return
    do throws(Failure) {
      result = try await body(&buffer)
    } catch {
      self.asyncWriteState.buffer = consume buffer
      throw .second(error)
    }

    do throws(IOError) {
      if buffer.count > 0 {
        try await self.write(from: buffer.span.bytes)
      }
    } catch {
      self.asyncWriteState.buffer = consume buffer
      throw .first(error)
    }
    self.asyncWriteState.buffer = consume buffer
    return result
  }

  public consuming func finish(finalElement: consuming Void) async throws(IOError) {
    try await self.shutdown()
  }
}

@available(anyAppleOS 27.0, *)
extension TCPConnection.WriteHalf: CallerAsyncWriter {
  public mutating func write<Buffer: RangeReplaceableContainer<UInt8> & ~Copyable>(
    buffer: inout Buffer
  ) async throws(IOError) where Buffer.Element: ~Copyable {
    var index = buffer.startIndex
    while index < buffer.endIndex {
      let span = buffer.nextSpan(after: &index)
      if !span.isEmpty {
        try await self.write(from: span.bytes)
      }
    }
    buffer.removeAll()
  }

  public consuming func finish<Buffer: RangeReplaceableContainer<UInt8> & ~Copyable>(
    buffer: inout Buffer,
    finalElement: consuming Void
  ) async throws(IOError) where Buffer.Element: ~Copyable {
    try await self.write(buffer: &buffer)
    try await self.shutdown()
  }
}
#endif
