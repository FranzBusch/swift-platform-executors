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

import BasicContainers

/// A pool that manages reusable I/O read buffers.
@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
protocol IOBufferPool: ~Copyable {
    /// The size of each buffer in bytes.
    var bufferSize: Int { get }

    /// Acquires a buffer from the pool.
    mutating func acquire() -> UniqueArray<UInt8>

    /// Returns a buffer to the pool for reuse.
    mutating func release(_ buffer: consuming UniqueArray<UInt8>)
}

/// A fixed-size buffer pool backed by `UniqueArray<UniqueArray<UInt8>>`.
///
/// Pre-allocates buffers on first use. If the pool is empty, allocates
/// a fresh buffer. Returned buffers are kept for reuse up to a maximum
/// pool size.
@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
struct FixedIOBufferPool: ~Copyable, IOBufferPool {
    let bufferSize: Int
    private let maxPoolSize: Int
    private var freeBuffers: UniqueArray<UniqueArray<UInt8>>

    init(bufferSize: Int = 65536, initialCount: Int = 16, maxPoolSize: Int = 64) {
        self.bufferSize = bufferSize
        self.maxPoolSize = maxPoolSize
        self.freeBuffers = UniqueArray()
        for _ in 0..<initialCount {
            var buf = UniqueArray<UInt8>()
            buf.reserveCapacity(bufferSize)
            freeBuffers.append(buf)
        }
    }

    mutating func acquire() -> UniqueArray<UInt8> {
        if !freeBuffers.isEmpty {
            return freeBuffers.removeLast()
        }
        var buf = UniqueArray<UInt8>()
        buf.reserveCapacity(bufferSize)
        return buf
    }

    mutating func release(_ buffer: consuming UniqueArray<UInt8>) {
        if freeBuffers.count < maxPoolSize {
            freeBuffers.append(buffer)
        }
    }
}
