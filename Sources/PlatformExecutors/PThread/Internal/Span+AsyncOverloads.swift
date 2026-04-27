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
//
// Async-throws overloads of `withUnsafeBufferPointer` /
// `withUnsafeMutableBufferPointer` / `withUnsafeMutableBytes` for
// `Span`/`MutableSpan`/`MutableRawSpan`/`OutputSpan`. The standard library
// ships only the synchronous variants today; the typed `IOExecutor` shape
// (closure-form pooled reads, caller-owned reads that suspend on a syscall)
// needs the async variants so the suspension can happen inside the closure
// while the span borrow is still live.
//
// Implementation mirrors the synchronous overloads' memory-rebinding /
// pointer-derivation pattern, then awaits `body` instead of calling it
// synchronously. Reaches private members (`_pointer`, `_count`,
// `_rawValue`) via the `-disable-access-control` frontend flag declared in
// the package settings; the alternative would be to reach them via
// `unsafeBitCast`, but the access-control bypass is more readable.
//
// Remove this file once the stdlib ships async-throws overloads.
//
//===----------------------------------------------------------------------===//

import Builtin

extension Span {
    /// Borrows the span's storage as an `UnsafeBufferPointer` for the
    /// duration of an async closure. Mirrors the synchronous
    /// `withUnsafeBufferPointer(_:)` overload but lets `body` suspend.
    @_alwaysEmitIntoClient
    public func withUnsafeBufferPointerAsync<E: Error, Result: ~Copyable>(
        _ body: (_ buffer: UnsafeBufferPointer<Element>) async throws(E) -> Result
    ) async throws(E) -> Result {
        guard let pointer = unsafe _pointer, _count > 0 else {
            return try await unsafe body(.init(start: nil, count: 0))
        }
        // Manual memory rebinding to avoid recalculating alignment.
        let binding = Builtin.bindMemory(
            pointer._rawValue, count._builtinWordValue, Element.self
        )
        defer { Builtin.rebindMemory(pointer._rawValue, binding) }
        return try await unsafe body(.init(start: .init(pointer._rawValue), count: count))
    }

    /// Borrows the span's storage as an `UnsafeRawBufferPointer` for the
    /// duration of an async closure. Mirrors the synchronous
    /// `withUnsafeBytes(_:)` overload but lets `body` suspend.
    @_alwaysEmitIntoClient
    public func withUnsafeBytesAsync<E: Error, Result: ~Copyable>(
        _ body: (_ buffer: UnsafeRawBufferPointer) async throws(E) -> Result
    ) async throws(E) -> Result {
        let bytes = UnsafeRawBufferPointer(
            start: (_count == 0) ? nil : unsafe _pointer,
            count: _count &* MemoryLayout<Element>.stride
        )
        return try await unsafe body(bytes)
    }
}

extension MutableSpan {
    /// Borrows the span's storage as an `UnsafeMutableBufferPointer` for
    /// the duration of an async closure.
    @_alwaysEmitIntoClient
    @lifetime(self: copy self)
    public mutating func withUnsafeMutableBufferPointerAsync<
        E: Error, Result: ~Copyable
    >(
        _ body: (UnsafeMutableBufferPointer<Element>) async throws(E) -> Result
    ) async throws(E) -> Result {
        guard let pointer = unsafe _pointer, count > 0 else {
            return try await unsafe body(.init(start: nil, count: 0))
        }
        let binding = Builtin.bindMemory(
            pointer._rawValue, count._builtinWordValue, Element.self
        )
        defer { Builtin.rebindMemory(pointer._rawValue, binding) }
        return try await unsafe body(.init(start: .init(pointer._rawValue), count: count))
    }
}

extension OutputSpan {
    /// Hands the span's mutable buffer to an async closure along with an
    /// `inout initializedCount`. The closure may suspend while the buffer
    /// borrow is live; on return the span's `count` is updated to the
    /// `initializedCount` value.
    @_alwaysEmitIntoClient
    @lifetime(self: copy self)
    public mutating func withUnsafeMutableBufferPointerAsync<E: Error, R: ~Copyable>(
        _ body: (
            UnsafeMutableBufferPointer<Element>,
            _ initializedCount: inout Int
        ) async throws(E) -> R
    ) async throws(E) -> R {
        guard let start = unsafe _pointer, capacity > 0 else {
            let buffer = UnsafeMutableBufferPointer<Element>(_empty: ())
            var initializedCount = 0
            defer {
                _precondition(initializedCount == 0, "OutputSpan capacity overflow")
            }
            return try await unsafe body(buffer, &initializedCount)
        }
        let binding = Builtin.bindMemory(
            start._rawValue, capacity._builtinWordValue, Element.self
        )
        defer { Builtin.rebindMemory(start._rawValue, binding) }
        let buffer = unsafe UnsafeMutableBufferPointer<Element>(
            _uncheckedStart: .init(start._rawValue), count: capacity
        )
        var initializedCount = self._count
        defer {
            _precondition(
                0 <= initializedCount && initializedCount <= capacity,
                "OutputSpan capacity overflow"
            )
            self._count = initializedCount
        }
        return try await unsafe body(buffer, &initializedCount)
    }
}
