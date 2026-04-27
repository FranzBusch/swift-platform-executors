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

#if canImport(Glibc) || canImport(Darwin)

/// Runs the given closure with a platform-native I/O executor.
///
/// Creates a single-threaded event loop backed by epoll on Linux or kqueue on
/// macOS, sets it as the current task's preferred executor, and passes it to
/// the closure as an ``IOExecutor``. The event loop thread is stopped and
/// joined when the closure returns.
///
/// ```swift
/// try await withIOExecutor { executor in
///     try await TCPListener.bind(
///         host: "127.0.0.1", port: 8080, executor: executor
///     ) { listener in
///         // ...
///     }
/// }
/// ```
///
/// - Parameters:
///   - name: The name assigned to the executor's background thread.
///   - body: A closure that receives the executor for the duration of the
///     call.
/// - Returns: The value produced by `body`.
/// - Throws: Any error thrown by `body`, or any error encountered while
///   starting or stopping the executor's thread.
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
public func withIOExecutor<Return>(
  name: String = "io-executor",
  body: (any IOExecutor) async throws -> Return
) async throws -> Return {
  try await PThreadExecutor.withExecutor(name: name) { executor in
    try await withTaskExecutorPreference(executor) {
      try await body(executor)
    }
  }
}

#endif
