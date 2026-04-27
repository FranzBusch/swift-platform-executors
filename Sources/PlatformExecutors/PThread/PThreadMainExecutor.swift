//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift.org open source project
//
// Copyright (c) 2025 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
// See https://swift.org/CONTRIBUTORS.txt for the list of Swift project authors
//
//===----------------------------------------------------------------------===//

#if os(Linux) || os(Android) || os(FreeBSD) || canImport(Darwin)

@_spi(ExperimentalScheduling) import _Concurrency
@_spi(ExperimentalCustomExecutors) import _Concurrency

/// A main executor that provides serial execution by taking over the current thread.
///
/// ## Usage
///
/// ```swift
/// // Create main executor on current thread
/// let mainExecutor = PThreadMainExecutor()
///
/// // Run the executor loop
/// try mainExecutor.run()
///
/// // Stop the executor from another context
/// mainExecutor.stop()
/// ```
@_spi(ExperimentalCustomExecutors)
@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
public final class PThreadMainExecutor: SerialExecutor, MainExecutor, @unchecked Sendable {
  private let pThreadExecutor: PThreadExecutor!

  /// Creates a new `PThreadMainExecutor` that takes control of the current thread.
  public init() {
    self.pThreadExecutor = PThreadExecutor()
  }

  public func enqueue(_ job: UnownedJob) {
    self.pThreadExecutor.enqueue(job)
  }

  public func run() throws {
    // We are taking over the current thread
    try self.pThreadExecutor.run { job in
      job.runSynchronously(on: self.asUnownedSerialExecutor())
    }
  }

  public func runUntil(_ condition: () -> Bool) throws {
    // Not needed for server use — just run until stopped
    try self.run()
  }

  public func stop() {
    // We are not waiting on the condition variable since it would result in a
    // dead lock due to us taking over the thread previously.
    _ = self.pThreadExecutor.stop()
  }
}

@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
extension PThreadMainExecutor: CustomStringConvertible {
  public var description: String {
    "PThreadMainExecutor(\(self.pThreadExecutor.threadDescription))"
  }
}
#endif
