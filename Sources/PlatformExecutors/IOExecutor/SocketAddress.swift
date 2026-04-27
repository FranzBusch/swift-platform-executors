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

#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif

/// A value-typed socket address.
///
/// A `SocketAddress` carries the platform `(sockaddr_storage, socklen_t)` pair
/// as a `Sendable` value so addresses can flow through ``IOExecutor/connect(fileHandle:address:)``
/// and ``IOExecutor/accept(fileHandle:)`` without exposing raw out-parameters
/// to the call site.
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
public struct SocketAddress: Sendable {
  /// The platform address storage.
  public var storage: sockaddr_storage

  /// The number of valid bytes in ``storage``.
  public var length: socklen_t

  /// Creates a socket address from platform storage and its length.
  ///
  /// - Parameters:
  ///   - storage: The platform `sockaddr_storage` value.
  ///   - length: The number of valid bytes in `storage`.
  public init(storage: sockaddr_storage, length: socklen_t) {
    self.storage = storage
    self.length = length
  }
}

#endif
