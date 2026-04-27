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

/// Identifies the kind of I/O operation that produced an ``IOError``.
///
/// The kind is attached to errors thrown from the ``IOExecutor`` surface so
/// callers can route or log a failure without inspecting state outside the
/// error.
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
public enum IOOperationKind: Sendable, Hashable {
  /// A read from a file handle.
  case read

  /// A write to a file handle.
  case write

  /// An accept on a listening socket.
  case accept

  /// A connect on a client socket.
  case connect

  /// A close of a file handle.
  case close
}

#endif
