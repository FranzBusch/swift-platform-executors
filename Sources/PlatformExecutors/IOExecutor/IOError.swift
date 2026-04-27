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

/// An error from a platform I/O operation.
///
/// `IOError` is the unified error type returned from every ``IOExecutor`` call.
/// A high-level ``Code`` classifies the failure; companion properties carry the
/// remaining diagnostic information — the operation that failed, the raw POSIX
/// errno when applicable, the number of bytes already transferred when the
/// operation was cancelled, and the underlying error that produced the failure.
///
/// The struct shape lets the error grow new properties as the I/O surface
/// grows without breaking source compatibility, and it lets a single error
/// value carry enough context for a logger or observer to route it without
/// inspecting state outside the error.
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
public struct IOError: Error, Sendable {
  /// A high-level classification of the failure.
  public var code: Code

  /// A human-readable description of the failure.
  public var message: String

  /// The I/O operation that produced this error, if applicable.
  public var operation: IOOperationKind?

  /// The raw POSIX errno that produced this error, if applicable.
  ///
  /// Populated when ``code`` is ``Code/posix``.
  public var errno: CInt?

  /// The number of bytes already transferred when the operation was cancelled.
  ///
  /// Populated when ``code`` is ``Code/cancelled``. For pooled reads the
  /// kernel-transferred bytes are not surfaced and `transferred` is `0`.
  public var transferred: Int?

  /// The underlying error that caused this failure, if any.
  public var cause: (any Error)?

  /// The location in source code where the error was constructed.
  public var location: SourceLocation

  /// Creates a new `IOError`.
  ///
  /// - Parameters:
  ///   - code: A high-level classification of the failure.
  ///   - message: A human-readable description of the failure.
  ///   - operation: The I/O operation that produced this error, if applicable.
  ///   - errno: The raw POSIX errno, if applicable.
  ///   - transferred: The number of bytes already transferred when the
  ///     operation was cancelled, if applicable.
  ///   - cause: The underlying error that caused this failure, if any.
  ///   - location: The location in source code where the error was constructed.
  public init(
    code: Code,
    message: String,
    operation: IOOperationKind? = nil,
    errno: CInt? = nil,
    transferred: Int? = nil,
    cause: (any Error)? = nil,
    location: SourceLocation = .here()
  ) {
    self.code = code
    self.message = message
    self.operation = operation
    self.errno = errno
    self.transferred = transferred
    self.cause = cause
    self.location = location
  }
}

@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
extension IOError: CustomStringConvertible {
  public var description: String {
    var result = "\(self.code): \(self.message)"
    if let operation = self.operation {
      result += " (operation: \(operation))"
    }
    if let errno = self.errno {
      result += " (errno: \(errno))"
    }
    if let transferred = self.transferred {
      result += " (transferred: \(transferred))"
    }
    if let cause = self.cause {
      result += " (cause: \(cause))"
    }
    result += " (\(self.location))"
    return result
  }
}

@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
extension IOError: CustomDebugStringConvertible {
  public var debugDescription: String {
    var parts: [String] = []
    parts.append(String(reflecting: self.code))
    parts.append(String(reflecting: self.message))
    if let operation = self.operation {
      parts.append("operation: \(String(reflecting: operation))")
    }
    if let errno = self.errno {
      parts.append("errno: \(errno)")
    }
    if let transferred = self.transferred {
      parts.append("transferred: \(transferred)")
    }
    if let cause = self.cause {
      parts.append("cause: \(String(reflecting: cause))")
    }
    parts.append("at: \(String(reflecting: self.location))")
    return "IOError(\(parts.joined(separator: ", ")))"
  }
}

// MARK: - Convenience initializers

@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
extension IOError {
  /// Creates an error that wraps a POSIX errno.
  ///
  /// - Parameters:
  ///   - errno: The errno value returned from the failing syscall.
  ///   - operation: The I/O operation that produced the error.
  ///   - cause: An optional underlying error.
  ///   - location: The location in source code where the error was constructed.
  /// - Returns: An `IOError` with ``Code/posix``.
  public static func posix(
    errno: CInt,
    operation: IOOperationKind,
    cause: (any Error)? = nil,
    location: SourceLocation = .here()
  ) -> IOError {
    IOError(
      code: .posix,
      message: posixMessage(errno: errno),
      operation: operation,
      errno: errno,
      cause: cause,
      location: location
    )
  }

  /// Creates an error indicating the operation was cancelled.
  ///
  /// - Parameters:
  ///   - transferred: The number of bytes already transferred when the
  ///     cancellation was observed. Defaults to `0`.
  ///   - operation: The I/O operation that was cancelled, if applicable.
  ///   - cause: An optional underlying error.
  ///   - location: The location in source code where the error was constructed.
  /// - Returns: An `IOError` with ``Code/cancelled``.
  public static func cancelled(
    transferred: Int = 0,
    operation: IOOperationKind? = nil,
    cause: (any Error)? = nil,
    location: SourceLocation = .here()
  ) -> IOError {
    IOError(
      code: .cancelled,
      message: "The operation was cancelled.",
      operation: operation,
      transferred: transferred,
      cause: cause,
      location: location
    )
  }

  /// Creates an error indicating the file handle was closed underneath the
  /// operation.
  ///
  /// - Parameters:
  ///   - operation: The I/O operation that was in flight when the handle
  ///     was closed.
  ///   - cause: An optional underlying error.
  ///   - location: The location in source code where the error was constructed.
  /// - Returns: An `IOError` with ``Code/fileHandleClosed``.
  public static func fileHandleClosed(
    operation: IOOperationKind,
    cause: (any Error)? = nil,
    location: SourceLocation = .here()
  ) -> IOError {
    IOError(
      code: .fileHandleClosed,
      message: "The file handle was closed underneath the operation.",
      operation: operation,
      cause: cause,
      location: location
    )
  }

  /// Creates an error indicating the backend cannot service the operation.
  ///
  /// - Parameters:
  ///   - operation: The I/O operation that was attempted.
  ///   - cause: An optional underlying error.
  ///   - location: The location in source code where the error was constructed.
  /// - Returns: An `IOError` with ``Code/unsupportedOperation``.
  public static func unsupportedOperation(
    operation: IOOperationKind,
    cause: (any Error)? = nil,
    location: SourceLocation = .here()
  ) -> IOError {
    IOError(
      code: .unsupportedOperation,
      message: "The backend does not support this operation kind.",
      operation: operation,
      cause: cause,
      location: location
    )
  }

  /// Creates an error that wraps a backend-specific failure.
  ///
  /// - Parameters:
  ///   - cause: The underlying backend error.
  ///   - operation: The I/O operation that produced the error, if applicable.
  ///   - location: The location in source code where the error was constructed.
  /// - Returns: An `IOError` with ``Code/backend``.
  public static func backend(
    _ cause: any Error,
    operation: IOOperationKind? = nil,
    location: SourceLocation = .here()
  ) -> IOError {
    IOError(
      code: .backend,
      message: "A backend-specific failure occurred.",
      operation: operation,
      cause: cause,
      location: location
    )
  }
}

// MARK: - Code

@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
extension IOError {
  /// A high-level classification of an ``IOError``.
  ///
  /// Use the ``code`` property to switch on broad categories of failure
  /// without exhausting on associated values. Carry-on properties on
  /// ``IOError`` (``operation``, ``errno``, ``transferred``, ``cause``)
  /// supply the rest of the diagnostic context.
  public struct Code: Hashable, Sendable, CustomStringConvertible {
    @usableFromInline
    internal enum Wrapped: Hashable, Sendable, CustomStringConvertible {
      case posix
      case cancelled
      case fileHandleClosed
      case unsupportedOperation
      case backend

      @usableFromInline
      var description: String {
        switch self {
        case .posix: return "POSIX error"
        case .cancelled: return "Cancelled"
        case .fileHandleClosed: return "File handle closed"
        case .unsupportedOperation: return "Unsupported operation"
        case .backend: return "Backend error"
        }
      }
    }

    @usableFromInline
    internal var code: Wrapped

    @usableFromInline
    internal init(_ code: Wrapped) {
      self.code = code
    }

    public var description: String {
      String(describing: self.code)
    }

    /// A POSIX syscall failed with an errno.
    @inlinable
    public static var posix: Self {
      Self(.posix)
    }

    /// The operation was cancelled before completing.
    @inlinable
    public static var cancelled: Self {
      Self(.cancelled)
    }

    /// The file handle was closed while the operation was in flight.
    @inlinable
    public static var fileHandleClosed: Self {
      Self(.fileHandleClosed)
    }

    /// The backend cannot service this operation kind.
    @inlinable
    public static var unsupportedOperation: Self {
      Self(.unsupportedOperation)
    }

    /// A backend-specific failure occurred.
    @inlinable
    public static var backend: Self {
      Self(.backend)
    }
  }
}

// MARK: - SourceLocation

@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
extension IOError {
  /// A location within source code where an ``IOError`` was constructed.
  public struct SourceLocation: Sendable, Hashable, CustomStringConvertible {
    /// The function in which the error was constructed.
    public var function: String

    /// The file in which the error was constructed.
    public var file: String

    /// The line on which the error was constructed.
    public var line: Int

    /// A string representation of the source location.
    public var description: String {
      "\(self.function) (\(self.file):\(self.line))"
    }

    /// Creates a new source location.
    ///
    /// - Parameters:
    ///   - function: The name of the function.
    ///   - file: The file the location resides within.
    ///   - line: The line the location indicates.
    public init(function: String, file: String, line: Int) {
      self.function = function
      self.file = file
      self.line = line
    }

    /// Captures the location at the call site.
    ///
    /// - Parameters:
    ///   - function: The name of the calling function (default `#function`).
    ///   - file: The file ID of the call site (default `#fileID`).
    ///   - line: The line of the call site (default `#line`).
    /// - Returns: A `SourceLocation` capturing the call site.
    public static func here(
      function: String = #function,
      file: String = #fileID,
      line: Int = #line
    ) -> SourceLocation {
      SourceLocation(function: function, file: file, line: line)
    }
  }
}

// MARK: - POSIX message lookup

/// Returns a short human-readable description of a POSIX errno.
///
/// Calls `strerror` and falls back to a numeric description if the lookup fails.
@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
private func posixMessage(errno code: CInt) -> String {
  if let cString = strerror(code) {
    return String(cString: cString)
  }
  return "Unknown errno \(code)"
}

#endif
