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

#if os(Windows)
/// A platform-native handle that identifies an open file or socket.
///
/// On Windows the handle is an unsigned integer that the operating system
/// uses as an index into its handle table. On POSIX platforms the handle is
/// the small non-negative integer returned by `open(2)`, `socket(2)`, and
/// related syscalls.
public typealias PlatformFileHandle = UInt
#else
/// A platform-native handle that identifies an open file or socket.
///
/// On POSIX platforms the handle is the small non-negative integer returned
/// by `open(2)`, `socket(2)`, and related syscalls.
public typealias PlatformFileHandle = CInt
#endif

#endif
