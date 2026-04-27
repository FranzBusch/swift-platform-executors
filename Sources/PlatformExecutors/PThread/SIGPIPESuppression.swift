//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-platform-executors open source project
//
// Copyright (c) 2026 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
//
//===----------------------------------------------------------------------===//

#if canImport(Glibc) || canImport(Darwin)

#if canImport(Glibc)
import Glibc

// Linux has no per-fd F_SETNOSIGPIPE, so suppress process-wide on first use.
private let _sigpipeIgnored: Bool = {
    _ = Glibc.signal(SIGPIPE, SIG_IGN)
    return true
}()

#elseif canImport(Darwin)
import Darwin
#endif

/// Suppresses SIGPIPE for the given file descriptor.
///
/// On Linux, installs a process-wide `SIG_IGN` handler on first call.
/// On Darwin, sets `F_SETNOSIGPIPE` on the individual descriptor.
func suppressSIGPIPE(descriptor fd: CInt) {
#if canImport(Glibc)
    _ = _sigpipeIgnored
#elseif canImport(Darwin)
    _ = Darwin.fcntl(fd, F_SETNOSIGPIPE, 1)
#endif
}

#endif  // canImport(Glibc) || canImport(Darwin)
