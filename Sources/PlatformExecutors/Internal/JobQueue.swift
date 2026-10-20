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

@_spi(ExperimentalScheduling) @_spi(ConcurrencyExecutors) @_spi(ExperimentalCustomExecutors) import _Concurrency

/// The jobs that wait to run on an executor: the job with the highest priority comes first, and jobs with the same
/// priority come in the order they were pushed.
///
/// An executor runs almost all of its jobs with a single priority. A heap that compares jobs by priority and
/// by a sequence number has to ask the job for both on every comparison, which are calls into the Swift runtime, and it
/// compares a number of times that grows with the size of the queue. This queue keeps a first in first out queue for
/// every priority and a bitmap of the priorities that have jobs, so a push and a pop take constant time and read the
/// priority of a job once, when it is pushed.
@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
struct JobQueue: ~Copyable {
  /// A first in first out queue in a ring buffer. Its capacity is a power of two.
  private struct Ring {
    var storage = ContiguousArray<UnownedJob?>()
    var head = 0
    var count = 0

    mutating func push(_ job: UnownedJob) {
      if self.count == self.storage.count {
        self.grow()
      }
      self.storage[(self.head &+ self.count) & (self.storage.count &- 1)] = job
      self.count &+= 1
    }

    mutating func pop() -> UnownedJob {
      let job = unsafe self.storage[self.head].unsafelyUnwrapped
      self.storage[self.head] = nil
      self.head = (self.head &+ 1) & (self.storage.count &- 1)
      self.count &-= 1
      return job
    }

    private mutating func grow() {
      let capacity = max(self.storage.count * 2, 16)
      var storage = ContiguousArray<UnownedJob?>(repeating: nil, count: capacity)
      let mask = self.storage.count &- 1
      for index in 0..<self.count {
        storage[index] = self.storage[(self.head &+ index) & mask]
      }
      self.storage = storage
      self.head = 0
    }
  }

  /// The queues, indexed by the raw value of the priority.
  private var rings = ContiguousArray<Ring>(repeating: Ring(), count: 256)

  /// The priorities that have jobs: bit `priority % 64` of word `priority / 64`.
  private var nonEmpty: (UInt64, UInt64, UInt64, UInt64) = (0, 0, 0, 0)

  /// The number of jobs in the queue.
  private(set) var count = 0

  var isEmpty: Bool {
    self.count == 0
  }

  /// Adds a job behind the jobs with the same priority.
  mutating func push(_ job: UnownedJob) {
    let priority = Int(job.priority.rawValue)
    self.rings[priority].push(job)
    self.count &+= 1
    switch priority >> 6 {
    case 0: self.nonEmpty.0 |= 1 << UInt64(priority & 63)
    case 1: self.nonEmpty.1 |= 1 << UInt64(priority & 63)
    case 2: self.nonEmpty.2 |= 1 << UInt64(priority & 63)
    default: self.nonEmpty.3 |= 1 << UInt64(priority & 63)
    }
  }

  /// Removes the job with the highest priority that was pushed first.
  mutating func pop() -> UnownedJob? {
    let priority: Int
    if self.nonEmpty.3 != 0 {
      priority = 192 + 63 - self.nonEmpty.3.leadingZeroBitCount
    } else if self.nonEmpty.2 != 0 {
      priority = 128 + 63 - self.nonEmpty.2.leadingZeroBitCount
    } else if self.nonEmpty.1 != 0 {
      priority = 64 + 63 - self.nonEmpty.1.leadingZeroBitCount
    } else if self.nonEmpty.0 != 0 {
      priority = 63 - self.nonEmpty.0.leadingZeroBitCount
    } else {
      return nil
    }

    let job = self.rings[priority].pop()
    self.count &-= 1
    if self.rings[priority].count == 0 {
      switch priority >> 6 {
      case 0: self.nonEmpty.0 &= ~(1 << UInt64(priority & 63))
      case 1: self.nonEmpty.1 &= ~(1 << UInt64(priority & 63))
      case 2: self.nonEmpty.2 &= ~(1 << UInt64(priority & 63))
      default: self.nonEmpty.3 &= ~(1 << UInt64(priority & 63))
      }
    }
    return job
  }
}
