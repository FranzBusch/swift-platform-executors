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

import Testing

@_spi(ExperimentalScheduling) @_spi(ConcurrencyExecutors) @_spi(ExperimentalCustomExecutors) import _Concurrency
@testable import PlatformExecutors

@Suite
struct JobQueueTests {
  /// A job that is only identified by its address. It never runs.
  struct TestJob {
    var job: UnownedJob
    var priority: UInt8

    var identity: UInt {
      unsafe unsafeBitCast(self.job, to: UInt.self)
    }
  }

  /// The priorities that the tests use. They cover every word of the bitmap of the queue.
  static let priorities: [UInt8] = [0x09, 0x11, 0x15, 0x19, 0x40, 0x80, 0xC0, 0xF0]

  @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
  static func makeJob(priority: UInt8) -> TestJob {
    // This is the undocumented function that the executor tests use to create jobs.
    let job = _swift_createJobForTestingOnly(priority: TaskPriority(rawValue: priority), {})
    return TestJob(job: UnownedJob(job), priority: priority)
  }

  @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
  static func identities(_ queue: inout JobQueue) -> [UInt] {
    var identities: [UInt] = []
    while let job = queue.pop() {
      identities.append(unsafe unsafeBitCast(job, to: UInt.self))
    }
    return identities
  }

  @Test
  @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
  func emptyQueue() {
    var queue = JobQueue()
    let isEmpty = queue.isEmpty
    let count = queue.count
    let job = queue.pop()
    #expect(isEmpty)
    #expect(count == 0)
    #expect(job == nil)
  }

  @Test
  @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
  func firstInFirstOutWithinAPriority() {
    var queue = JobQueue()
    let jobs = (0..<100).map { _ in Self.makeJob(priority: 0x15) }
    for job in jobs { queue.push(job.job) }
    let countBefore = queue.count
    let popped = Self.identities(&queue)
    let isEmpty = queue.isEmpty
    #expect(countBefore == 100)
    #expect(popped == jobs.map(\.identity))
    #expect(isEmpty)
  }

  @Test
  @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
  func higherPrioritiesComeFirst() {
    var queue = JobQueue()
    // Push in an order that is neither ascending nor descending.
    let jobs = (0..<64).map { Self.makeJob(priority: Self.priorities[($0 * 5) % Self.priorities.count]) }
    for job in jobs { queue.push(job.job) }

    // Priority descending, and in the order they were pushed within a priority.
    let expected = jobs.enumerated()
      .sorted { lhs, rhs in
        lhs.element.priority != rhs.element.priority
          ? lhs.element.priority > rhs.element.priority
          : lhs.offset < rhs.offset
      }
      .map(\.element.identity)
    let popped = Self.identities(&queue)
    #expect(popped == expected)
  }

  @Test
  @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
  func orderSurvivesGrowingAndWrappingAround() {
    var queue = JobQueue()
    var pushed: [TestJob] = []
    var popped: [UInt] = []

    // The ring buffer starts with 16 slots and doubles. Moving the head forward before it grows exercises the wrap
    // around of the copy.
    for round in 0..<10 {
      for _ in 0..<(7 + round * 13) {
        let job = Self.makeJob(priority: 0x15)
        pushed.append(job)
        queue.push(job.job)
      }
      for _ in 0..<(5 + round * 3) {
        if let job = queue.pop() { popped.append(unsafe unsafeBitCast(job, to: UInt.self)) }
      }
    }
    popped += Self.identities(&queue)

    #expect(popped == pushed.map(\.identity))
  }

  @Test
  @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
  func jobsThatArriveWhileOthersWaitKeepTheirPlace() {
    var queue = JobQueue()
    let low1 = Self.makeJob(priority: 0x11)
    let low2 = Self.makeJob(priority: 0x11)
    let high1 = Self.makeJob(priority: 0x19)
    queue.push(low1.job)
    queue.push(low2.job)
    let first = unsafe unsafeBitCast(queue.pop()!, to: UInt.self)
    #expect(first == low1.identity)

    // A higher priority job jumps ahead of the remaining low priority job, and a later low priority job goes behind it.
    queue.push(high1.job)
    let low3 = Self.makeJob(priority: 0x11)
    queue.push(low3.job)
    let rest = Self.identities(&queue)
    #expect(rest == [high1.identity, low2.identity, low3.identity])
  }
}
