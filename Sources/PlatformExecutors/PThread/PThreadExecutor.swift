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
//===----------------------------------------------------------------------===//
//
// This source file is part of the SwiftNIO open source project
//
// Copyright (c) 2017-2024 Apple Inc. and the SwiftNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of SwiftNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

#if os(Linux) || os(Android) || os(FreeBSD) || canImport(Darwin)
internal import Synchronization

#if canImport(Darwin)
import Dispatch
#endif

@_spi(ExperimentalScheduling) public import _Concurrency

/// A task executor that is backed by a single dedicated thread with platform-optimized I/O event handling.
///
/// `PThreadExecutor` provides a high-performance, single-threaded execution environment for Swift Concurrency tasks.
/// It maintains thread affinity by ensuring all operations execute on a dedicated background thread, making it ideal for
/// actor executors and scenarios requiring ordered processing.
///
/// ## Usage
///
/// ```swift
/// // Use with task executor preference
/// let executor = PThreadExecutor(name: "ProcessingThread")
/// await withTaskExecutorPreference(executor) {
///     // Work executes on dedicated thread
/// }
/// ```
@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
package final class PThreadExecutor: TaskExecutor, @unchecked Sendable {
  #if canImport(Darwin)
  typealias Selector = KQueueSelector
  #elseif canImport(Glibc)
  typealias Selector = EpollSelector
  #else
  #error("Unsupported platform")
  #endif


  // MARK: - I/O cancellation

  /// A cancellation request for a specific submission.
  ///
  /// Submitted from a task's `withTaskCancellationHandler` `onCancel` handler;
  /// processed by ``run(runJobSynchronously:)`` after each `epoll_wait` so the
  /// dequeue runs serialized on the executor thread (no lock-on-mutate of the
  /// per-fd queues from off-thread). Each submission gets a unique
  /// `submissionID` allocated by ``allocateSubmissionID()`` so cancellation
  /// can pick the exact pending entry even when several concurrent
  /// submissions share the same fd/direction.
  struct PendingIOCancellation {
    var fd: PlatformFileHandle
    var direction: PendingDirection
    var submissionID: UInt64
  }

  /// This is the state that is accessed from multiple threads; hence, it must be protected via a lock.
  private struct MultiThreadedState: ~Copyable {
    /// Indicates if we are running and about to pop more jobs. If this is true then we don't have to wake the selector.
    var pendingJobPop = false
    /// The condition variable that gets signalled once the thread is stopped.
    var stopConditionVariable: ConditionVariable<Bool>? = nil
    /// I/O cancellation requests enqueued from task cancellation handlers.
    var pendingIOCancellations: ContiguousArray<PendingIOCancellation> = []
    /// This is the queue of enqueued jobs that we have to execute in the order they got enqueued.
    var jobs: NonCopyablePriorityQueue<UnownedJob> = {
      guard #available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, visionOS 9999, *) else {
        return .init(compare: compareJobsByPriorityAndID)
      }
      return .init(compare: compareJobsByPriorityAndSequenceNumber)
    }()
    /// This is the queue of enqueued jobs for the continuous clock.
    var continuousClockJobs: NonCopyablePriorityQueue<(ContinuousClock.Instant, UnownedJob)> = {
      guard #available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, visionOS 9999, *) else {
        return .init(compare: compareJobsByContinuousClockInstantAndPriorityAndID(lhs:rhs:))
      }
      return .init(compare: compareJobsByContinuousClockInstantAndPriorityAndSequenceNumber(lhs:rhs:))
    }()
    /// This is the queue of enqueued jobs for the suspending clock.
    var suspendingClockJobs: NonCopyablePriorityQueue<(SuspendingClock.Instant, UnownedJob)> = {
      guard #available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, visionOS 9999, *) else {
        return .init(compare: compareJobsBySuspendingClockInstantAndPriorityAndID(lhs:rhs:))
      }
      return .init(compare: compareJobsBySuspendingClockInstantAndPriorityAndSequenceNumber(lhs:rhs:))
    }()
  }

  /// This is the state that is bound to this thread.
  struct ThreadBoundState: ~Copyable {
    /// Indicates if the executor took over the calling thread
    fileprivate var tookOverThread: Bool = false

    /// The executor's selector.
    var selector: Selector {
      _read {
        yield self._selector
      }
      _modify {
        yield &self._selector
      }
    }

    /// The jobs that are next in line to be executed.
    fileprivate var nextExecutedJobs: ContiguousArray<UnownedJob> {
      _read {
        yield self._nextExecutedJobs
      }
      _modify {
        yield &self._nextExecutedJobs
      }
    }

    /// This method can be called from off thread so we are not asserting here.
    mutating func wakeupSelector() throws {
      try self._selector.wakeup()
    }

    /// The backing storage for the selector.
    ///
    /// This is a force try since there really is no way to handle these errors and this should never fail.
    var _selector = try! Selector()

    /// The I/O buffer pool for read operations.
    var ioBufferPool = FixedIOBufferPool()

    /// The backing storage of the next executed jobs.
    fileprivate var _nextExecutedJobs: ContiguousArray<UnownedJob>

    fileprivate init(_nextExecutedJobs: consuming ContiguousArray<UnownedJob>) {
      self._nextExecutedJobs = _nextExecutedJobs
    }
  }

  /// This is the state that is accessed from multiple threads; hence, it is protected via a lock.
  ///
  /// - Note:In the future we could use an MPSC queue and atomics here.
  private let _multiThreadedState = Mutex(MultiThreadedState())

  /// This is the state that is accessed from the thread backing the executor.
  private var _threadBoundState: ThreadBoundState

  /// The executor's thread. Stored separately from _threadBoundState so
  /// it can be read from any thread for onExecutor checks without
  /// exclusivity conflicts.
  private var _executorThread: Thread?

  /// The next sequence number of an enqueued jobs.
  private let sequenceNumber = Atomic<UInt64>(0)

  /// Monotonically-allocated submission ids for I/O operations. Used by
  /// ``EpollSelector/cancelSubmission(fd:direction:submissionID:)`` to find
  /// the exact pending entry on cancel even when multiple submissions
  /// share a fd/direction.
  private let _submissionIDCounter = Atomic<UInt64>(0)

  /// The amount of jobs to process in a single executor tick.
  /// This is a static var since those optimize better
  private static var jobsBatchSize: Int {
    4096
  }

  internal var threadDescription: String {
    return self._executorThread?.description ?? "not running"
  }

  /// Returns if we are currently running on the executor.
  private var onExecutor: Bool {
    return self._executorThread?.isCurrent ?? false
  }

  /// Creates a new platform-native task executor.
  ///
  /// This method creates a task executor backed by a dedicated pthread and ensures proper
  /// thread lifecycle management. The executor's thread will be automatically stopped and
  /// joined when the body closure completes, ensuring no thread leaks.
  ///
  /// - Parameters:
  ///   - name: The name assigned to the executor's background thread.
  ///   - body: A closure that gets access to the task executor for the duration of execution.
  /// - Returns: The value returned by the body closure.
  package nonisolated(nonsending) static func withExecutor<Return, Failure: Error>(
    name: String,
    body: (PThreadExecutor) async throws(Failure) -> Return
  ) async throws(Failure) -> Return {
    do {
      return try await self._withExecutor(
        name: name,
        taskExecutor: nil,
        serialExecutor: nil,
        body: body
      )
    } catch {
      throw error as! Failure
    }
  }

  // For some reason using typed throws here trips over the compiler
  // and it is not able to reason that the thrown error inside asyncDo is a Failure
  internal nonisolated(nonsending) static func _withExecutor<Return>(
    name: String,
    taskExecutor: UnownedTaskExecutor?,
    serialExecutor: UnownedSerialExecutor?,
    body: (PThreadExecutor) async throws -> Return
  ) async rethrows -> Return {
    let executor = PThreadExecutor(
      name: name,
      serialExecutor: serialExecutor,
      taskExecutor: taskExecutor
    )

    return try await asyncDo {
      try await body(executor)
    } finally: {
      executor.shutdown()
    }
  }

  internal convenience init(
    name: String,
    serialExecutor: UnownedSerialExecutor?,
    taskExecutor: UnownedTaskExecutor?
  ) {
    self.init()

    let conditionVariable = ConditionVariable(true)
    let thread = Thread.spawnAndRun(name: name) {
      do {
        // Block until we've set the thread in the thread bound state
        conditionVariable.wait {
          return !$0
        } block: {
          _ in
        }

        // Signal that we've started running
        conditionVariable.signal { $0.toggle() }

        // It is incredibly important that we pass the right task executor
        // to the run methods otherwise the Concurrency runtime will re-enqueue
        // the task over and over again. If this executor is part of a thread pool
        // then we must pass the pool as the executor.
        if let taskExecutor {
          try self.run { job in
            job.runSynchronously(on: taskExecutor)
          }
        } else if let serialExecutor {
          try self.run { job in
            job.runSynchronously(on: serialExecutor)
          }
        } else {
          try self.run { job in
            job.runSynchronously(on: self.asUnownedTaskExecutor())
          }
        }
      } catch {
        // We fatalError here because the only reasons this can be hit is if the underlying kqueue/epoll give us
        // errors that we cannot handle which is an unrecoverable error for us.
        fatalError("Unexpected error while running SelectableEventLoop: \(error).")
      }
    }

    self._executorThread = consume thread

    // Signal that we've set the thread in the thread bound state
    conditionVariable.signal { $0.toggle() }

    // Block until we've started running
    conditionVariable.wait {
      $0
    } block: { _ in
    }
  }

  internal init() {
    self._threadBoundState = .init(
      _nextExecutedJobs: ContiguousArray()
    )
  }

  deinit {
    precondition(
      self._multiThreadedState.withLock { $0.jobs.queue.isEmpty },
      "PThreadExecutor had left over jobs when deiniting."
    )
  }

  package func enqueue(_ job: consuming ExecutorJob) {
    let isOnExecutor = self.onExecutor
    let unownedJob = UnownedJob(job)
    self.modifyMultiThreadedStateAndWakeUpIfNeeded { state in
      state.jobs.push(unownedJob)
      let depth = state.jobs.queue.count
      // print("[E enq] onExec=\(isOnExecutor) queueDepth=\(depth)")
    }
  }

  internal func stop() -> ConditionVariable<Bool> {
    let conditionVariable = ConditionVariable(false)
    self.modifyMultiThreadedStateAndWakeUpIfNeeded { state in
      state.stopConditionVariable = conditionVariable
    }
    return conditionVariable
  }

  internal func shutdown() {
    let stopConditionVariable = self.stop()
    stopConditionVariable.wait {
      $0
    } block: { _ in
    }
    guard let thread = self._executorThread.take() else {
      fatalError("Executor already shutdown")
    }

    thread.join()
  }

  private func modifyMultiThreadedStateAndWakeUpIfNeeded(body: (inout MultiThreadedState) -> Void) {
    if self.onExecutor {
      // We are in the executor so we can just modify the state.
      self._multiThreadedState.withLock { state in
        body(&state)
      }
    } else {
      let shouldWakeSelector = self._multiThreadedState.withLock { state in
        body(&state)
        guard state.pendingJobPop else {
          // We have to wake the selector and we are going to store that we are about to do that.
          state.pendingJobPop = true
          return true
        }
        // There is already a next tick scheduled so we don't have to wake the selector.
        return false
      }

      // We only need to wake up the selector if we're not in the executor. If we're in the executor already,
      // we're running a job already which means that we'll check at least once more if there are other jobs to run.
      // While we had the lock we also checked whether the executor was _already_ going to be woken.
      // This saves us a syscall on hot loops.
      //
      // In the future we'll use an MPSC queue here and that will complicate things, so we may get some spurious wakeups,
      // but as long as we're using the big dumb lock we can make this optimization safely.
      if shouldWakeSelector {
        // Nothing we can do really if we fail to wake the selector
        try? self._threadBoundState.wakeupSelector()
      }
    }
  }

  package func isIsolatingCurrentContext() -> Bool? {
    return self.onExecutor
  }

  private func assertOnExecutor() {
    assert(self.onExecutor)
  }

  private func preconditionOnExecutor() {
    precondition(self.onExecutor)
  }

  /// Wake the `Selector` which means `Selector.whenReady(...)` will unblock.
  internal func _wakeupSelector() throws {
    try self._threadBoundState.selector.wakeup()
  }

  /// Start processing the jobs and handle any I/O.
  ///
  /// This method will continue running and blocking if needed.
  internal func run(runJobSynchronously: (UnownedJob) -> Void) throws {
    if self._executorThread == nil {
      self._executorThread = Thread.current
      self._threadBoundState.tookOverThread = true
    }
    self.assertOnExecutor()

    // This is the outer loop that we use to block on our selector
    // and check if we should stop
    var stopConditionVariable: ConditionVariable<Bool>? = nil
    defer {
      stopConditionVariable?.signal { $0.toggle() }
    }
    while true {
      var moreJobsQueued = false
      var nextContinuousClockDeadline: ContinuousClock.Instant?
      var nextSuspendingClockDeadline: SuspendingClock.Instant?
      var innerLoopIterations = 0
      var totalJobsRun = 0

      // This is the inner loop that processes one tick at a time. It can run
      // multiple times without blocking on the selector if there are many jobs
      // to processes or jobs are enqueued during a tick.
      while true {
        innerLoopIterations += 1
        (stopConditionVariable, moreJobsQueued, nextContinuousClockDeadline, nextSuspendingClockDeadline) = self
          ._multiThreadedState.withLock { state in
            // We were flagged to stop so we need to exit this loop
            if let stopConditionVariable = state.stopConditionVariable {
              state.stopConditionVariable = nil
              return (stopConditionVariable, false, nil, nil)
            }
            // We got some jobs that we should execute. Let's copy them over so we can
            // give up the lock.
            let (moreJobsQueued, nextContinuousClockDeadline, nextSuspendingClockDeadline) = Self._popJobsLocked(
              jobs: &state.jobs,
              continuousClockJobs: &state.continuousClockJobs,
              suspendingClockJobs: &state.suspendingClockJobs,
              jobsCopy: &self._threadBoundState.nextExecutedJobs,
              batchSize: Self.jobsBatchSize
            )

            if self._threadBoundState.nextExecutedJobs.isEmpty {
              // We got no jobs to execute so we will block and need to be woken up.
              assert(moreJobsQueued == false)
              state.pendingJobPop = false
            }
            return (nil, moreJobsQueued, nextContinuousClockDeadline, nextSuspendingClockDeadline)
          }

        if stopConditionVariable != nil {
          // We need to stop now and break out of the inner loop
          break
        }

        if self._threadBoundState.nextExecutedJobs.isEmpty {
          // There are no more jobs to execute so we have to block now
          break
        }

        let jobCount = self._threadBoundState.nextExecutedJobs.count
        totalJobsRun += jobCount
        // print("[E run] \(jobCount) jobs (totalThisTick=\(totalJobsRun))")
        for job in self._threadBoundState.nextExecutedJobs {
          runJobSynchronously(job)
        }

        // Remove all the just executed jobs but keep the capacity.
        self._threadBoundState.nextExecutedJobs.removeAll(keepingCapacity: true)
      }

      if stopConditionVariable != nil {
        // We need to stop now and need to break out of the outer loop
        break
      }

      let strategy = self.currentSelectorStrategy(
        moreJobsQueued: moreJobsQueued,
        nextContinuousClockDeadline: nextContinuousClockDeadline,
        nextSuspendingClockDeadline: nextSuspendingClockDeadline
      )

      // print("[E epoll] blocking strategy=\(strategy) innerIter=\(innerLoopIterations) jobsRun=\(totalJobsRun)")

      // Let's wait on the selector until an event happens
      try self._threadBoundState.selector.whenReady(
        strategy: strategy
      )
      // print("[E epoll] unblocked")

      // Process any I/O cancellation requests before running jobs.
      // This ensures a stale cancellation can never affect a newer wait,
      // because the task that would register the newer wait hasn't run yet.
      let cancellations = self._multiThreadedState.withLock { state -> ContiguousArray<PendingIOCancellation> in
        guard !state.pendingIOCancellations.isEmpty else {
          return []
        }
        let result = state.pendingIOCancellations
        state.pendingIOCancellations.removeAll(keepingCapacity: true)
        return result
      }
      for cancellation in cancellations {
        self._threadBoundState.selector.cancelSubmission(
          fd: cancellation.fd,
          direction: cancellation.direction,
          submissionID: cancellation.submissionID
        )
      }

      // Our selector unblocked and we are going to pop some jobs
      self._multiThreadedState.withLock {
        $0.pendingJobPop = true
      }
    }
  }

  private static func _popJobsLocked(
    jobs: inout NonCopyablePriorityQueue<UnownedJob>,
    continuousClockJobs: inout NonCopyablePriorityQueue<(ContinuousClock.Instant, UnownedJob)>,
    suspendingClockJobs: inout NonCopyablePriorityQueue<(SuspendingClock.Instant, UnownedJob)>,
    jobsCopy: inout ContiguousArray<UnownedJob>,
    batchSize: Int
  ) -> (Bool, ContinuousClock.Instant?, SuspendingClock.Instant?) {
    // We expect empty jobsCopy, to put a new batch of tasks into
    assert(jobsCopy.isEmpty)

    var moreJobsToConsider = !jobs.queue.isEmpty
    var moreContinuousClockJobsToConsider = !continuousClockJobs.queue.isEmpty
    var moreSuspendingClockJobsToConsider = !suspendingClockJobs.queue.isEmpty

    guard moreJobsToConsider || moreContinuousClockJobsToConsider || moreSuspendingClockJobsToConsider else {
      // There are no jobs to consider.
      return (false, nil, nil)
    }

    // We only fetch the time one time as this may be expensive and is generally good enough as if we miss anything we will just do a non-blocking select again anyway.
    let continuousClockNow = ContinuousClock.now
    let suspendingClockNow = SuspendingClock.now
    var nextContinuousClockDeadline: ContinuousClock.Instant?
    var nextSuspendingClockDeadline: SuspendingClock.Instant?

    while moreJobsToConsider || moreContinuousClockJobsToConsider || moreSuspendingClockJobsToConsider {
      // We pick one job per iteration of the loop.
      // This prevents one queue starving the other.
      if moreJobsToConsider, jobsCopy.count < batchSize, let job = jobs.pop() {
        jobsCopy.append(job)
      } else {
        moreJobsToConsider = false
      }

      if moreContinuousClockJobsToConsider, jobsCopy.count < batchSize, let job = continuousClockJobs.peek() {
        if continuousClockNow.duration(to: job.0) <= .nanoseconds(0) {
          _ = continuousClockJobs.pop()
          jobsCopy.append(job.1)
        } else {
          nextContinuousClockDeadline = job.0
          moreContinuousClockJobsToConsider = false
        }
      } else {
        moreContinuousClockJobsToConsider = false
      }

      if moreSuspendingClockJobsToConsider, jobsCopy.count < batchSize, let job = suspendingClockJobs.peek() {
        if suspendingClockNow.duration(to: job.0) <= .nanoseconds(0) {
          _ = suspendingClockJobs.pop()
          jobsCopy.append(job.1)
        } else {
          nextSuspendingClockDeadline = job.0
          moreSuspendingClockJobsToConsider = false
        }
      } else {
        moreSuspendingClockJobsToConsider = false
      }
    }

    return (!jobs.queue.isEmpty, nextContinuousClockDeadline, nextSuspendingClockDeadline)
  }

  private func currentSelectorStrategy(
    moreJobsQueued: Bool,
    nextContinuousClockDeadline: ContinuousClock.Instant?,
    nextSuspendingClockDeadline: SuspendingClock.Instant?,
  ) -> SelectorStrategy {
    guard !moreJobsQueued else {
      // There are more jobs queued without a deadline so we just need to select all events again
      return .now
    }

    let continuousClockNow = ContinuousClock.now
    let suspendingClockNow = SuspendingClock.now
    let nextContinuousClockReady = nextContinuousClockDeadline.flatMap { continuousClockNow.duration(to: $0) }
    let nextSuspendingClockReady = nextSuspendingClockDeadline.flatMap { suspendingClockNow.duration(to: $0) }

    switch (nextContinuousClockReady, nextSuspendingClockReady) {
    case (.some(let nextContinuousClockReady), .some(let nextSuspendingClockReady)):
      guard nextContinuousClockReady <= .nanoseconds(0) || nextSuspendingClockReady <= .nanoseconds(0) else {
        return .blockUntilTimeout(
          continuousClockInstant: nextContinuousClockDeadline,
          suspendingClockInstant: nextSuspendingClockDeadline
        )
      }
      // Something is ready to be processed just do a non-blocking select of events.
      return .now
    case (.some(let nextContinuousClockReady), .none):
      guard nextContinuousClockReady <= .nanoseconds(0) else {
        return .blockUntilTimeout(
          continuousClockInstant: nextContinuousClockDeadline,
          suspendingClockInstant: nextSuspendingClockDeadline
        )
      }
      // Something is ready to be processed just do a non-blocking select of events.
      return .now
    case (.none, .some(let nextSuspendingClockReady)):
      guard nextSuspendingClockReady <= .nanoseconds(0) else {
        return .blockUntilTimeout(
          continuousClockInstant: nextContinuousClockDeadline,
          suspendingClockInstant: nextSuspendingClockDeadline
        )
      }
      // Something is ready to be processed just do a non-blocking select of events.
      return .now
    case (.none, .none):
      // No jobs to handle so just block.
      return .block
    }
  }
}

// #if !canImport(Darwin)
// @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, visionOS 9999, *)
// extension PThreadExecutor: SchedulingExecutor {
//   package var asSchedulingExecutor: SchedulingExecutor? {
//     return self
//   }
//
//   package func enqueue<C: Clock>(
//     _ job: consuming ExecutorJob,
//     at instant: C.Instant,
//     tolerance: C.Duration?,
//     clock: C
//   ) {
//     job.sequenceNumber =
//       self.sequenceNumber.wrappingAdd(
//         1,
//         ordering: .relaxed
//       ).newValue
//     switch instant {
//     case let instant as ContinuousClock.Instant:
//       let unownedJob = UnownedJob(job)
//       self.modifyMultiThreadedStateAndWakeUpIfNeeded { state in
//         state.continuousClockJobs.push((instant, unownedJob))
//       }
//     case let instant as SuspendingClock.Instant:
//       let unownedJob = UnownedJob(job)
//       self.modifyMultiThreadedStateAndWakeUpIfNeeded { state in
//         state.suspendingClockJobs.push((instant, unownedJob))
//       }
//     default:
//       clock.enqueue(
//         job,
//         on: self,
//         at: instant,
//         tolerance: tolerance
//       )
//     }
//   }
// }
// #endif

@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
extension PThreadExecutor: CustomStringConvertible {
  package var description: String {
    "PThreadExecutor(\(self._executorThread?.description ?? "not running"))"
  }
}

// MARK: - I/O registration support

#if canImport(Glibc) || canImport(Darwin)
#if canImport(Glibc)
import Glibc
#elseif canImport(Darwin)
import Darwin
#endif
import ContainersPreview
import BasicContainers

// The IOExecutor extension is shared across Glibc and Darwin via conditional
// imports above. The two shims below hide the per-module qualifier so the
// extension body doesn't need a `#if` at every syscall site.

@inline(__always)
private func _systemClose(_ fd: CInt) -> CInt {
#if canImport(Glibc)
    return Glibc.close(fd)
#elseif canImport(Darwin)
    return Darwin.close(fd)
#endif
}

@inline(__always)
private func _systemConnect(_ fd: CInt, _ addr: UnsafePointer<sockaddr>, _ len: socklen_t) -> CInt {
#if canImport(Glibc)
    return Glibc.connect(fd, addr, len)
#elseif canImport(Darwin)
    return Darwin.connect(fd, addr, len)
#endif
}

@available(macOS 26.0, iOS 26.0, watchOS 26.0, tvOS 26.0, visionOS 26.0, *)
extension PThreadExecutor: IOExecutor {

    // MARK: - Internal package helpers

    package func registerIO(fd: CInt) throws {
        suppressSIGPIPE(descriptor: fd)
        try _threadBoundState.selector.registerIO(fd: fd)
    }

    package func deregisterIO(fd: CInt) {
        _threadBoundState.selector.deregisterIO(fd: fd)
    }

    package func wakeupSelector() throws {
        try _threadBoundState.selector.wakeup()
    }

    /// Allocates a fresh I/O submission id, used to disambiguate
    /// pending entries on the per-fd queue from the cancellation path.
    private func allocateSubmissionID() -> UInt64 {
        return self._submissionIDCounter.wrappingAdd(1, ordering: .relaxed).newValue
    }

    /// Enqueues a cancellation for a specific submission and wakes the
    /// selector so the next `whenReady` iteration processes it.
    private func cancelPendingIO(
        fd: PlatformFileHandle,
        direction: PendingDirection,
        submissionID: UInt64
    ) {
        self._multiThreadedState.withLock { state in
            state.pendingIOCancellations.append(
                PendingIOCancellation(fd: fd, direction: direction, submissionID: submissionID)
            )
        }
        // Always wake the selector. Cancellations are processed at the top of
        // each `whenReady` iteration, so we must unblock the wait even when
        // the cancel call originates from the executor thread.
        try? self._threadBoundState.wakeupSelector()
    }

    // MARK: - Submission helpers

    /// Lazy-registers the file handle on first use.
    ///
    /// The first I/O operation on a fresh file handle registers it with the
    /// kernel selector and installs SIGPIPE suppression. Subsequent
    /// operations on the same handle find an existing registration and
    /// return immediately.
    ///
    /// A cheap `fcntl(F_GETFD)` runs first to surface invalid handles as a
    /// typed `IOError` rather than letting the kernel-level syscall
    /// (`epoll_ctl` / `kevent`) hit its EBADF-trap precondition.
    private func ensureRegistered(
        _ fd: PlatformFileHandle,
        operation: IOOperationKind
    ) throws(IOError) {
        if _threadBoundState.selector.ioRegistrations[fd] != nil { return }
        if fcntl(fd, F_GETFD) < 0 {
            throw IOError.posix(errno: errno, operation: operation)
        }
        suppressSIGPIPE(descriptor: fd)
        do {
            try _threadBoundState.selector.registerIO(fd: fd)
        } catch let err as POSIXError {
            throw IOError.posix(errno: err.errnoCode, operation: operation)
        } catch {
            throw IOError.backend(error, operation: operation)
        }
    }

    /// Suspends on a single submission and returns its result.
    ///
    /// The file handle is registered on first use. Cancellation is routed
    /// through the per-submission cancel path so the dispatch loop can pop
    /// the pending entry off the per-fd queue and resume the continuation
    /// with a typed cancellation error.
    private func submitOnce(
        operation: LoweredIOOperation
    ) async throws(IOError) -> IOOperationResult {
        let fd = operation.fileHandle
        try self.ensureRegistered(fd, operation: operation.kind)
        let submissionID = self.allocateSubmissionID()
        let direction = operation.direction
        do {
            return try await withTaskCancellationHandler {
                try await withUnsafeThrowingContinuation {
                    (cont: UnsafeContinuation<IOOperationResult, IOError>) in
                    self._threadBoundState.selector.submit(
                        operation,
                        submissionID: submissionID,
                        continuation: cont
                    )
                    // Wake the selector so it observes the new interest mask.
                    try? self._threadBoundState.selector.wakeup()
                }
            } onCancel: {
                self.cancelPendingIO(
                    fd: fd,
                    direction: direction,
                    submissionID: submissionID
                )
            }
        } catch let err as IOError {
            throw err
        } catch {
            throw .backend(error)
        }
    }

    // MARK: - IOExecutor conformance — typed methods

    public func read<Return: ~Copyable>(
        fileHandle fd: PlatformFileHandle,
        body: (inout UniqueArray<UInt8>) async throws -> sending Return
    ) async throws(IOError) -> sending Return {
        // Pool-buffer release is hand-rolled on every exit path because Swift's
        // `defer` cannot consume a `~Copyable` capture: the compiler rejects
        // "missing reinitialization of closure capture after consume" because
        // `defer` would have to leave the variable initialized for fallthrough.
        // Each release site is `removeAll() + release()`.
        let bufferSize = _threadBoundState.ioBufferPool.bufferSize
        var readBuf = _threadBoundState.ioBufferPool.acquire()
        do {
            try await readBuf.append(count: bufferSize) { (outputSpan: inout OutputSpan<UInt8>) -> Void in
                try await unsafe outputSpan.withUnsafeMutableBufferPointerAsync { ptr, initializedCount in
                    let result = try await self.submitOnce(
                        operation: .read(
                            fd: fd,
                            buffer: unsafe UnsafeMutableRawBufferPointer(
                                start: ptr.baseAddress, count: ptr.count
                            )
                        )
                    )
                    switch result {
                    case .read(let n):
                        if n > 0 { initializedCount = n }
                    default:
                        // The selector should never return a non-`.read` result
                        // for a `.read` submission; surface it as a typed error
                        // rather than trapping.
                        throw IOError.unsupportedOperation(operation: .read)
                    }
                }
            }
        } catch let err as IOError {
            readBuf.removeAll()
            _threadBoundState.ioBufferPool.release(readBuf)
            throw err
        } catch {
            readBuf.removeAll()
            _threadBoundState.ioBufferPool.release(readBuf)
            throw .backend(error)
        }

        let returnValue: Return
        do {
            returnValue = try await body(&readBuf)
        } catch let err as IOError {
            readBuf.removeAll()
            _threadBoundState.ioBufferPool.release(readBuf)
            throw err
        } catch {
            readBuf.removeAll()
            _threadBoundState.ioBufferPool.release(readBuf)
            throw .backend(error)
        }
        readBuf.removeAll()
        _threadBoundState.ioBufferPool.release(readBuf)
        return returnValue
    }

    public func read<Buffer: RangeReplaceableContainer<UInt8> & ~Copyable>(
        fileHandle fd: PlatformFileHandle,
        into buffer: inout Buffer
    ) async throws(IOError) where Buffer.Element: ~Copyable {
        // Lower the syscall against a pool-leased `UniqueArray<UInt8>` whose
        // `@_alwaysEmitIntoClient @inline(__always)` `append(count:initializingWith:)`
        // accepts an async closure via inlining, then bulk-copy the kernel's
        // bytes into the caller's buffer with `append(copying: Span<UInt8>)`.
        // The intermediate is a single memcpy. Appending directly into
        // `buffer` does not compile because async-via-inlining does not pass
        // through generic protocol-witness dispatch; the protocol's witness
        // contract is checked at the call site as synchronous.
        //
        // `append(copying:)` is non-throwing for the conformers that ship
        // today (it traps on capacity overflow). The wrap-and-rethrow is
        // there to recover the pool buffer should a future conformer throw.
        let chunk = _threadBoundState.ioBufferPool.bufferSize
        var pooled = _threadBoundState.ioBufferPool.acquire()
        do {
            try await pooled.append(count: chunk) { (outputSpan: inout OutputSpan<UInt8>) -> Void in
                try await unsafe outputSpan.withUnsafeMutableBufferPointerAsync { ptr, initializedCount in
                    let freeCount = ptr.count - initializedCount
                    guard freeCount > 0 else { return }
                    let freeStart = unsafe ptr.baseAddress!.advanced(by: initializedCount)
                    let result = try await self.submitOnce(
                        operation: .read(
                            fd: fd,
                            buffer: unsafe UnsafeMutableRawBufferPointer(start: freeStart, count: freeCount)
                        )
                    )
                    switch result {
                    case .read(let n):
                        if n > 0 { initializedCount += n }
                    default:
                        throw IOError.unsupportedOperation(operation: .read)
                    }
                }
            }
            if pooled.count > 0 {
                buffer.append(copying: pooled.span)
            }
        } catch let err as IOError {
            pooled.removeAll()
            _threadBoundState.ioBufferPool.release(pooled)
            throw err
        } catch {
            pooled.removeAll()
            _threadBoundState.ioBufferPool.release(pooled)
            throw .backend(error)
        }
        pooled.removeAll()
        _threadBoundState.ioBufferPool.release(pooled)
    }

    public func write<Buffer: RangeReplaceableContainer<UInt8> & ~Copyable>(
        fileHandle fd: PlatformFileHandle,
        from buffer: borrowing Buffer
    ) async throws(IOError) where Buffer.Element: ~Copyable {
        // Walk the buffer span-by-span. Most conforming containers expose a
        // single contiguous span, but multi-segment containers (deques) yield
        // their bytes across several spans; iterate `nextSpan` until we have
        // covered the whole buffer.
        var index = buffer.startIndex
        while index != buffer.endIndex {
            let span = buffer.nextSpan(after: &index, maximumCount: .max)
            try await self.writeAll(fileHandle: fd, span: span)
        }
    }

    /// Writes the entire span to the file handle, looping on partial writes.
    private func writeAll(
        fileHandle fd: PlatformFileHandle,
        span: Span<UInt8>
    ) async throws(IOError) {
        var offset = 0
        let count = span.count
        while offset < count {
            let written: Int
            do {
                written = try await unsafe span.withUnsafeBytesAsync { (rawBuf: UnsafeRawBufferPointer) -> Int in
                    let result = try await self.submitOnce(
                        operation: .write(
                            fd: fd,
                            buffer: unsafe UnsafeRawBufferPointer(
                                start: rawBuf.baseAddress, count: rawBuf.count
                            ),
                            offset: offset
                        )
                    )
                    switch result {
                    case .write(let n):
                        return n
                    default:
                        throw IOError.unsupportedOperation(operation: .write)
                    }
                }
            } catch let err as IOError {
                throw err
            } catch {
                throw .backend(error)
            }
            if written <= 0 {
                throw .posix(errno: EIO, operation: .write)
            }
            offset += written
        }
    }

    public func write(
        fileHandle fd: PlatformFileHandle,
        body: (inout UniqueArray<UInt8>) async throws -> Void
    ) async throws(IOError) {
        // See `read<Return>(fileHandle:body:)` for the rationale on hand-rolled
        // release: Swift `~Copyable` defer/consume incompatibility prevents a
        // `defer { release(buf) }` form here.
        var writeBuf = _threadBoundState.ioBufferPool.acquire()
        do {
            try await body(&writeBuf)
        } catch let err as IOError {
            writeBuf.removeAll()
            _threadBoundState.ioBufferPool.release(writeBuf)
            throw err
        } catch {
            writeBuf.removeAll()
            _threadBoundState.ioBufferPool.release(writeBuf)
            throw .backend(error)
        }

        if writeBuf.count > 0 {
            do {
                try await self.writeAll(fileHandle: fd, span: writeBuf.span)
            } catch {
                writeBuf.removeAll()
                _threadBoundState.ioBufferPool.release(writeBuf)
                throw error
            }
        }
        writeBuf.removeAll()
        _threadBoundState.ioBufferPool.release(writeBuf)
    }

    public func accept(
        fileHandle fd: PlatformFileHandle
    ) async throws(IOError) -> (acceptedFileHandle: PlatformFileHandle, peer: SocketAddress) {
        let result = try await self.submitOnce(operation: .accept(fd: fd))
        switch result {
        case .accept(let acceptedFD, let storage, let length):
            // The first I/O operation on the accepted handle will lazy-register
            // it. We don't pre-register here because the caller may close the
            // handle before issuing any I/O, and a pre-registration would then
            // need a deregister on close. Lazy-register keeps the bookkeeping
            // confined to actual I/O.
            return (
                acceptedFileHandle: acceptedFD,
                peer: SocketAddress(storage: storage, length: length)
            )
        default:
            throw .unsupportedOperation(operation: .accept)
        }
    }

    public func connect(
        fileHandle fd: PlatformFileHandle,
        address: SocketAddress
    ) async throws(IOError) {
        // Try a non-blocking connect synchronously first. If it returns
        // EINPROGRESS — the common case for a non-blocking socket — suspend
        // on the selector for writability and check SO_ERROR via the
        // .connect completion path.
        var addr = address.storage
        let immediateResult = withUnsafePointer(to: &addr) { addrPtr -> CInt in
            addrPtr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sa in
                _systemConnect(fd, sa, address.length)
            }
        }
        if immediateResult == 0 { return }
        let err = errno
        if err == EINTR || err == EINPROGRESS {
            let result = try await self.submitOnce(
                operation: .connect(fd: fd, address: address.storage, addressLength: address.length)
            )
            switch result {
            case .connect:
                return
            default:
                throw .unsupportedOperation(operation: .connect)
            }
        }
        throw .posix(errno: err, operation: .connect)
    }

    public func close(
        fileHandle fd: PlatformFileHandle
    ) async throws(IOError) {
        // Drain any pending submissions on this fd (each resumes with
        // `.fileHandleClosed`) and remove the kernel-level registration,
        // then issue close(2). Deregister is a no-op when the handle was
        // never registered (e.g. fresh accept that never saw I/O).
        deregisterIO(fd: fd)
        let result = _systemClose(fd)
        if result != 0 {
            let err = errno
            // Eat EINTR — the file handle is closed regardless.
            if err != EINTR {
                throw .posix(errno: err, operation: .close)
            }
        }
    }
}
#endif

@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
private struct NonCopyablePriorityQueue<T>: ~Copyable {
  var queue: PriorityQueue<T>

  init(compare: @escaping (borrowing T, borrowing T) -> Bool) {
    self.queue = .init(compare: compare)
  }

  mutating func pop() -> T? {
    self.queue.pop()
  }

  func peek() -> T? {
    self.queue.peek()
  }

  mutating func push(_ newElement: T) {
    self.queue.push(newElement)
  }
}
#endif
