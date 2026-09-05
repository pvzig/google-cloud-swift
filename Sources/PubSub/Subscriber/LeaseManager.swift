import Foundation
import GRPCCore
import Logging
import Synchronization

actor LeaseManager {
    /// Pub/Sub caps acknowledge/modifyAckDeadline request size (512KB); like
    /// upstream's MAX_IDS_PER_RPC, never send more ack IDs than this per RPC.
    static let maxIDsPerRPC = 1_000

    /// gRPC rejects an already-expired deadline locally. A final shutdown RPC
    /// gets this small positive floor so it can reach the transport even when
    /// the caller's grace period has just elapsed.
    static let minimumShutdownRPCTimeout = Duration.milliseconds(1)

    /// Upstream's EXTEND_PERIOD/EXTEND_BUFFER: the deadline sweep runs often and
    /// re-extends only leases whose current extension would lapse before the next
    /// sweep plus a safety margin, instead of re-sending every lease every tick.
    static let extendPeriod = Duration.seconds(3)
    static let extendBuffer = Duration.seconds(2)
    /// Upstream's `extend_start`: the first sweep runs promptly so a message is
    /// covered soon after receipt rather than one full period later.
    static let extendStart = Duration.milliseconds(500)

    private enum DeliveryState {
        case available
        case pendingAck
        case pendingNack
    }

    private enum LeaseCommand: Sendable {
        case acknowledgeAtLeastOnce(String)
        case acknowledgeExactlyOnce(String)
        case negativelyAcknowledgeAtLeastOnce(String)
        case negativelyAcknowledgeExactlyOnce(String)
    }

    private struct LeaseCommandIngress: Sendable {
        var isShutdown = false
        var pending: [LeaseCommand] = []
    }

    private struct AckRetryState {
        let startedAt: ContinuousClock.Instant
        var failedAttempts: Int
        var nextAttemptAt: ContinuousClock.Instant
    }

    private struct FlushWork: Sendable {
        let chunk: [String]
        let isAck: Bool
        let timeout: Duration?
    }

    private enum FlushOutcome: Sendable {
        case success
        case cancelled
        case failure(PubSubServiceError)
    }

    private struct PerIDAckFailures {
        var transient: Set<String> = []
        var permanent: Set<String> = []

        var isEmpty: Bool {
            transient.isEmpty && permanent.isEmpty
        }
    }

    private struct LeaseExtensionResult: Sendable {
        let ackIDs: [String]
        let extendedAt: ContinuousClock.Instant?
    }

    private struct ActiveLease {
        let receivedAt: ContinuousClock.Instant
        let byteCount: Int
        let exactlyOnce: Bool
        let resultBox: AsyncValue<Void, AckError>?
        var deliveryState: DeliveryState
        /// When the deadline was last successfully pushed out, or nil if it has
        /// never been extended (or the last attempt failed and must be retried).
        var lastExtension: ContinuousClock.Instant?
    }

    private let subscription: String
    private let service: any SubscriberRPC
    private let shutdownBehavior: ShutdownBehavior
    private let maxLease: Duration
    private let maxLeaseExtension: Duration
    private let maxOutstandingMessages: Int
    private let maxOutstandingBytes: Int
    private let extendInterval: Duration
    private let extendStart: Duration
    private let flushInterval: Duration
    private let shutdownGracePeriod: Duration
    private let retryPolicy: RetryPolicy
    private let ackRetryDelay: @Sendable (Int) -> Duration
    private let commandIngress = Mutex(LeaseCommandIngress())

    private static let logger = Logger(label: "google-cloud-swift.pubsub.lease-manager")

    private let clock = ContinuousClock()
    private var activeLeases: [String: ActiveLease] = [:]
    private var outstandingBytes = 0
    private var pendingAckIDs: Set<String> = []
    private var pendingNackIDs: Set<String> = []
    private var ackRetryStates: [String: AckRetryState] = [:]
    private var flushTask: Task<Void, Never>?
    private var immediateFlushTask: Task<Void, Never>?
    private var needsAnotherImmediateFlush = false
    private var extendTask: Task<Void, Never>?
    private var extensionSweepTasks: [UUID: Task<Void, Never>] = [:]
    private var leaseExtensionInFlight: Set<String> = []
    private var shuttingDown = false

    init(
        subscription: String,
        service: any SubscriberRPC,
        shutdownBehavior: ShutdownBehavior,
        maxLease: Duration,
        maxLeaseExtension: Duration,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        flushInterval: Duration = .milliseconds(200),
        shutdownGracePeriod: Duration = .seconds(30),
        extendInterval: Duration? = nil,
        extendStart: Duration? = nil,
        retryPolicy: RetryPolicy = RetryPolicy(),
        ackRetryDelay: (@Sendable (Int) -> Duration)? = nil
    ) {
        self.subscription = subscription
        self.service = service
        self.shutdownBehavior = shutdownBehavior
        self.maxLease = maxLease
        self.maxLeaseExtension = maxLeaseExtension
        self.maxOutstandingMessages = maxOutstandingMessages
        self.maxOutstandingBytes = maxOutstandingBytes
        self.flushInterval = flushInterval
        self.shutdownGracePeriod = max(.zero, shutdownGracePeriod)
        self.extendInterval = extendInterval ?? Self.extendPeriod
        self.extendStart = extendStart ?? min(Self.extendStart, self.extendInterval)
        // Upstream retries confirmed acks/nacks for 600 seconds with no attempt
        // cap (`eo_ack_options`). A tighter budget turns a routine multi-second
        // service blip into permanently failed acknowledgements, which for
        // exactly-once means redelivering messages the application has already
        // processed. Upstream's 1s..64s backoff is deliberately not adopted: this
        // policy also drives at-least-once acks, which upstream retries with no
        // backoff at all, so the configured (100ms..60s) shape is kept for both.
        let budgetedRetryPolicy = retryPolicy.withDefaultBudget(
            maxElapsedTime: .seconds(600)
        )
        self.retryPolicy = budgetedRetryPolicy
        self.ackRetryDelay =
            ackRetryDelay ?? { attempt in
                budgetedRetryPolicy.delay(forAttempt: attempt)
            }
    }

    func start() {
        flushTask = Task { [weak self] in
            while let self, !Task.isCancelled {
                do {
                    try await Task.sleep(for: flushInterval)
                } catch {
                    break
                }
                guard !Task.isCancelled else {
                    break
                }
                await self.flush()
            }
        }
        extendTask = Task { [weak self] in
            guard let self else {
                return
            }

            var delay = extendStart
            while !Task.isCancelled {
                do {
                    try await Task.sleep(for: delay)
                } catch {
                    break
                }
                guard !Task.isCancelled else {
                    break
                }
                await self.scheduleOutstandingLeaseExtension()
                delay = extendInterval
            }
        }
    }

    func register(_ receivedMessage: ReceivedMessage, exactlyOnce: Bool) -> Handler {
        let resultBox = exactlyOnce ? AsyncValue<Void, AckError>() : nil
        let byteCount = receivedMessage.message.estimatedByteCount
        activeLeases[receivedMessage.ackID] = ActiveLease(
            receivedAt: clock.now,
            byteCount: byteCount,
            exactlyOnce: exactlyOnce,
            resultBox: resultBox,
            deliveryState: .available,
            lastExtension: nil
        )
        outstandingBytes += byteCount

        // The service populates deliveryAttempt only for subscriptions with a
        // dead-letter policy, so zero means "not reported" rather than zero tries.
        let deliveryAttempt =
            receivedMessage.deliveryAttempt > 0 ? receivedMessage.deliveryAttempt : nil

        if let resultBox {
            return .exactlyOnce(
                ExactlyOnce(
                    ackID: receivedMessage.ackID,
                    leaseManager: self,
                    resultBox: resultBox,
                    deliveryAttempt: deliveryAttempt
                )
            )
        }

        return .atLeastOnce(
            AtLeastOnce(
                ackID: receivedMessage.ackID,
                leaseManager: self,
                deliveryAttempt: deliveryAttempt
            )
        )
    }

    nonisolated func enqueueAcknowledgeAtLeastOnce(_ ackID: String) {
        enqueue(.acknowledgeAtLeastOnce(ackID))
    }

    nonisolated func enqueueAcknowledgeExactlyOnce(_ ackID: String) {
        enqueue(.acknowledgeExactlyOnce(ackID))
    }

    nonisolated func enqueueNegativelyAcknowledgeAtLeastOnce(_ ackID: String) {
        enqueue(.negativelyAcknowledgeAtLeastOnce(ackID))
    }

    nonisolated func enqueueNegativelyAcknowledgeExactlyOnce(_ ackID: String) {
        enqueue(.negativelyAcknowledgeExactlyOnce(ackID))
    }

    private nonisolated func enqueue(_ command: LeaseCommand) {
        let needsDrain = commandIngress.withLock { state -> Bool? in
            guard !state.isShutdown else {
                return nil
            }

            let wasEmpty = state.pending.isEmpty
            state.pending.append(command)
            return wasEmpty
        }

        if needsDrain == true {
            Task {
                await self.drainCommandIngress()
            }
        }
    }

    private func drainCommandIngress() {
        let commands = commandIngress.withLock { state in
            let commands = state.pending
            state.pending.removeAll(keepingCapacity: true)
            return commands
        }
        process(commands)
    }

    private func process(_ commands: [LeaseCommand]) {
        for command in commands {
            switch command {
            case .acknowledgeAtLeastOnce(let ackID):
                acknowledgeAtLeastOnce(ackID)
            case .acknowledgeExactlyOnce(let ackID):
                acknowledgeExactlyOnce(ackID)
            case .negativelyAcknowledgeAtLeastOnce(let ackID):
                negativelyAcknowledgeAtLeastOnce(ackID)
            case .negativelyAcknowledgeExactlyOnce(let ackID):
                negativelyAcknowledgeExactlyOnce(ackID)
            }
        }
    }

    private func acknowledgeAtLeastOnce(_ ackID: String) {
        removeLease(ackID)
        pendingAckIDs.insert(ackID)
        flushIfFull()
    }

    private func acknowledgeExactlyOnce(_ ackID: String) {
        guard var lease = activeLeases[ackID], lease.exactlyOnce else {
            return
        }

        lease.deliveryState = .pendingAck
        activeLeases[ackID] = lease
        pendingAckIDs.insert(ackID)
        flushIfFull()
    }

    func negativelyAcknowledgeAtLeastOnce(_ ackID: String) {
        guard removeLease(ackID) != nil else {
            return
        }

        pendingNackIDs.insert(ackID)
        flushIfFull()
    }

    func negativelyAcknowledgeExactlyOnce(_ ackID: String) {
        guard var lease = activeLeases[ackID], lease.exactlyOnce else {
            return
        }

        lease.deliveryState = .pendingNack
        activeLeases[ackID] = lease
        pendingNackIDs.insert(ackID)
        flushIfFull()
    }

    /// Nacks messages that were delivered on the stream but never registered
    /// (e.g. the tail of a response when shutdown interrupts mid-batch), so the
    /// server can redeliver them promptly instead of waiting out the deadline.
    func nackUnregistered(_ ackIDs: [String]) {
        pendingNackIDs.formUnion(ackIDs)
        flushIfFull()
    }

    /// Flushes early once a full RPC's worth of IDs is pending instead of
    /// waiting for the next periodic tick, like upstream's needs_flush.
    private func flushIfFull() {
        guard !shuttingDown,
            pendingAckIDs.count >= Self.maxIDsPerRPC || pendingNackIDs.count >= Self.maxIDsPerRPC
        else {
            return
        }

        // A flush is already in flight: record that the backlog crossed the
        // threshold again so it is coalesced into another round rather than
        // dropped until the next periodic tick.
        guard immediateFlushTask == nil else {
            needsAnotherImmediateFlush = true
            return
        }

        immediateFlushTask = Task { [weak self] in
            guard !Task.isCancelled else {
                return
            }
            await self?.flushImmediately()
        }
    }

    private func flushImmediately() async {
        repeat {
            needsAnotherImmediateFlush = false
            await flush()
        } while needsAnotherImmediateFlush && !shuttingDown && !Task.isCancelled

        needsAnotherImmediateFlush = false
        immediateFlushTask = nil
    }

    func availablePullCapacity(defaultBatchSize: Int) -> Int {
        let outstandingMessages = activeLeases.count

        if maxOutstandingBytes > 0, outstandingBytes >= maxOutstandingBytes {
            return 0
        }

        let messageCapacity: Int
        if maxOutstandingMessages > 0 {
            messageCapacity = max(0, maxOutstandingMessages - outstandingMessages)
        } else {
            messageCapacity = defaultBatchSize
        }

        guard messageCapacity > 0 else {
            return 0
        }

        return min(defaultBatchSize, messageCapacity)
    }

    func shutdown() async {
        guard !shuttingDown else {
            return
        }

        shuttingDown = true
        // Each phase receives its own bounded grace window. Reusing the processing
        // deadline for the acknowledgement and nack drains leaves them with only
        // the 1 ms transport floor whenever a consumer holds a handler for the
        // complete processing window.
        let processingDeadline = clock.now.advanced(by: shutdownGracePeriod)

        await stopFlushWorkers()

        switch shutdownBehavior {
        case .waitForProcessing:
            // The command ingress stays open for this phase: consumers are still
            // releasing handlers, and their acks/nacks are exactly what this loop is
            // waiting for. The periodic extendTask keeps the leases alive meanwhile.
            // The Task.isCancelled check is a safety valve: a cancelled sleep returns
            // immediately and would otherwise turn this wait into a busy-spin.
            while !activeLeases.isEmpty, clock.now < processingDeadline, !Task.isCancelled {
                drainCommandIngress()
                await flush(deadline: processingDeadline)
                try? await Task.sleep(for: flushInterval)
            }
        case .nackImmediately:
            break
        }

        // Close the ingress and take whatever the consumers queued last. From here
        // the lease state is final, so late commands can no longer be honored.
        let queuedCommands = commandIngress.withLock { state in
            state.isShutdown = true
            let commands = state.pending
            state.pending.removeAll(keepingCapacity: false)
            return commands
        }
        process(queuedCommands)

        // Keep pending exactly-once acknowledgements leased through their bounded
        // final drain. Stopping extension first can let the server redeliver an
        // unsettled message while its original confirmation is still waiting.
        await drainPendingOperations(
            until: clock.now.advanced(by: shutdownGracePeriod))

        // No positive lease extension may race the shutdown nacks below.
        // Cancelling and joining the stored unstructured task establishes that
        // ordering for both shutdown behaviors.
        await stopLeaseExtension()

        // Anything still leased is returned to the server now rather than left to
        // expire: for .nackImmediately that is the whole point, and for
        // .waitForProcessing it is what the grace period timing out means.
        await prepareAvailableLeasesForShutdownNack()

        // Final drain of pending (or transiently failed and requeued) acks/nacks,
        // honoring each ID's retry backoff through this phase's deadline.
        await drainPendingOperations(
            until: clock.now.advanced(by: shutdownGracePeriod))

        for (ackID, lease) in activeLeases {
            if lease.exactlyOnce {
                await lease.resultBox?.fail(
                    .shutdown("subscriber shut down while nacking \(ackID)"))
            }
        }
        activeLeases.removeAll()
        ackRetryStates.removeAll()
        outstandingBytes = 0
    }

    private func drainPendingOperations(until deadline: ContinuousClock.Instant) async {
        while !pendingAckIDs.isEmpty || !pendingNackIDs.isEmpty, !Task.isCancelled {
            await flush(deadline: deadline)
            guard !pendingAckIDs.isEmpty || !pendingNackIDs.isEmpty else {
                break
            }

            let delay = nextPendingRetryDelay()
            let remaining = clock.now.duration(to: deadline)
            guard remaining > .zero, delay < remaining else {
                break
            }

            if delay > .zero {
                try? await Task.sleep(for: delay)
            } else {
                await Task.yield()
            }
        }
    }

    private func prepareAvailableLeasesForShutdownNack() async {
        var shutdownConfirmations: [(String, AsyncValue<Void, AckError>)] = []
        for (ackID, var lease) in activeLeases where lease.deliveryState == .available {
            lease.deliveryState = .pendingNack
            activeLeases[ackID] = lease
            pendingNackIDs.insert(ackID)
            if lease.exactlyOnce, let resultBox = lease.resultBox {
                shutdownConfirmations.append((ackID, resultBox))
            }
        }

        // These nacks are lifecycle cleanup, not application-confirmed nacks. Fail
        // the box before the RPC so its success path cannot report a confirmed ack
        // to a handler that the application has not consumed yet.
        for (ackID, resultBox) in shutdownConfirmations {
            await resultBox.fail(.shutdown("subscriber shut down while nacking \(ackID)"))
        }
    }

    private func stopFlushWorkers() async {
        let flushTask = self.flushTask
        let immediateFlushTask = self.immediateFlushTask
        self.flushTask = nil
        self.immediateFlushTask = nil
        flushTask?.cancel()
        immediateFlushTask?.cancel()
        await flushTask?.value
        await immediateFlushTask?.value
    }

    private func stopLeaseExtension() async {
        let extendTask = self.extendTask
        self.extendTask = nil
        extendTask?.cancel()
        await extendTask?.value

        let sweepTasks = Array(extensionSweepTasks.values)
        extensionSweepTasks.removeAll(keepingCapacity: false)
        for task in sweepTasks {
            task.cancel()
        }
        for task in sweepTasks {
            await task.value
        }
        leaseExtensionInFlight.removeAll(keepingCapacity: false)
    }

    private func flush(deadline: ContinuousClock.Instant? = nil) async {
        let now = clock.now
        let ackIDs = Array(pendingAckIDs.filter { isReadyToRetry($0, at: now) })
        let nackIDs = Array(pendingNackIDs.filter { isReadyToRetry($0, at: now) })
        pendingAckIDs.subtract(ackIDs)
        pendingNackIDs.subtract(nackIDs)

        // Retry budgets are per ID, so IDs that have run out are failed
        // individually instead of poisoning the RPC chunk they happen to land in.
        let (liveAckIDs, expiredAckIDs) = partitionByRetryBudget(ackIDs)
        let (liveNackIDs, expiredNackIDs) = partitionByRetryBudget(nackIDs)
        await failExpiredRetryBudget(expiredAckIDs)
        await failExpiredRetryBudget(expiredNackIDs)

        // Every chunk's RPC is issued concurrently, like upstream's flush spawning
        // each ack/nack into its task tracker: serializing them would make one slow
        // RPC eat into every later chunk's deadline budget. Outcomes are applied
        // afterwards on the actor, so state updates stay serialized.
        let remainingShutdownTime = deadline.map {
            max(Self.minimumShutdownRPCTimeout, clock.now.duration(to: $0))
        }
        let work =
            Self.chunked(liveAckIDs).map {
                FlushWork(
                    chunk: $0,
                    isAck: true,
                    timeout: cappedRetryTimeout(for: $0, maximum: remainingShutdownTime)
                )
            }
            + Self.chunked(liveNackIDs).map {
                FlushWork(
                    chunk: $0,
                    isAck: false,
                    timeout: cappedRetryTimeout(for: $0, maximum: remainingShutdownTime)
                )
            }
        guard !work.isEmpty else {
            return
        }

        let outcomes = await withTaskGroup(of: (FlushWork, FlushOutcome).self) { group in
            for item in work {
                group.addTask { [service, subscription] in
                    do {
                        if item.isAck {
                            try await service.acknowledge(
                                subscription: subscription,
                                ackIDs: item.chunk,
                                timeout: item.timeout
                            )
                        } else {
                            try await service.modifyAckDeadline(
                                subscription: subscription,
                                ackIDs: item.chunk,
                                ackDeadlineSeconds: 0,
                                timeout: item.timeout
                            )
                        }
                        return (item, .success)
                    } catch {
                        // The raw error cannot cross back to the actor (`any Error` is not
                        // Sendable), so classify it here into the two things the caller
                        // needs to distinguish.
                        if isCancellation(error) {
                            return (item, .cancelled)
                        }
                        return (item, .failure(asServiceError(error)))
                    }
                }
            }

            var outcomes: [(FlushWork, FlushOutcome)] = []
            for await outcome in group {
                outcomes.append(outcome)
            }
            return outcomes
        }

        for (item, outcome) in outcomes {
            guard case .success = outcome else {
                await handleFlushFailure(
                    outcome, ackIDs: item.chunk,
                    requeue: { [item] ids in
                        if item.isAck {
                            self.pendingAckIDs.formUnion(ids)
                        } else {
                            self.pendingNackIDs.formUnion(ids)
                        }
                    })
                continue
            }

            await confirmExactlyOnce(item.chunk, result: .success(()))
        }
    }

    private static func chunked(_ ids: [String]) -> [[String]] {
        guard ids.count > maxIDsPerRPC else {
            return ids.isEmpty ? [] : [ids]
        }

        return stride(from: 0, to: ids.count, by: maxIDsPerRPC).map {
            Array(ids[$0..<min($0 + maxIDsPerRPC, ids.count)])
        }
    }

    /// Transient failures requeue the batch so the next flush tick retries it
    /// (exactly-once leases stay active and keep being extended meanwhile);
    /// only permanent failures drop the batch and fail the confirmation boxes.
    private func handleFlushFailure(
        _ outcome: FlushOutcome,
        ackIDs: [String],
        requeue: ([String]) -> Void
    ) async {
        let serviceError: PubSubServiceError
        switch outcome {
        case .success:
            return
        case .cancelled where shuttingDown:
            // shutdown() cancels tracked flush workers before its final drain. Those
            // idempotent ack/nack RPCs must be made eligible for that drain rather
            // than classified as a permanent caller cancellation. The attempt never
            // completed, so no failure is charged against it and any backoff already
            // scheduled for these IDs stays as it was.
            let now = clock.now
            for ackID in ackIDs where ackRetryStates[ackID] == nil {
                ackRetryStates[ackID] = AckRetryState(
                    startedAt: now,
                    failedAttempts: 0,
                    nextAttemptAt: now
                )
            }
            requeue(ackIDs)
            return
        case .cancelled:
            // Not shutting down, so this is a deliberate abort (upstream treats
            // CANCELLED as permanent and never replays it) rather than teardown.
            serviceError = PubSubServiceError(
                code: .cancelled, message: "acknowledgement cancelled")
        case .failure(let error):
            serviceError = error
        }

        Self.logger.debug(
            "Acknowledgement operation failed",
            metadata: [
                "subscription": "\(subscription)", "ack_id_count": "\(ackIDs.count)",
                "error": "\(serviceError)",
            ]
        )

        let perIDFailures = Self.extractPerIDFailures(from: serviceError)
        if !perIDFailures.isEmpty {
            let successful = ackIDs.filter {
                !perIDFailures.transient.contains($0) && !perIDFailures.permanent.contains($0)
            }
            let permanent = ackIDs.filter { perIDFailures.permanent.contains($0) }
            let transient = ackIDs.filter { perIDFailures.transient.contains($0) }

            await confirmExactlyOnce(successful, result: .success(()))
            await confirmExactlyOnce(permanent, result: .failure(.service(serviceError)))
            await retryOrFail(
                transient,
                serviceError: serviceError,
                forceTransient: true,
                requeue: requeue
            )
            return
        }

        await retryOrFail(
            ackIDs,
            serviceError: serviceError,
            forceTransient: false,
            requeue: requeue
        )
    }

    /// Reads the per-ID dispositions out of the already-decoded
    /// `google.rpc.ErrorInfo` on the service error, so the protobuf status is
    /// unpacked once per failure rather than once here and once in
    /// `asServiceError`.
    private static func extractPerIDFailures(
        from serviceError: PubSubServiceError
    ) -> PerIDAckFailures {
        var failures = PerIDAckFailures()
        for errorInfo in serviceError.errorInfo {
            // Other ErrorInfo metadata describes the RPC failure (for example,
            // quota consumer/service fields), not rejected acknowledgement IDs.
            guard errorInfo.reason == "EXACTLY_ONCE_ACK_FAILURE",
                errorInfo.domain == "pubsub.googleapis.com"
            else {
                continue
            }

            for (ackID, disposition) in errorInfo.metadata {
                if disposition.hasPrefix("TRANSIENT_FAILURE_") {
                    failures.transient.insert(ackID)
                } else if disposition.hasPrefix("PERMANENT_FAILURE_") {
                    failures.permanent.insert(ackID)
                } else {
                    // An ID explicitly listed in an EXACTLY_ONCE_ACK_FAILURE was rejected.
                    // Unknown dispositions must fail closed; treating them as absent makes
                    // confirmedAck() report success for a server-rejected acknowledgement.
                    failures.permanent.insert(ackID)
                }
            }
        }
        failures.transient.subtract(failures.permanent)
        return failures
    }

    private func retryOrFail(
        _ ackIDs: [String],
        serviceError: PubSubServiceError,
        forceTransient: Bool,
        requeue: ([String]) -> Void
    ) async {
        let now = clock.now
        var retryIDs: [String] = []
        var failedIDs: [String] = []

        for ackID in ackIDs {
            var state =
                ackRetryStates[ackID]
                ?? AckRetryState(startedAt: now, failedAttempts: 0, nextAttemptAt: now)
            let nextFailedAttempts = state.failedAttempts + 1
            let retryAt = now.advanced(by: ackRetryDelay(nextFailedAttempts))
            let attemptsRemain =
                retryPolicy.maxAttempts.map {
                    nextFailedAttempts < $0
                } ?? true
            let timeRemains =
                retryPolicy.maxElapsedTime.map {
                    now - state.startedAt < $0
                } ?? true
            let retryFitsTimeBudget =
                retryPolicy.maxElapsedTime.map {
                    state.startedAt.duration(to: retryAt) < $0
                } ?? true
            let codeIsRetryable =
                forceTransient
                || serviceError.code.map(retryPolicy.retryableCodes.contains) == true

            if attemptsRemain && timeRemains && retryFitsTimeBudget && codeIsRetryable {
                state.failedAttempts = nextFailedAttempts
                state.nextAttemptAt = retryAt
                ackRetryStates[ackID] = state
                retryIDs.append(ackID)
            } else {
                ackRetryStates[ackID] = nil
                failedIDs.append(ackID)
            }
        }

        if !retryIDs.isEmpty {
            requeue(retryIDs)
        }
        if !failedIDs.isEmpty {
            await confirmExactlyOnce(failedIDs, result: .failure(.service(serviceError)))
        }
    }

    /// Starts each ID's elapsed-time budget immediately before its first RPC —
    /// so the budget counts time spent inside attempts, not only the delays
    /// between them — and splits the batch into IDs that still have budget left
    /// and IDs whose budget has run out.
    private func partitionByRetryBudget(
        _ ackIDs: [String]
    ) -> (live: [String], expired: [String]) {
        let now = clock.now
        for ackID in ackIDs where ackRetryStates[ackID] == nil {
            ackRetryStates[ackID] = AckRetryState(
                startedAt: now,
                failedAttempts: 0,
                nextAttemptAt: now
            )
        }

        guard let maxElapsedTime = retryPolicy.maxElapsedTime else {
            return (ackIDs, [])
        }

        var live: [String] = []
        var expired: [String] = []
        for ackID in ackIDs {
            guard let state = ackRetryStates[ackID] else {
                live.append(ackID)
                continue
            }

            if now - state.startedAt < maxElapsedTime {
                live.append(ackID)
            } else {
                expired.append(ackID)
            }
        }
        return (live, expired)
    }

    /// The deadline for a chunk is the longest remaining budget in it. Taking the
    /// shortest instead would let one nearly-expired ID hand every other ID in
    /// the same RPC a near-zero deadline; per-ID budgets are enforced by
    /// `partitionByRetryBudget` and `retryOrFail`, not by the call deadline.
    private func retryTimeout(for ackIDs: [String]) -> Duration? {
        guard let maxElapsedTime = retryPolicy.maxElapsedTime else {
            return nil
        }

        let now = clock.now
        var longestRemaining: Duration?
        for ackID in ackIDs {
            guard let state = ackRetryStates[ackID] else {
                continue
            }

            let deadline = state.startedAt.advanced(by: maxElapsedTime)
            let remaining = max(.zero, now.duration(to: deadline))
            longestRemaining = max(longestRemaining ?? remaining, remaining)
        }
        return longestRemaining
    }

    private func cappedRetryTimeout(
        for ackIDs: [String],
        maximum: Duration?
    ) -> Duration? {
        guard let maximum else {
            return retryTimeout(for: ackIDs)
        }

        return max(
            Self.minimumShutdownRPCTimeout,
            min(retryTimeout(for: ackIDs) ?? maximum, maximum)
        )
    }

    private func failExpiredRetryBudget(_ ackIDs: [String]) async {
        guard !ackIDs.isEmpty else {
            return
        }

        await confirmExactlyOnce(
            ackIDs,
            result: .failure(
                .service(
                    PubSubServiceError(
                        code: .deadlineExceeded,
                        message: "acknowledgement retry deadline exceeded"
                    )
                )
            )
        )
    }

    private func isReadyToRetry(
        _ ackID: String,
        at now: ContinuousClock.Instant
    ) -> Bool {
        guard let retryState = ackRetryStates[ackID] else {
            return true
        }

        return retryState.nextAttemptAt <= now
    }

    private func nextPendingRetryDelay() -> Duration {
        let pendingIDs = pendingAckIDs.union(pendingNackIDs)
        guard !pendingIDs.isEmpty else {
            return .zero
        }

        let now = clock.now
        var earliestRetry: ContinuousClock.Instant?
        for ackID in pendingIDs {
            guard let retryAt = ackRetryStates[ackID]?.nextAttemptAt else {
                return .zero
            }

            earliestRetry = min(earliestRetry ?? retryAt, retryAt)
        }

        return earliestRetry.map { max(.zero, now.duration(to: $0)) } ?? .zero
    }

    /// All lease removals go through here so the outstanding-byte counter used
    /// for flow control stays consistent with the lease map.
    @discardableResult
    private func removeLease(_ ackID: String) -> ActiveLease? {
        guard let lease = activeLeases.removeValue(forKey: ackID) else {
            return nil
        }

        outstandingBytes -= lease.byteCount
        return lease
    }

    private func confirmExactlyOnce(_ ackIDs: [String], result: Result<Void, AckError>) async {
        for ackID in ackIDs {
            ackRetryStates[ackID] = nil
            guard let lease = removeLease(ackID), lease.exactlyOnce else {
                continue
            }

            switch result {
            case .success:
                await lease.resultBox?.succeed(())
            case .failure(let error):
                await lease.resultBox?.fail(error)
            }
        }
    }

    private func scheduleOutstandingLeaseExtension() async {
        let now = clock.now
        let expired = activeLeases.compactMap { ackID, lease -> String? in
            guard lease.deliveryState == .available, now - lease.receivedAt > maxLease else {
                return nil
            }

            return ackID
        }
        var expiredExactlyOnce: [AsyncValue<Void, AckError>] = []
        for ackID in expired {
            guard let lease = removeLease(ackID) else {
                continue
            }

            pendingAckIDs.remove(ackID)
            pendingNackIDs.remove(ackID)
            ackRetryStates[ackID] = nil
            if lease.exactlyOnce, let resultBox = lease.resultBox {
                expiredExactlyOnce.append(resultBox)
            }
        }
        for resultBox in expiredExactlyOnce {
            await resultBox.fail(.leaseExpired)
        }

        // Only leases whose current extension would lapse before the next sweep
        // (plus a safety buffer) are re-sent. Extending every lease on every tick
        // multiplies modack traffic by the sweep rate for no added safety.
        let extensionHorizon = now.advanced(by: extendInterval + Self.extendBuffer)
        let ackIDs = activeLeases.compactMap { ackID, lease -> String? in
            guard leaseExtensionInFlight.contains(ackID) == false else {
                return nil
            }
            switch lease.deliveryState {
            case .pendingNack:
                return nil
            case .available, .pendingAck:
                guard let lastExtension = lease.lastExtension else {
                    return ackID
                }

                return lastExtension.advanced(by: maxLeaseExtension) > extensionHorizon
                    ? nil : ackID
            }
        }
        guard !ackIDs.isEmpty else {
            return
        }

        leaseExtensionInFlight.formUnion(ackIDs)
        let retryBudget = max(.milliseconds(1), maxLeaseExtension - extendInterval)
        let sweepID = UUID()
        let task = Task { [service, subscription, retryPolicy, maxLeaseExtension] in
            let results = await withTaskGroup(of: LeaseExtensionResult.self) { group in
                for chunk in Self.chunked(ackIDs) {
                    group.addTask {
                        await Self.extendLease(
                            service: service,
                            subscription: subscription,
                            ackIDs: chunk,
                            ackDeadline: maxLeaseExtension,
                            retryBudget: retryBudget,
                            retryPolicy: retryPolicy
                        )
                    }
                }

                var results: [LeaseExtensionResult] = []
                for await result in group {
                    results.append(result)
                }
                return results
            }
            self.finishLeaseExtensionSweep(sweepID: sweepID, results: results)
        }
        extensionSweepTasks[sweepID] = task
    }

    private func finishLeaseExtensionSweep(
        sweepID: UUID,
        results: [LeaseExtensionResult]
    ) {
        extensionSweepTasks[sweepID] = nil
        for result in results {
            leaseExtensionInFlight.subtract(result.ackIDs)
            guard let extendedAt = result.extendedAt else {
                continue
            }
            // Record the conservative request-send time, not the time the slowest
            // sibling chunk happened to finish.
            for ackID in result.ackIDs {
                activeLeases[ackID]?.lastExtension = extendedAt
            }
        }
    }

    /// Returns the ack IDs whose deadline was successfully pushed out, or an
    /// empty array if the chunk never landed, so the caller only records an
    /// extension it actually achieved.
    private nonisolated static func extendLease(
        service: any SubscriberRPC,
        subscription: String,
        ackIDs: [String],
        ackDeadline: Duration,
        retryBudget: Duration,
        retryPolicy: RetryPolicy
    ) async -> LeaseExtensionResult {
        let clock = ContinuousClock()
        let policyBudget = retryPolicy.maxElapsedTime.map { min($0, retryBudget) } ?? retryBudget
        let deadline = clock.now.advanced(by: policyBudget)
        var attempt = 0

        while !Task.isCancelled {
            let remainingTime = clock.now.duration(to: deadline)
            guard remainingTime > .zero else {
                return LeaseExtensionResult(ackIDs: ackIDs, extendedAt: nil)
            }

            do {
                let requestStartedAt = clock.now
                try await service.modifyAckDeadline(
                    subscription: subscription,
                    ackIDs: ackIDs,
                    ackDeadlineSeconds: Int32(ackDeadline.components.seconds),
                    timeout: remainingTime
                )
                return LeaseExtensionResult(ackIDs: ackIDs, extendedAt: requestStartedAt)
            } catch {
                if isCancellation(error) {
                    return LeaseExtensionResult(ackIDs: ackIDs, extendedAt: nil)
                }
                let serviceError = asServiceError(error)
                guard retryPolicy.shouldRetry(code: serviceError.code, attempt: attempt) else {
                    logger.error(
                        "Lease extension failed permanently",
                        metadata: [
                            "subscription": "\(subscription)", "ack_id_count": "\(ackIDs.count)",
                            "error": "\(serviceError)",
                        ]
                    )
                    return LeaseExtensionResult(ackIDs: ackIDs, extendedAt: nil)
                }

                attempt += 1
                logger.debug(
                    "Lease extension failed; retrying",
                    metadata: [
                        "subscription": "\(subscription)", "ack_id_count": "\(ackIDs.count)",
                        "attempt": "\(attempt)", "error": "\(serviceError)",
                    ]
                )
                let delay = retryPolicy.delay(forAttempt: attempt)
                let timeAfterFailure = clock.now.duration(to: deadline)
                guard timeAfterFailure > .zero, delay < timeAfterFailure else {
                    return LeaseExtensionResult(ackIDs: ackIDs, extendedAt: nil)
                }

                do {
                    try await Task.sleep(for: delay)
                } catch {
                    return LeaseExtensionResult(ackIDs: ackIDs, extendedAt: nil)
                }
            }
        }

        return LeaseExtensionResult(ackIDs: ackIDs, extendedAt: nil)
    }
}
