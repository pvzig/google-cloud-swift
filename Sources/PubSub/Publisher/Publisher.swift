import Auth
import Foundation
import Synchronization

protocol PublisherRPC: Sendable {
    func publish(topic: String, messages: [Message], timeout: Duration?) async throws -> [String]
    func shutdown() async
}

extension PublisherRPC {
    func shutdown() async {}
}

struct LivePublisherRPC: PublisherRPC {
    let connection: ServiceConnection

    func publish(topic: String, messages: [Message], timeout: Duration?) async throws -> [String] {
        var request = Google_Pubsub_V1_PublishRequest()
        request.topic = topic
        request.messages = messages

        let client = try await connection.publisherClient()
        let response: Google_Pubsub_V1_PublishResponse = try await client.publish(
            request,
            metadata: ServiceConnection.routingMetadata(["topic": topic]),
            options: connection.callOptions(timeout: timeout)
        )
        return response.messageIds
    }

    func shutdown() async {
        await connection.shutdown()
    }
}

public struct PublishFuture: Sendable {
    fileprivate let valueBox: AsyncValue<String, PublishError>

    fileprivate init(valueBox: AsyncValue<String, PublishError>) {
        self.valueBox = valueBox
    }

    public func value() async throws -> String {
        try await valueBox.value()
    }
}

public struct Publisher: Sendable {
    private let dispatcher: PublisherDispatcher
    private let publisherRegistry: PublisherRegistry?
    private let registrationID: UUID?

    init(
        dispatcher: PublisherDispatcher,
        publisherRegistry: PublisherRegistry? = nil,
        registrationID: UUID? = nil
    ) {
        self.dispatcher = dispatcher
        self.publisherRegistry = publisherRegistry
        self.registrationID = registrationID
    }

    public static func builder(_ topic: String) -> PublisherBuilder {
        PublisherBuilder(topic: topic)
    }

    @discardableResult
    public func publish(_ message: Message) -> PublishFuture {
        let valueBox = AsyncValue<String, PublishError>()
        dispatcher.enqueue(message, valueBox: valueBox)
        return PublishFuture(valueBox: valueBox)
    }

    public func flush() async {
        await dispatcher.flush()
    }

    public func resumePublish(_ orderingKey: String) async {
        await dispatcher.resumePublish(orderingKey)
    }

    public func shutdown() async {
        await dispatcher.shutdown()
        if let publisherRegistry, let registrationID {
            publisherRegistry.unregister(id: registrationID)
        }
    }
}

public struct PublisherBuilder: Sendable, ConfigurableClientBuilder {
    private var topic: String
    private var batchingOptions: BatchingOptions
    public var configuration: ClientConfiguration

    init(
        topic: String,
        batchingOptions: BatchingOptions = BatchingOptions(),
        configuration: ClientConfiguration = ClientConfiguration()
    ) {
        self.topic = topic
        self.batchingOptions = batchingOptions
        self.configuration = configuration
    }

    public func build() async throws -> Publisher {
        // A standalone publisher owns its connection, unlike publishers built
        // from a shared BasePublisher, so its shutdown() closes the connection.
        let connection = try ServiceConnection(configuration: configuration)
        return PublisherPartialBuilder(
            service: LivePublisherRPC(connection: connection),
            topic: topic,
            configuration: configuration,
            batchingOptions: batchingOptions,
            ownsService: true
        ).build()
    }

    public func setMessageCountThreshold(_ threshold: Int) -> Self {
        var copy = self
        copy.batchingOptions = batchingOptions.setMessageCountThreshold(threshold)
        return copy
    }

    public func setByteThreshold(_ threshold: Int) -> Self {
        var copy = self
        copy.batchingOptions = batchingOptions.setByteThreshold(threshold)
        return copy
    }

    public func setDelayThreshold(_ threshold: Duration) -> Self {
        var copy = self
        copy.batchingOptions = batchingOptions.setDelayThreshold(threshold)
        return copy
    }
}

public struct PublisherPartialBuilder: Sendable {
    private let service: any PublisherRPC
    private let topic: String
    private let configuration: ClientConfiguration
    private let ownsService: Bool
    private var batchingOptions: BatchingOptions
    private let publisherRegistry: PublisherRegistry?

    init(
        service: any PublisherRPC,
        topic: String,
        configuration: ClientConfiguration,
        batchingOptions: BatchingOptions = BatchingOptions(),
        ownsService: Bool = false,
        publisherRegistry: PublisherRegistry? = nil
    ) {
        self.service = service
        self.topic = topic
        self.configuration = configuration
        self.batchingOptions = batchingOptions
        self.ownsService = ownsService
        self.publisherRegistry = publisherRegistry
    }

    public func build() -> Publisher {
        let dispatcher = PublisherDispatcher(
            service: service,
            topic: topic,
            batchingOptions: batchingOptions.normalized,
            retryPolicy: configuration.retryPolicy,
            ownsService: ownsService
        )
        guard let publisherRegistry else {
            return Publisher(dispatcher: dispatcher)
        }
        guard let registrationID = publisherRegistry.register(dispatcher) else {
            return Publisher(
                dispatcher: PublisherDispatcher(
                    service: service,
                    topic: topic,
                    batchingOptions: batchingOptions.normalized,
                    retryPolicy: configuration.retryPolicy,
                    startsShutdown: true
                )
            )
        }
        return Publisher(
            dispatcher: dispatcher,
            publisherRegistry: publisherRegistry,
            registrationID: registrationID
        )
    }

    public func setMessageCountThreshold(_ threshold: Int) -> Self {
        var copy = self
        copy.batchingOptions = batchingOptions.setMessageCountThreshold(threshold)
        return copy
    }

    public func setByteThreshold(_ threshold: Int) -> Self {
        var copy = self
        copy.batchingOptions = batchingOptions.setByteThreshold(threshold)
        return copy
    }

    public func setDelayThreshold(_ threshold: Duration) -> Self {
        var copy = self
        copy.batchingOptions = batchingOptions.setDelayThreshold(threshold)
        return copy
    }
}

private struct PendingPublish: Sendable {
    let message: Message
    let valueBox: AsyncValue<String, PublishError>

    /// Serializing a message is O(payload), and batching consults this size on
    /// every chunking pass, so it is computed once at admission instead of
    /// re-encoding the payload each time a batch is measured.
    let serializedByteCount: Int

    init(message: Message, valueBox: AsyncValue<String, PublishError>) {
        self.message = message
        self.valueBox = valueBox
        self.serializedByteCount = message.serializedByteCount
    }
}

private struct PublisherIngressState: Sendable {
    var isShutdown = false
    var pending: [PendingPublish] = []
}

private struct BatchState {
    var pending: [PendingPublish] = []
    var pendingByteCount = 0
    var inFlight = false
    var inFlightEntries: [PendingPublish] = []
    var completingEntries: [PendingPublish] = []
    var paused = false
    var flushRequested = false
    var scheduledFlush: Task<Void, Never>?
}

actor PublisherDispatcher {
    private let ingress: Mutex<PublisherIngressState>
    private let service: any PublisherRPC
    private let topic: String
    private let batchingOptions: BatchingOptions
    private let publishRetryPolicy: RetryPolicy
    private let ownsService: Bool

    private var batches: [String: BatchState] = [:]
    // Only the result box is tracked, not the whole PendingPublish: flush() needs
    // nothing else, and parking the message here would hold payloads (up to 10 MB
    // each) alive until the rejection task runs.
    private var localCompletions: [ObjectIdentifier: AsyncValue<String, PublishError>] = [:]

    init(
        service: any PublisherRPC,
        topic: String,
        batchingOptions: BatchingOptions,
        retryPolicy: RetryPolicy,
        ownsService: Bool = false,
        startsShutdown: Bool = false
    ) {
        self.service = service
        self.topic = topic
        self.batchingOptions = batchingOptions
        // Upstream bounds the publish retry loop with a 600s time limit so a
        // persistently failing batch eventually resolves its futures.
        self.publishRetryPolicy = retryPolicy.withDefaultBudget(maxElapsedTime: .seconds(600))
        self.ownsService = ownsService
        self.ingress = Mutex(PublisherIngressState(isShutdown: startsShutdown))
    }

    nonisolated func enqueue(_ message: Message, valueBox: AsyncValue<String, PublishError>) {
        let pending = PendingPublish(message: message, valueBox: valueBox)
        let needsDrain = ingress.withLock { state -> Bool? in
            guard !state.isShutdown else {
                return nil
            }

            let wasEmpty = state.pending.isEmpty
            state.pending.append(pending)
            return wasEmpty
        }

        guard let needsDrain else {
            Task {
                await valueBox.fail(.shutdown)
            }
            return
        }

        // Only the empty-to-non-empty transition needs a wakeup: a non-empty
        // buffer means a drain task is already pending and will pick this up,
        // so high-rate publishing doesn't allocate one task per message.
        if needsDrain {
            Task {
                await processIngress()
            }
        }
    }

    private func processIngress() {
        let pendingPublishes = ingress.withLock { state in
            let result = state.pending
            state.pending.removeAll(keepingCapacity: true)
            return result
        }

        for pending in pendingPublishes {
            enqueuePending(pending)
        }
    }

    private func enqueuePending(_ pending: PendingPublish) {
        guard publishRequestByteCount(for: [pending]) <= BatchingOptions.maxBytes else {
            reject(
                pending,
                with: .service(
                    PubSubServiceError(
                        code: .invalidArgument,
                        message: "serialized publish request exceeds the 10 MB Pub/Sub limit"
                    )
                )
            )
            return
        }

        let key = pending.message.orderingKey
        var state = batches[key, default: BatchState()]
        guard !state.paused else {
            reject(pending, with: .orderingKeyPaused)
            return
        }

        state.pending.append(pending)
        state.pendingByteCount += pending.serializedByteCount
        batches[key] = state

        if shouldFlush(state) {
            flushOrderingKey(key)
            return
        }

        scheduleFlushIfNeeded(for: key)
    }

    private func reject(_ pending: PendingPublish, with error: PublishError) {
        let valueBox = pending.valueBox
        localCompletions[ObjectIdentifier(valueBox)] = valueBox
        Task {
            await valueBox.fail(error)
            finishLocalCompletion(valueBox)
        }
    }

    private func finishLocalCompletion(_ valueBox: AsyncValue<String, PublishError>) {
        localCompletions[ObjectIdentifier(valueBox)] = nil
    }

    func flush() async {
        processIngress()
        let localBoxes = Array(localCompletions.values)
        let batched = Array(batches.keys).flatMap { requestFlush(for: $0) }

        for valueBox in localBoxes {
            _ = await valueBox.terminalResult()
        }
        for valueBox in batched {
            _ = await valueBox.terminalResult()
        }
    }

    func shutdown() async {
        // New publishes fail fast with .shutdown; the drain below keeps the full
        // retry stack (bounded by the 600s publish budget), matching upstream's
        // graceful drop-driven drain.
        let pendingPublishes = ingress.withLock { state in
            state.isShutdown = true
            let pending = state.pending
            state.pending.removeAll(keepingCapacity: true)
            return pending
        }
        for pending in pendingPublishes {
            enqueuePending(pending)
        }

        await Task { [self] in
            await flush()
        }.value
        if ownsService {
            await service.shutdown()
        }
    }

    func resumePublish(_ orderingKey: String) {
        // publish() commits to the synchronized ingress before returning. Drain
        // that ingress while the key is still paused so every earlier publish is
        // classified before resume takes effect.
        processIngress()

        guard var state = batches[orderingKey] else {
            return
        }

        state.paused = false
        if isIdle(state) {
            batches[orderingKey] = nil
            return
        }

        batches[orderingKey] = state
        if shouldFlush(state) {
            flushOrderingKey(orderingKey)
        }
    }

    private func shouldFlush(_ state: BatchState) -> Bool {
        !state.inFlight
            && (state.pending.count >= batchingOptions.messageCountThreshold
                || state.pendingByteCount >= batchingOptions.byteThreshold)
    }

    private func scheduleFlushIfNeeded(for orderingKey: String) {
        guard var state = batches[orderingKey], !state.inFlight, state.scheduledFlush == nil else {
            return
        }

        state.scheduledFlush = Task { [self] in
            try? await Task.sleep(for: batchingOptions.delayThreshold)
            guard !Task.isCancelled else {
                return
            }
            flushOrderingKey(orderingKey)
        }
        batches[orderingKey] = state
    }

    private func flushOrderingKey(_ orderingKey: String) {
        guard var state = batches[orderingKey], !state.pending.isEmpty else {
            return
        }

        if orderingKey.isEmpty {
            // Unordered messages publish concurrently: every batch goes in flight
            // immediately, like upstream's concurrent batch actor for the empty
            // ordering key, so one slow batch never head-of-line blocks the rest.
            state.scheduledFlush?.cancel()
            state.scheduledFlush = nil
            let entries = state.pending
            state.pending = []
            state.pendingByteCount = 0
            state.inFlightEntries.append(contentsOf: entries)
            batches[orderingKey] = state

            var remaining = entries[...]
            while !remaining.isEmpty {
                let chunk = firstChunkWithinBatchLimits(Array(remaining))
                remaining = remaining.dropFirst(chunk.entries.count)
                let chunkEntries = chunk.entries
                Task {
                    let result = await publishBatch(chunkEntries, orderingKey: orderingKey)
                    await finishConcurrentBatch(entries: chunkEntries, result: result)
                }
            }
            return
        }

        guard !state.inFlight else {
            return
        }

        state.scheduledFlush?.cancel()
        state.scheduledFlush = nil

        // Ordered keys publish one batch at a time to preserve ordering: take
        // the first chunk within the batch limits and leave the rest pending for
        // the next finishBatch pass.
        let chunk = firstChunkWithinBatchLimits(state.pending)
        state.inFlight = true
        state.inFlightEntries = chunk.entries
        state.pending.removeFirst(chunk.entries.count)
        state.pendingByteCount -= chunk.messageByteCount
        batches[orderingKey] = state

        let chunkEntries = chunk.entries
        Task {
            let result = await publishBatch(chunkEntries, orderingKey: orderingKey)
            await finishBatch(orderingKey: orderingKey, entries: chunkEntries, result: result)
        }
    }

    /// The largest prefix within the configured batch thresholds (which are
    /// normalized to the 1,000-message/10MB API limits). Upstream caps every
    /// request at the configured thresholds, not just the API hard caps. A
    /// single over-sized message still goes alone — it cannot be split.
    private func firstChunkWithinBatchLimits(
        _ entries: [PendingPublish]
    ) -> (entries: [PendingPublish], messageByteCount: Int) {
        var count = 0
        var messageBytes = 0
        var requestBytes = encodedStringFieldByteCount(topic)
        for entry in entries {
            let entryBytes = entry.serializedByteCount
            let framedEntryBytes = ProtobufSize.lengthDelimitedField(
                fieldNumber: 2, payloadBytes: entryBytes)
            if count > 0,
                count + 1 > batchingOptions.messageCountThreshold
                    || messageBytes + entryBytes > batchingOptions.byteThreshold
                    || requestBytes + framedEntryBytes > BatchingOptions.maxBytes
            {
                break
            }

            count += 1
            messageBytes += entryBytes
            requestBytes += framedEntryBytes
        }

        return (Array(entries.prefix(count)), messageBytes)
    }

    private func publishRequestByteCount(for entries: [PendingPublish]) -> Int {
        entries.reduce(encodedStringFieldByteCount(topic)) { partialResult, entry in
            let entryBytes = entry.serializedByteCount
            return partialResult
                + ProtobufSize.lengthDelimitedField(fieldNumber: 2, payloadBytes: entryBytes)
        }
    }

    private func encodedStringFieldByteCount(_ value: String) -> Int {
        ProtobufSize.stringField(fieldNumber: 1, value: value)
    }

    private func requestFlush(for orderingKey: String) -> [AsyncValue<String, PublishError>] {
        guard var state = batches[orderingKey] else {
            return []
        }

        let valueBoxes = (state.inFlightEntries + state.completingEntries + state.pending).map(
            \.valueBox)
        state.flushRequested = true
        batches[orderingKey] = state
        if state.inFlight == false {
            flushOrderingKey(orderingKey)
        }

        return valueBoxes
    }

    private func publishBatch(_ entries: [PendingPublish], orderingKey: String) async -> Result<
        [String], PublishError
    > {
        do {
            let ids = try await withRetry(policy: publishRetryPolicy) { remainingTime in
                try await self.service.publish(
                    topic: self.topic,
                    messages: entries.map(\.message),
                    timeout: remainingTime
                )
            }

            // A short response would otherwise leave the surplus futures unresolved
            // forever; fail the whole batch loudly instead.
            guard ids.count == entries.count else {
                return .failure(
                    .service(
                        PubSubServiceError(
                            message:
                                "publish returned \(ids.count) message IDs for \(entries.count) messages"
                        )
                    )
                )
            }

            return .success(ids)
        } catch let error as PubSubServiceError {
            return .failure(.service(error))
        } catch is CancellationError {
            return .failure(
                .service(PubSubServiceError(code: .cancelled, message: "publish cancelled")))
        } catch {
            return .failure(.service(asServiceError(error)))
        }
    }

    private func finishBatch(
        orderingKey: String,
        entries: [PendingPublish],
        result: Result<[String], PublishError>
    ) async {
        guard var state = batches[orderingKey] else {
            for entry in entries {
                await entry.valueBox.fail(.shutdown)
            }
            return
        }

        // Mutate the stored state synchronously, before any suspension: awaiting
        // mid-update and writing a stale copy back would clobber messages that
        // publish() enqueues while this actor method is suspended.
        state.inFlight = false
        state.inFlightEntries = []

        var failedPending: [PendingPublish] = []
        if case .failure = result, !orderingKey.isEmpty {
            state.paused = true
            failedPending = state.pending
            state.pending = []
            state.pendingByteCount = 0
        }
        state.completingEntries.append(contentsOf: entries + failedPending)

        batches[orderingKey] = state

        switch result {
        case .success(let ids):
            for (entry, id) in zip(entries, ids) {
                await entry.valueBox.succeed(id)
            }
        case .failure(let error):
            for entry in entries {
                await entry.valueBox.fail(error)
            }
            for entry in failedPending {
                await entry.valueBox.fail(.orderingKeyPaused)
            }
        }

        // The state may have changed during the awaits above; re-read it before
        // deciding whether to flush again.
        guard var current = batches[orderingKey] else {
            return
        }
        let completed = Set((entries + failedPending).map { ObjectIdentifier($0.valueBox) })
        current.completingEntries.removeAll {
            completed.contains(ObjectIdentifier($0.valueBox))
        }
        if current.flushRequested, current.pending.isEmpty {
            current.flushRequested = false
        }
        if isIdle(current) {
            // Evict completed, unpaused keys so the dictionary doesn't grow
            // unboundedly with high-cardinality ordering keys.
            batches[orderingKey] = nil
            return
        }
        batches[orderingKey] = current

        if current.flushRequested, !current.pending.isEmpty {
            flushOrderingKey(orderingKey)
        } else if shouldFlush(current) {
            flushOrderingKey(orderingKey)
        } else if !current.pending.isEmpty {
            scheduleFlushIfNeeded(for: orderingKey)
        }
    }

    /// Completion for a concurrent unordered batch: failures fail only that
    /// batch's futures (unordered publishing never pauses), and the shared
    /// in-flight list sheds just this batch's entries.
    private func finishConcurrentBatch(
        entries: [PendingPublish],
        result: Result<[String], PublishError>
    ) async {
        // Same reentrancy rule as finishBatch: update the stored state before
        // any suspension, then resolve the futures.
        if var state = batches[""] {
            let finished = Set(entries.map { ObjectIdentifier($0.valueBox) })
            state.inFlightEntries.removeAll { finished.contains(ObjectIdentifier($0.valueBox)) }
            state.completingEntries.append(contentsOf: entries)
            batches[""] = state
        }

        switch result {
        case .success(let ids):
            for (entry, id) in zip(entries, ids) {
                await entry.valueBox.succeed(id)
            }
        case .failure(let error):
            for entry in entries {
                await entry.valueBox.fail(error)
            }
        }

        guard var state = batches[""] else {
            return
        }
        let completed = Set(entries.map { ObjectIdentifier($0.valueBox) })
        state.completingEntries.removeAll { completed.contains(ObjectIdentifier($0.valueBox)) }
        if isIdle(state) {
            batches[""] = nil
        } else {
            batches[""] = state
        }
    }

    private func isIdle(_ state: BatchState) -> Bool {
        state.pending.isEmpty && state.inFlightEntries.isEmpty && state.completingEntries.isEmpty
            && !state.paused
            && state.scheduledFlush == nil
    }
}
