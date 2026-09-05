import Foundation
import GRPCCore
import GRPCProtobuf
import Synchronization
import Testing

@testable import PubSub

private actor FakeSubscriberService: SubscriberRPC {
    struct StreamingPullRequest: Sendable, Equatable {
        let subscription: String
        let streamAckDeadlineSeconds: Int32
        let maxOutstandingMessages: Int
        let maxOutstandingBytes: Int
        let clientID: String
    }

    private var streamingResponses: [Google_Pubsub_V1_StreamingPullResponse]
    private var acknowledgeFailuresRemaining: Int
    private var acknowledgeRPCFailures: [RPCError]
    private var acknowledgeServiceFailures: [PubSubServiceError]
    private var nackRPCFailures: [RPCError]
    private let streamingFailure: PubSubServiceError?
    let acknowledgementAttempted = AsyncValue<Void, PubSubServiceError>()
    let streamingResponsesDelivered = AsyncValue<Void, PubSubServiceError>()
    private(set) var streamingPullRequests: [StreamingPullRequest] = []
    private(set) var acknowledgeAttempts = 0
    private(set) var acknowledgedBatches: [[String]] = []
    private(set) var acknowledgeTimeouts: [Duration] = []
    private(set) var modifiedAckDeadlineBatches:
        [(ids: [String], seconds: Int32, timeout: Duration?)] = []

    init(
        streamingResponses: [Google_Pubsub_V1_StreamingPullResponse] = [],
        acknowledgeFailures: Int = 0,
        acknowledgeRPCFailures: [RPCError] = [],
        acknowledgeServiceFailures: [PubSubServiceError] = [],
        nackRPCFailures: [RPCError] = [],
        streamingFailure: PubSubServiceError? = nil
    ) {
        self.streamingResponses = streamingResponses
        self.acknowledgeFailuresRemaining = acknowledgeFailures
        self.acknowledgeRPCFailures = acknowledgeRPCFailures
        self.acknowledgeServiceFailures = acknowledgeServiceFailures
        self.nackRPCFailures = nackRPCFailures
        self.streamingFailure = streamingFailure
    }

    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws {
        streamingPullRequests.append(
            StreamingPullRequest(
                subscription: subscription,
                streamAckDeadlineSeconds: streamAckDeadlineSeconds,
                maxOutstandingMessages: maxOutstandingMessages,
                maxOutstandingBytes: maxOutstandingBytes,
                clientID: clientID
            )
        )
        if let streamingFailure {
            throw streamingFailure
        }

        let responses = streamingResponses
        streamingResponses.removeAll(keepingCapacity: true)

        await onConnected()
        for response in responses {
            try await onResponse(response)
        }
        await streamingResponsesDelivered.succeed(())
        while !Task.isCancelled {
            try await Task.sleep(for: .seconds(60))
        }
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {
        acknowledgeAttempts += 1
        if let timeout {
            acknowledgeTimeouts.append(timeout)
        }
        await acknowledgementAttempted.succeed(())
        if !acknowledgeRPCFailures.isEmpty {
            throw acknowledgeRPCFailures.removeFirst()
        }
        if !acknowledgeServiceFailures.isEmpty {
            throw acknowledgeServiceFailures.removeFirst()
        }
        if acknowledgeFailuresRemaining > 0 {
            acknowledgeFailuresRemaining -= 1
            throw PubSubServiceError(code: .unavailable, message: "transient failure")
        }

        acknowledgedBatches.append(ackIDs)
    }

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {
        modifiedAckDeadlineBatches.append((ackIDs, ackDeadlineSeconds, timeout))
        if ackDeadlineSeconds == 0, !nackRPCFailures.isEmpty {
            throw nackRPCFailures.removeFirst()
        }
    }
}

/// Two idle-but-successfully-opened streams fail before a third remains open.
/// A successful reconnect must reset the failure window even with no response.
private actor IdleReconnectSubscriberService: SubscriberRPC {
    let thirdConnectionOpened = AsyncValue<Void, PubSubServiceError>()
    private(set) var connectionAttempts = 0

    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws {
        connectionAttempts += 1
        let attempt = connectionAttempts
        await onConnected()
        if attempt <= 2 {
            throw PubSubServiceError(code: .unavailable, message: "connection lost")
        }

        await thirdConnectionOpened.succeed(())
        while !Task.isCancelled {
            try await Task.sleep(for: .seconds(60))
        }
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {}

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {}
}

/// Holds the first lease-extension RPC open while observing whether later
/// sweeps can extend a newly registered lease independently.
private actor GatedLeaseExtensionSubscriberService: SubscriberRPC {
    let firstExtensionStarted = AsyncValue<Void, PubSubServiceError>()
    let secondExtensionStarted = AsyncValue<Void, PubSubServiceError>()
    private let firstExtensionGate = AsyncValue<Void, PubSubServiceError>()
    private var positiveExtensionAttempts = 0

    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws {
        await onConnected()
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {}

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {
        guard ackDeadlineSeconds > 0 else {
            return
        }

        positiveExtensionAttempts += 1
        if positiveExtensionAttempts == 1 {
            await firstExtensionStarted.succeed(())
            _ = await firstExtensionGate.terminalResult()
        } else {
            await secondExtensionStarted.succeed(())
        }
    }

    func releaseFirstExtension() async {
        await firstExtensionGate.succeed(())
    }
}

private actor WrappedCancellationSubscriberService: SubscriberRPC {
    private(set) var acknowledgeAttempts = 0
    private(set) var acknowledgedIDs: [String] = []

    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws {
        await onConnected()
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {
        acknowledgeAttempts += 1
        if acknowledgeAttempts == 1 {
            throw RPCError(code: .unknown, message: "", cause: CancellationError())
        }
        acknowledgedIDs.append(contentsOf: ackIDs)
    }

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {}
}

private final class StreamLifecycle: @unchecked Sendable {
    private struct State: Sendable {
        var terminated = false
        var shutdownObservedTermination = false
    }

    private let state = Mutex(State())

    func markTerminated() {
        state.withLock { state in
            state.terminated = true
        }
    }

    func markServiceShutdown() {
        state.withLock { state in
            state.shutdownObservedTermination = state.terminated
        }
    }

    var shutdownObservedTermination: Bool {
        state.withLock { $0.shutdownObservedTermination }
    }
}

private actor ShutdownTrackingSubscriberService: SubscriberRPC {
    let streamStarted = AsyncValue<Void, PubSubServiceError>()
    let lifecycle = StreamLifecycle()

    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws {
        await streamStarted.succeed(())
        let lifecycle = self.lifecycle
        defer { lifecycle.markTerminated() }
        await onConnected()
        while !Task.isCancelled {
            try await Task.sleep(for: .seconds(60))
        }
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {}

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {}

    func shutdown() async {
        lifecycle.markServiceShutdown()
    }
}

private actor CancellationGatedSubscriberService: SubscriberRPC {
    let firstAttemptStarted = AsyncValue<Void, PubSubServiceError>()
    private let firstAttemptGate = AsyncValue<Void, PubSubServiceError>()
    private(set) var acknowledgeAttempts = 0
    private(set) var acknowledgedBatches: [[String]] = []

    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws {
        await onConnected()
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {
        acknowledgeAttempts += 1
        if acknowledgeAttempts == 1 {
            await firstAttemptStarted.succeed(())
            try await firstAttemptGate.value()
        }
        acknowledgedBatches.append(ackIDs)
    }

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {}
}

/// Fails every acknowledge batch that contains `poisonedID`, so a single ID can
/// hold a whole chunk in the retry loop until its own budget runs out.
private actor PoisonedAckSubscriberService: SubscriberRPC {
    private let poisonedID: String
    private(set) var acknowledgedBatches: [[String]] = []
    private(set) var acknowledgeAttempts: [(ids: [String], timeout: Duration?)] = []

    init(poisonedID: String) {
        self.poisonedID = poisonedID
    }

    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws {
        await onConnected()
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {
        acknowledgeAttempts.append((ackIDs.sorted(), timeout))
        guard !ackIDs.contains(poisonedID) else {
            throw PubSubServiceError(code: .unavailable, message: "transient failure")
        }

        acknowledgedBatches.append(ackIDs)
    }

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {}
}

/// Holds its first acknowledge open until released, so a second full batch can
/// accumulate behind an immediate flush that is already in flight.
private actor GatedAckSubscriberService: SubscriberRPC {
    let firstAttemptStarted = AsyncValue<Void, PubSubServiceError>()
    private let gate = AsyncValue<Void, PubSubServiceError>()
    private var attempts = 0
    private(set) var acknowledgedBatches: [[String]] = []

    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws {
        await onConnected()
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {
        attempts += 1
        if attempts == 1 {
            await firstAttemptStarted.succeed(())
            _ = await gate.terminalResult()
        }

        acknowledgedBatches.append(ackIDs)
    }

    func releaseGate() async {
        await gate.succeed(())
    }

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {}
}

/// Returns a genuine per-ID rejection, but only after its caller has been
/// cancelled — the shape of a permanent failure racing a shutdown.
private actor CancelledThenRejectingSubscriberService: SubscriberRPC {
    let firstAttemptStarted = AsyncValue<Void, PubSubServiceError>()
    private(set) var acknowledgeAttempts = 0

    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws {
        await onConnected()
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {
        acknowledgeAttempts += 1
        guard acknowledgeAttempts == 1 else {
            return
        }

        await firstAttemptStarted.succeed(())
        while !Task.isCancelled {
            try? await Task.sleep(for: .milliseconds(1))
        }

        throw RPCError(
            GoogleRPCStatus(
                code: .failedPrecondition,
                message: "partial acknowledgement failure",
                details: .errorInfo(
                    reason: "EXACTLY_ONCE_ACK_FAILURE",
                    domain: "pubsub.googleapis.com",
                    metadata: ["ack-1": "PERMANENT_FAILURE_INVALID_ACK_ID"]
                )
            )
        )
    }

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {}
}

/// Records how many acknowledge RPCs are in flight at once. The first RPC
/// yields until a peer enters (or a bounded deadline passes), so cancellation
/// cannot collapse the observation window before the second chunk is scheduled.
private actor ConcurrencyTrackingSubscriberService: SubscriberRPC {
    private var inFlight = 0
    private(set) var peakInFlight = 0
    private(set) var acknowledgedBatches: [[String]] = []

    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws {
        await onConnected()
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {
        inFlight += 1
        peakInFlight = max(peakInFlight, inFlight)
        if peakInFlight == 1 {
            let peerDeadline = ContinuousClock().now + .seconds(1)
            while peakInFlight == 1, ContinuousClock().now < peerDeadline {
                await Task.yield()
            }
        }
        inFlight -= 1
        acknowledgedBatches.append(ackIDs)
    }

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {}
}

/// Holds a positive lease extension open until cancellation and records when
/// the final zero-deadline nack arrives, making their shutdown ordering
/// deterministic rather than timing-dependent.
private actor LeaseShutdownOrderingSubscriberService: SubscriberRPC {
    let positiveExtensionStarted = AsyncValue<Void, PubSubServiceError>()
    private var extensionActive = false
    private(set) var acknowledgementObservedActiveExtension = false
    private(set) var events: [String] = []

    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws {
        await onConnected()
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {
        acknowledgementObservedActiveExtension = extensionActive
    }

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {
        guard ackDeadlineSeconds > 0 else {
            events.append("nack")
            return
        }

        events.append("extension-started")
        extensionActive = true
        await positiveExtensionStarted.succeed(())
        do {
            try await Task.sleep(for: .seconds(60))
        } catch {
            extensionActive = false
            events.append("extension-stopped")
            throw error
        }
    }
}

private func makeReceivedMessage(
    _ ackID: String,
    body: String,
    deliveryAttempt: Int32 = 0
) -> ReceivedMessage {
    var received = ReceivedMessage()
    received.ackID = ackID
    received.message = Message(data: Data(body.utf8))
    received.deliveryAttempt = deliveryAttempt
    return received
}

private func makeStreamingPullResponse(
    _ messages: [ReceivedMessage],
    exactlyOnce: Bool = false
) -> Google_Pubsub_V1_StreamingPullResponse {
    var response = Google_Pubsub_V1_StreamingPullResponse()
    response.receivedMessages = messages

    var subscriptionProperties = Google_Pubsub_V1_StreamingPullResponse.SubscriptionProperties()
    subscriptionProperties.exactlyOnceDeliveryEnabled = exactlyOnce
    response.subscriptionProperties = subscriptionProperties

    return response
}

@Suite("Subscriber")
struct SubscriberTests {
    @Test("Subscriber shutdown finishes active streams before closing the service")
    func subscriberShutdownStopsActiveStreams() async throws {
        let service = ShutdownTrackingSubscriberService()
        let subscriber = Subscriber(service: service, configuration: ClientConfiguration())
        let stream = subscriber.subscribe("projects/p/subscriptions/s").build()

        try await service.streamStarted.value()
        await subscriber.shutdown()

        #expect(service.lifecycle.shutdownObservedTermination)
        await stream.shutdownToken().shutdown()
    }

    @Test("Subscribe builder clamps the ack deadline extension")
    func clampAckDeadline() {
        let service = FakeSubscriberService()
        let builder = Subscribe(
            service: service,
            subscription: "projects/p/subscriptions/s",
            retryPolicy: RetryPolicy()
        )
        .setMaxLeaseExtension(.seconds(0))
        #expect(builder.maxLeaseExtension == .seconds(10))

        let builder2 = builder.setMaxLeaseExtension(.seconds(4_200))
        #expect(builder2.maxLeaseExtension == .seconds(600))
    }

    @Test("Dropping an at-least-once handler triggers a nack")
    func atLeastOnceDropNacks() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024
        )
        await leaseManager.start()

        do {
            _ = await leaseManager.register(
                makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: false)
        }

        try await Task.sleep(for: .milliseconds(250))
        let modified = await service.modifiedAckDeadlineBatches
        #expect(modified.contains { $0.ids == ["ack-1"] && $0.seconds == 0 })

        await leaseManager.shutdown()
    }

    @Test("Flow control returns zero when outstanding message capacity is exhausted")
    func flowControlStopsAtOutstandingMessageLimit() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 2,
            maxOutstandingBytes: 1024
        )

        let first = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: false)
        let second = await leaseManager.register(
            makeReceivedMessage("ack-2", body: "world"), exactlyOnce: false)

        #expect(await leaseManager.availablePullCapacity(defaultBatchSize: 100) == 0)
        first.ack()
        second.ack()
    }

    @Test("Flow control returns zero when outstanding byte capacity is exhausted")
    func flowControlStopsAtOutstandingByteLimit() async {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 0,
            maxOutstandingBytes: 5
        )

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: false)

        #expect(await leaseManager.availablePullCapacity(defaultBatchSize: 100) == 0)
        handler.ack()
        await leaseManager.shutdown()
    }

    @Test("Exactly-once leased messages fail confirmation when the max lease expires")
    func exactlyOnceLeaseExpiryCompletesAckFuture() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .zero,
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            extendInterval: .milliseconds(10)
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: true)
        guard case .exactlyOnce(let exactlyOnce) = handler else {
            Issue.record("expected an exactly-once handler")
            return
        }

        try await Task.sleep(for: .milliseconds(50))
        await #expect(throws: AckError.leaseExpired) {
            try await exactlyOnce.confirmedAck()
        }

        await leaseManager.shutdown()
    }

    @Test("Exactly-once confirmed ack resolves after the ack batch flushes")
    func exactlyOnceConfirmedAck() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: true)
        guard case .exactlyOnce(let exactlyOnce) = handler else {
            Issue.record("expected an exactly-once handler")
            return
        }

        try await exactlyOnce.confirmedAck()
        #expect(await service.acknowledgedBatches.contains(["ack-1"]))

        await leaseManager.shutdown()
    }

    @Test("MessageStream yields streamed messages and forwards acks")
    func messageStreamNext() async throws {
        let service = FakeSubscriberService(
            streamingResponses: [
                makeStreamingPullResponse([makeReceivedMessage("ack-1", body: "hello")])
            ]
        )

        let subscribe = Subscribe(
            service: service,
            subscription: "projects/p/subscriptions/s",
            retryPolicy: RetryPolicy(retryableCodes: [], maxAttempts: 1)
        )
        let stream = MessageStream(subscribe: subscribe)

        guard let (message, handler) = try await stream.next() else {
            await stream.shutdownToken().shutdown()
            Issue.record("expected a message")
            return
        }

        #expect(String(decoding: message.data, as: UTF8.self) == "hello")
        handler.ack()

        try await Task.sleep(for: .milliseconds(250))
        #expect(await service.acknowledgedBatches.contains(["ack-1"]))
        let requests = await service.streamingPullRequests
        #expect(requests.count == 1)
        #expect(requests.first?.subscription == "projects/p/subscriptions/s")
        #expect(requests.first?.streamAckDeadlineSeconds == 60)
        #expect(requests.first?.maxOutstandingMessages == 1_000)
        #expect(requests.first?.maxOutstandingBytes == 100 * 1_024 * 1_024)
        #expect(requests.first?.clientID.isEmpty == false)

        await stream.shutdownToken().shutdown()
    }

    @Test("MessageStream registers a complete response before consumer capacity frees")
    func messageStreamLeasesTheCompleteResponse() async throws {
        let service = FakeSubscriberService(
            streamingResponses: [
                makeStreamingPullResponse([
                    makeReceivedMessage("ack-1", body: "first"),
                    makeReceivedMessage("ack-2", body: "second"),
                ])
            ]
        )
        let subscribe = Subscribe(
            service: service,
            subscription: "projects/p/subscriptions/s",
            retryPolicy: RetryPolicy(retryableCodes: [], maxAttempts: 1)
        )
        .setMaxOutstandingMessages(1)
        let stream = MessageStream(subscribe: subscribe)

        try await service.streamingResponsesDelivered.value()
        try await Task.sleep(for: .milliseconds(750))
        let extendedIDs = Set(
            await service.modifiedAckDeadlineBatches
                .filter { $0.seconds > 0 }
                .flatMap(\.ids)
        )
        #expect(extendedIDs == ["ack-1", "ack-2"])

        await stream.shutdownToken().shutdown()
    }

    @Test("MessageStream shutdown nacks every buffered response")
    func messageStreamShutdownNacksAllBufferedResponses() async throws {
        let service = FakeSubscriberService(
            streamingResponses: [
                makeStreamingPullResponse([makeReceivedMessage("ack-1", body: "first")]),
                makeStreamingPullResponse([makeReceivedMessage("ack-2", body: "second")]),
            ]
        )
        let subscribe = Subscribe(
            service: service,
            subscription: "projects/p/subscriptions/s",
            retryPolicy: RetryPolicy(retryableCodes: [], maxAttempts: 1)
        )
        let stream = MessageStream(subscribe: subscribe)

        try await service.streamingResponsesDelivered.value()
        await stream.shutdownToken().shutdown()

        let nackedIDs = Set(
            await service.modifiedAckDeadlineBatches
                .filter { $0.seconds == 0 }
                .flatMap(\.ids)
        )
        #expect(nackedIDs == ["ack-1", "ack-2"])
    }

    @Test("An idle successful reconnect resets the stream failure budget")
    func idleReconnectResetsFailureBudget() async throws {
        let service = IdleReconnectSubscriberService()
        let subscribe = Subscribe(
            service: service,
            subscription: "projects/p/subscriptions/s",
            retryPolicy: RetryPolicy(
                initialBackoff: .zero,
                maxBackoff: .zero,
                maxAttempts: 2
            )
        )
        let stream = MessageStream(subscribe: subscribe)

        try await service.thirdConnectionOpened.value()
        #expect(await service.connectionAttempts == 3)
        await stream.shutdownToken().shutdown()
    }

    @Test("MessageStream uses exactly-once properties from streaming responses")
    func messageStreamUsesStreamingExactlyOnceProperties() async throws {
        let service = FakeSubscriberService(
            streamingResponses: [
                makeStreamingPullResponse(
                    [makeReceivedMessage("ack-1", body: "hello")],
                    exactlyOnce: true
                )
            ]
        )

        let subscribe = Subscribe(
            service: service,
            subscription: "projects/p/subscriptions/s",
            retryPolicy: RetryPolicy(retryableCodes: [], maxAttempts: 1)
        )
        let stream = MessageStream(subscribe: subscribe)

        guard let (_, handler) = try await stream.next() else {
            await stream.shutdownToken().shutdown()
            Issue.record("expected a message")
            return
        }

        guard case .exactlyOnce(let exactlyOnce) = handler else {
            await stream.shutdownToken().shutdown()
            Issue.record("expected exactly-once handler from streaming response properties")
            return
        }

        try await exactlyOnce.confirmedAck()
        #expect(await service.acknowledgedBatches.contains(["ack-1"]))

        await stream.shutdownToken().shutdown()
    }

    @Test("Ack and nack RPCs are chunked to the per-request ID cap")
    func ackRPCsAreChunked() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 5_000,
            maxOutstandingBytes: 50 * 1_024 * 1_024
        )

        var handlers: [Handler] = []
        for index in 0..<1_500 {
            handlers.append(
                await leaseManager.register(
                    makeReceivedMessage("ack-\(index)", body: "m"), exactlyOnce: false))
        }

        await leaseManager.shutdown()

        let batches = await service.modifiedAckDeadlineBatches
        #expect(batches.allSatisfy { $0.ids.count <= LeaseManager.maxIDsPerRPC })
        #expect(batches.filter { $0.seconds == 0 }.reduce(0) { $0 + $1.ids.count } == 1_500)
        withExtendedLifetime(handlers) {}
    }

    @Test("Transient ack failures are requeued and retried")
    func transientAckFailureIsRetried() async throws {
        let service = FakeSubscriberService(acknowledgeFailures: 1)
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(50)
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: false)
        handler.ack()

        try await Task.sleep(for: .milliseconds(400))
        #expect(await service.acknowledgedBatches.contains(["ack-1"]))

        await leaseManager.shutdown()
    }

    @Test("Shutdown drains an acknowledgement cancelled in a periodic flush")
    func shutdownDrainsCancelledInFlightAcknowledgement() async throws {
        let service = CancellationGatedSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(1)
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: false)
        handler.ack()
        try await service.firstAttemptStarted.value()

        await leaseManager.shutdown()

        #expect(await service.acknowledgeAttempts == 2)
        #expect(await service.acknowledgedBatches.contains(["ack-1"]))
    }

    @Test("Acknowledgement retries that exceed the elapsed budget fail without another RPC")
    func acknowledgementRetryHonorsElapsedBudget() async throws {
        let service = FakeSubscriberService(acknowledgeFailures: 1)
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(1),
            retryPolicy: RetryPolicy(maxAttempts: 3, maxElapsedTime: .milliseconds(100)),
            ackRetryDelay: { _ in .seconds(1) }
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: true)
        guard case .exactlyOnce(let exactlyOnce) = handler else {
            Issue.record("expected an exactly-once handler")
            return
        }

        await #expect(throws: AckError.self) {
            try await exactlyOnce.confirmedAck()
        }
        #expect(await service.acknowledgeAttempts == 1)
        let timeouts = await service.acknowledgeTimeouts
        let timeout = try #require(timeouts.first)
        #expect(timeout > .zero)
        #expect(timeout <= .milliseconds(100))

        await leaseManager.shutdown()
    }

    @Test("Initial ack and nack RPCs receive their remaining elapsed-time budget")
    func initialAckAndNackRPCsReceiveRemainingBudget() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            retryPolicy: RetryPolicy(maxElapsedTime: .seconds(1))
        )

        let ackHandler = await leaseManager.register(
            makeReceivedMessage("ack", body: "a"), exactlyOnce: false)
        let nackHandler = await leaseManager.register(
            makeReceivedMessage("nack", body: "b"), exactlyOnce: false)
        ackHandler.ack()
        nackHandler.nack()
        await leaseManager.shutdown()

        let acknowledgeTimeouts = await service.acknowledgeTimeouts
        let acknowledgeTimeout = try #require(acknowledgeTimeouts.first)
        #expect(acknowledgeTimeout > .zero)
        #expect(acknowledgeTimeout <= .seconds(1))

        let modifiedAckDeadlines = await service.modifiedAckDeadlineBatches
        let nackAttempt = try #require(
            modifiedAckDeadlines.first { $0.ids == ["nack"] && $0.seconds == 0 })
        let nackTimeout = try #require(nackAttempt.timeout)
        #expect(nackTimeout > .zero)
        #expect(nackTimeout <= .seconds(1))
    }

    @Test("Final ack and nack RPCs use a positive floor after shutdown grace expires")
    func shutdownCapsFinalAckAndNackTimeouts() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            shutdownGracePeriod: .zero,
            retryPolicy: RetryPolicy(maxElapsedTime: .seconds(600))
        )

        let ackHandler = await leaseManager.register(
            makeReceivedMessage("ack", body: "a"), exactlyOnce: false)
        let nackHandler = await leaseManager.register(
            makeReceivedMessage("nack", body: "b"), exactlyOnce: false)
        ackHandler.ack()

        await leaseManager.shutdown()

        #expect(await service.acknowledgeTimeouts == [LeaseManager.minimumShutdownRPCTimeout])
        let finalNack = try #require(
            await service.modifiedAckDeadlineBatches.first {
                $0.ids == ["nack"] && $0.seconds == 0
            })
        #expect(finalNack.timeout == LeaseManager.minimumShutdownRPCTimeout)
        withExtendedLifetime(nackHandler) {}
    }

    @Test("Shutdown auto-nacks fail unconsumed exactly-once confirmations")
    func shutdownAutoNackFailsExactlyOnceConfirmation() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            shutdownGracePeriod: .seconds(1)
        )

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: true)
        guard case .exactlyOnce(let exactlyOnce) = handler else {
            Issue.record("expected an exactly-once handler")
            return
        }

        await leaseManager.shutdown()

        do {
            try await exactlyOnce.confirmedAck()
            Issue.record("expected shutdown auto-nack to fail confirmation")
        } catch AckError.shutdown {
            // Expected: transport success confirmed a shutdown nack, not an ack.
        } catch {
            Issue.record("expected a shutdown error, got \(error)")
        }
    }

    @Test("Lease extension stops before shutdown sends final nacks")
    func shutdownStopsLeaseExtensionBeforeFinalNacks() async throws {
        let service = LeaseShutdownOrderingSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            shutdownGracePeriod: .zero,
            extendInterval: .seconds(3),
            extendStart: .zero
        )

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: false)
        await leaseManager.start()
        try await service.positiveExtensionStarted.value()

        await leaseManager.shutdown()

        #expect(await service.events == ["extension-started", "extension-stopped", "nack"])
        withExtendedLifetime(handler) {}
    }

    @Test("Pending exactly-once acknowledgements stay extended through shutdown drain")
    func shutdownKeepsPendingExactlyOnceAckExtended() async throws {
        let service = LeaseShutdownOrderingSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            shutdownGracePeriod: .seconds(1),
            extendInterval: .seconds(3),
            extendStart: .zero
        )

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: true)
        guard case .exactlyOnce(let exactlyOnce) = handler else {
            Issue.record("expected an exactly-once handler")
            return
        }
        await leaseManager.start()
        try await service.positiveExtensionStarted.value()
        exactlyOnce.ack()

        await leaseManager.shutdown()

        #expect(await service.acknowledgementObservedActiveExtension)
        try await exactlyOnce.waitForConfirmation()
    }

    @Test("Codeless acknowledgement errors fail without retrying")
    func codelessAcknowledgementErrorFailsFast() async throws {
        let codelessError = PubSubServiceError(message: "internal acknowledgement failure")
        let service = FakeSubscriberService(
            acknowledgeServiceFailures: [codelessError, codelessError]
        )
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(1),
            retryPolicy: RetryPolicy(maxAttempts: 2, maxElapsedTime: .seconds(600)),
            ackRetryDelay: { _ in .zero }
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: true)
        guard case .exactlyOnce(let exactlyOnce) = handler else {
            Issue.record("expected an exactly-once handler")
            return
        }

        await #expect(throws: AckError.self) {
            try await exactlyOnce.confirmedAck()
        }
        #expect(await service.acknowledgeAttempts == 1)

        await leaseManager.shutdown()
    }

    @Test("Exactly-once batches resolve per-ID outcomes")
    func exactlyOncePerIDOutcomes() async throws {
        let status = GoogleRPCStatus(
            code: .failedPrecondition,
            message: "partial acknowledgement failure",
            details: .errorInfo(
                reason: "EXACTLY_ONCE_ACK_FAILURE",
                domain: "pubsub.googleapis.com",
                metadata: [
                    "permanent": "PERMANENT_FAILURE_INVALID_ACK_ID",
                    "transient": "TRANSIENT_FAILURE_INTERNAL",
                ]
            )
        )
        let service = FakeSubscriberService(acknowledgeRPCFailures: [RPCError(status)])
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(1),
            retryPolicy: RetryPolicy(initialBackoff: .zero, maxBackoff: .zero, maxAttempts: 3)
        )

        let permanentHandler = await leaseManager.register(
            makeReceivedMessage("permanent", body: "a"), exactlyOnce: true)
        let transientHandler = await leaseManager.register(
            makeReceivedMessage("transient", body: "b"), exactlyOnce: true)
        let successfulHandler = await leaseManager.register(
            makeReceivedMessage("successful", body: "c"), exactlyOnce: true)
        guard case .exactlyOnce(let permanent) = permanentHandler,
            case .exactlyOnce(let transient) = transientHandler,
            case .exactlyOnce(let successful) = successfulHandler
        else {
            Issue.record("expected exactly-once handlers")
            return
        }

        permanent.ack()
        transient.ack()
        successful.ack()
        await leaseManager.shutdown()

        do {
            try await permanent.waitForConfirmation()
            Issue.record("expected the permanent acknowledgement to fail")
        } catch AckError.service(let error) {
            #expect(
                error.errorInfo == [
                    PubSubErrorInfo(
                        reason: "EXACTLY_ONCE_ACK_FAILURE",
                        domain: "pubsub.googleapis.com",
                        metadata: [
                            "permanent": "PERMANENT_FAILURE_INVALID_ACK_ID",
                            "transient": "TRANSIENT_FAILURE_INTERNAL",
                        ]
                    )
                ]
            )
        } catch {
            Issue.record("expected a structured service error, got \(error)")
        }
        await #expect(throws: Never.self) {
            try await transient.waitForConfirmation()
        }
        await #expect(throws: Never.self) {
            try await successful.waitForConfirmation()
        }
        #expect(await service.acknowledgedBatches.contains(["transient"]))
    }

    @Test(
        "Unrelated ErrorInfo cannot confirm rejected acknowledgements",
        arguments: [
            (reason: "RATE_LIMIT_EXCEEDED", domain: "googleapis.com"),
            (reason: "RATE_LIMIT_EXCEEDED", domain: "pubsub.googleapis.com"),
            (reason: "EXACTLY_ONCE_ACK_FAILURE", domain: "another.googleapis.com"),
        ], [false, true]
    )
    func unrelatedErrorInfoDoesNotConfirmAcknowledgement(
        _ errorInfo: (reason: String, domain: String),
        nack: Bool
    ) async throws {
        let status = GoogleRPCStatus(
            code: .resourceExhausted,
            message: "quota exceeded",
            details: .errorInfo(
                reason: errorInfo.reason,
                domain: errorInfo.domain,
                metadata: ["consumer": "projects/p", "service": "pubsub.googleapis.com"]
            )
        )
        let rpcError = RPCError(status)
        let failures = [rpcError, rpcError]
        let service = FakeSubscriberService(
            acknowledgeRPCFailures: nack ? [] : failures,
            nackRPCFailures: nack ? failures : []
        )
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(1),
            retryPolicy: RetryPolicy(initialBackoff: .zero, maxBackoff: .zero, maxAttempts: 2)
        )
        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: true)
        guard case .exactlyOnce(let exactlyOnce) = handler else {
            Issue.record("expected an exactly-once handler")
            return
        }
        await leaseManager.start()

        await #expect(throws: AckError.service(PubSubServiceError(rpcError: rpcError))) {
            if nack {
                try await exactlyOnce.confirmedNack()
            } else {
                try await exactlyOnce.confirmedAck()
            }
        }
        await leaseManager.shutdown()

        if nack {
            let attempts = await service.modifiedAckDeadlineBatches.filter { $0.seconds == 0 }
            #expect(attempts.map(\.ids) == [["ack-1"], ["ack-1"]])
        } else {
            #expect(await service.acknowledgeAttempts == 2)
            #expect(await service.acknowledgedBatches.isEmpty)
        }
    }

    @Test("Unknown exactly-once dispositions fail closed")
    func unknownExactlyOnceDispositionFails() async throws {
        let status = GoogleRPCStatus(
            code: .failedPrecondition,
            message: "partial acknowledgement failure",
            details: .errorInfo(
                reason: "EXACTLY_ONCE_ACK_FAILURE",
                domain: "pubsub.googleapis.com",
                metadata: ["ack-1": "NEW_FAILURE_KIND"]
            )
        )
        let service = FakeSubscriberService(acknowledgeRPCFailures: [RPCError(status)])
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(1),
            retryPolicy: RetryPolicy(maxAttempts: 1)
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: true)
        guard case .exactlyOnce(let exactlyOnce) = handler else {
            Issue.record("expected an exactly-once handler")
            return
        }

        await #expect(throws: AckError.self) {
            try await exactlyOnce.confirmedAck()
        }
        await leaseManager.shutdown()
    }

    @Test("A wrapped transport cancellation is replayed during shutdown")
    func wrappedCancellationIsReplayedDuringShutdown() async {
        let service = WrappedCancellationSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(1),
            shutdownGracePeriod: .seconds(1)
        )
        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: false)
        handler.ack()

        await leaseManager.shutdown()

        #expect(await service.acknowledgeAttempts == 2)
        #expect(await service.acknowledgedIDs == ["ack-1"])
    }

    @Test("Exactly-once retry exhaustion cannot hang shutdown")
    func exactlyOnceRetryExhaustionFinishesShutdown() async throws {
        let service = FakeSubscriberService(acknowledgeFailures: 10)
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(1),
            retryPolicy: RetryPolicy(maxAttempts: 1)
        )
        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: true)
        guard case .exactlyOnce(let exactlyOnce) = handler else {
            Issue.record("expected an exactly-once handler")
            return
        }

        exactlyOnce.ack()
        await leaseManager.shutdown()

        await #expect(throws: AckError.self) {
            try await exactlyOnce.waitForConfirmation()
        }
    }

    @Test("Synchronous ack admission wins an immediate nack shutdown")
    func synchronousAckAdmissionPrecedesShutdown() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024
        )
        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: false)

        handler.ack()
        await leaseManager.shutdown()

        #expect(await service.acknowledgedBatches.contains(["ack-1"]))
        #expect(
            await service.modifiedAckDeadlineBatches.contains {
                $0.ids.contains("ack-1") && $0.seconds == 0
            } == false
        )
    }

    @Test("Streaming reconnect honors maxElapsedTime", .timeLimit(.minutes(1)))
    func streamingReconnectHonorsElapsedBudget() async throws {
        let service = FakeSubscriberService(
            streamingFailure: PubSubServiceError(code: .unavailable, message: "down"))
        let subscribe = Subscribe(
            service: service,
            subscription: "projects/p/subscriptions/s",
            retryPolicy: RetryPolicy(
                initialBackoff: .zero,
                maxBackoff: .zero,
                maxElapsedTime: .milliseconds(10)
            )
        )
        let stream = MessageStream(subscribe: subscribe)

        await #expect(throws: PubSubServiceError.self) {
            _ = try await stream.next()
        }
        #expect(await service.streamingPullRequests.count >= 1)
    }

    @Test("Cancelling a consumer task waiting on next() resumes it")
    func cancelledNextResumes() async throws {
        let service = FakeSubscriberService()
        let subscribe = Subscribe(
            service: service,
            subscription: "projects/p/subscriptions/s",
            retryPolicy: RetryPolicy(retryableCodes: [], maxAttempts: 1)
        )
        let stream = MessageStream(subscribe: subscribe)

        let consumer = Task {
            try await stream.next()
        }
        try await Task.sleep(for: .milliseconds(100))
        consumer.cancel()

        let resumed = await withThrowingTaskGroup(of: Bool.self) { group in
            group.addTask {
                _ = await consumer.result
                return true
            }
            group.addTask {
                try await Task.sleep(for: .seconds(5))
                return false
            }
            let first = try? await group.next()
            group.cancelAll()
            return first ?? false
        }
        #expect(resumed == true)

        await stream.shutdownToken().shutdown()
    }

    @Test("Iterating an unbound MessageStream delivers messages")
    func iteratingTemporaryStream() async throws {
        let service = FakeSubscriberService(
            streamingResponses: [
                makeStreamingPullResponse([makeReceivedMessage("ack-1", body: "hello")])
            ]
        )
        let subscribe = Subscribe(
            service: service,
            subscription: "projects/p/subscriptions/s",
            retryPolicy: RetryPolicy(retryableCodes: [], maxAttempts: 1)
        )

        // The stream is intentionally not bound to a variable: the iterator must
        // keep it alive, or deinit shuts the subscription down mid-loop.
        var received: String?
        for try await (message, handler) in MessageStream(subscribe: subscribe) {
            received = String(decoding: message.data, as: UTF8.self)
            handler.ack()
            break
        }
        #expect(received == "hello")
    }

    @Test(
        "Shutdown processes acknowledgements released while it waits",
        .timeLimit(.minutes(1))
    )
    func shutdownProcessesLateHandlerCommands() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60 * 60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(5),
            shutdownGracePeriod: .seconds(10)
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: false)

        // Waiting for the consumer to finish is the whole point of
        // .waitForProcessing, so the command ingress has to stay open for it.
        let shutdownTask = Task { await leaseManager.shutdown() }
        try await Task.sleep(for: .milliseconds(50))
        handler.ack()
        await shutdownTask.value

        #expect(await service.acknowledgedBatches.contains(["ack-1"]))
        #expect(await service.modifiedAckDeadlineBatches.contains { $0.seconds == 0 } == false)
    }

    @Test(
        "Shutdown is bounded by the grace period when a handler is never released",
        .timeLimit(.minutes(1))
    )
    func shutdownGracePeriodBoundsTheWait() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60 * 60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(5),
            shutdownGracePeriod: .milliseconds(200)
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: false)

        let start = ContinuousClock().now
        await leaseManager.shutdown()
        let elapsed = ContinuousClock().now - start

        // Without the grace period this waits out maxLease, an hour by default.
        #expect(elapsed < .seconds(5))
        let nack = try #require(
            await service.modifiedAckDeadlineBatches.first {
                $0.ids == ["ack-1"] && $0.seconds == 0
            })
        let nackTimeout = try #require(nack.timeout)
        #expect(nackTimeout > .milliseconds(100))
        withExtendedLifetime(handler) {}
    }

    @Test(
        "An ack ID that exhausts its retry budget does not fail the rest of its chunk",
        .timeLimit(.minutes(1))
    )
    func expiredAckIDDoesNotPoisonItsChunk() async throws {
        let service = PoisonedAckSubscriberService(poisonedID: "expired")
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(5),
            shutdownGracePeriod: .seconds(10),
            retryPolicy: RetryPolicy(
                initialBackoff: .zero,
                maxBackoff: .zero,
                maxAttempts: 10_000,
                maxElapsedTime: .milliseconds(500)
            )
        )
        await leaseManager.start()

        let expiredHandler = await leaseManager.register(
            makeReceivedMessage("expired", body: "a"), exactlyOnce: true)
        guard case .exactlyOnce(let expired) = expiredHandler else {
            Issue.record("expected an exactly-once handler")
            return
        }
        expired.ack()

        // Acked late enough that "expired" is deep into its own budget, but early
        // enough to keep sharing a chunk with it until that budget runs out.
        try await Task.sleep(for: .milliseconds(250))
        let freshHandler = await leaseManager.register(
            makeReceivedMessage("fresh", body: "b"), exactlyOnce: true)
        guard case .exactlyOnce(let fresh) = freshHandler else {
            Issue.record("expected an exactly-once handler")
            return
        }
        fresh.ack()

        do {
            try await expired.waitForConfirmation()
            Issue.record("expected the exhausted retry budget to fail this ack")
        } catch AckError.service(let error) {
            // Whether the budget runs out in the pre-RPC partition (deadlineExceeded)
            // or in the post-failure retry decision (the service's own error) is a
            // race on where the 500ms boundary lands; both are the same exhaustion.
            #expect([.deadlineExceeded, .unavailable].contains(error.code))
        } catch {
            Issue.record("expected a service error, got \(error)")
        }
        await #expect(throws: Never.self) {
            try await fresh.waitForConfirmation()
        }
        #expect(await service.acknowledgedBatches.contains(["fresh"]))

        // The chunk they shared got a deadline drawn from the longest budget in it
        // (~500ms, "fresh"), not the shortest (<250ms, "expired").
        let sharedChunk = try #require(
            await service.acknowledgeAttempts.first {
                $0.ids.sorted() == ["expired", "fresh"]
            })
        let sharedTimeout = try #require(sharedChunk.timeout)
        #expect(sharedTimeout > .milliseconds(350))

        await leaseManager.shutdown()
    }

    @Test(
        "A rejection arriving on a cancelled shutdown flush keeps its per-ID outcome",
        .timeLimit(.minutes(1))
    )
    func permanentFailureDuringShutdownIsNotTreatedAsCancellation() async throws {
        let service = CancelledThenRejectingSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(1),
            shutdownGracePeriod: .seconds(10)
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: true)
        guard case .exactlyOnce(let exactlyOnce) = handler else {
            Issue.record("expected an exactly-once handler")
            return
        }
        exactlyOnce.ack()
        try await service.firstAttemptStarted.value()

        await leaseManager.shutdown()

        do {
            try await exactlyOnce.waitForConfirmation()
            Issue.record("expected the permanent per-ID failure to surface")
        } catch AckError.service(let error) {
            #expect(error.code == .failedPrecondition)
            #expect(error.errorInfo.first?.reason == "EXACTLY_ONCE_ACK_FAILURE")
        } catch {
            Issue.record("expected a structured service error, got \(error)")
        }
        // A real rejection is a completed attempt, not a cancelled one, so it is
        // classified rather than replayed by the shutdown drain.
        #expect(await service.acknowledgeAttempts == 1)
    }

    @Test(
        "A backlog that refills during an immediate flush is flushed again, not deferred",
        .timeLimit(.minutes(1))
    )
    func immediateFlushesCoalesceInsteadOfDropping() async throws {
        let service = GatedAckSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 5_000,
            maxOutstandingBytes: 50 * 1_024 * 1_024,
            // Long enough that only the needs-flush fast path can drain the backlog:
            // if a refill is dropped rather than coalesced, nothing else picks it up.
            flushInterval: .seconds(60),
            shutdownGracePeriod: .seconds(10)
        )
        await leaseManager.start()

        var handlers: [Handler] = []
        for index in 0..<LeaseManager.maxIDsPerRPC {
            handlers.append(
                await leaseManager.register(
                    makeReceivedMessage("first-\(index)", body: "m"), exactlyOnce: false))
        }
        for handler in handlers {
            handler.ack()
        }

        try await service.firstAttemptStarted.value()

        // The first flush is parked inside its RPC; this batch crosses the
        // threshold again while it is still in flight.
        for index in 0..<LeaseManager.maxIDsPerRPC {
            let handler = await leaseManager.register(
                makeReceivedMessage("second-\(index)", body: "m"), exactlyOnce: false)
            handlers.append(handler)
            handler.ack()
        }
        await service.releaseGate()

        var acknowledged: [[String]] = []
        for _ in 0..<200 {
            acknowledged = await service.acknowledgedBatches
            if acknowledged.count >= 2 {
                break
            }

            try await Task.sleep(for: .milliseconds(10))
        }

        #expect(acknowledged.count == 2)
        #expect(acknowledged.flatMap { $0 }.count == 2 * LeaseManager.maxIDsPerRPC)
        withExtendedLifetime(handlers) {}

        await leaseManager.shutdown()
    }

    @Test("Handlers expose deliveryAttempt only when the service reports it")
    func handlersExposeDeliveryAttempt() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024
        )

        let reported = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "a", deliveryAttempt: 3), exactlyOnce: false)
        let unreported = await leaseManager.register(
            makeReceivedMessage("ack-2", body: "b"), exactlyOnce: false)
        let exactlyOnce = await leaseManager.register(
            makeReceivedMessage("ack-3", body: "c", deliveryAttempt: 7), exactlyOnce: true)

        #expect(reported.deliveryAttempt == 3)
        // Zero means "no dead-letter policy, not reported", not zero attempts.
        #expect(unreported.deliveryAttempt == nil)
        #expect(exactlyOnce.deliveryAttempt == 7)

        reported.ack()
        unreported.ack()
        exactlyOnce.ack()
        await leaseManager.shutdown()
    }

    @Test(
        "A lease covered by its last extension is not re-extended every sweep",
        .timeLimit(.minutes(1))
    )
    func leaseExtensionTracksLastExtension() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(600),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            extendInterval: .milliseconds(10)
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: false)

        // ~20 sweeps in this window; a 20s extension covers all of them, so the
        // deadline should be pushed out once rather than once per sweep.
        try await Task.sleep(for: .milliseconds(200))
        let extensions = await service.modifiedAckDeadlineBatches.filter { $0.seconds == 20 }
        #expect(extensions.count == 1)
        #expect(extensions.first?.ids == ["ack-1"])

        withExtendedLifetime(handler) {}
        await leaseManager.shutdown()
    }

    @Test("A slow lease extension does not suspend later sweeps")
    func leaseExtensionSweepsDoNotHeadOfLineBlock() async throws {
        let service = GatedLeaseExtensionSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(1),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            extendInterval: .milliseconds(20),
            extendStart: .zero
        )
        await leaseManager.start()

        let first = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "first"), exactlyOnce: false)
        try await service.firstExtensionStarted.value()
        let second = await leaseManager.register(
            makeReceivedMessage("ack-2", body: "second"), exactlyOnce: false)
        try await service.secondExtensionStarted.value()

        await service.releaseFirstExtension()
        withExtendedLifetime((first, second)) {}
        await leaseManager.shutdown()
    }

    @Test("Ack chunks are sent concurrently rather than one after another")
    func ackChunksAreSentConcurrently() async throws {
        let service = ConcurrencyTrackingSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .nackImmediately,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 5_000,
            maxOutstandingBytes: 50 * 1_024 * 1_024,
            shutdownGracePeriod: .seconds(10)
        )

        var handlers: [Handler] = []
        for index in 0..<(2 * LeaseManager.maxIDsPerRPC) {
            handlers.append(
                await leaseManager.register(
                    makeReceivedMessage("ack-\(index)", body: "m"), exactlyOnce: false))
        }
        for handler in handlers {
            handler.ack()
        }

        await leaseManager.shutdown()

        #expect(await service.peakInFlight == 2)
        #expect(
            await service.acknowledgedBatches.flatMap { $0 }.count == 2 * LeaseManager.maxIDsPerRPC)
        withExtendedLifetime(handlers) {}
    }

    @Test("Exactly-once acknowledgements default to upstream's 600 second budget")
    func exactlyOnceAckDefaultBudget() async throws {
        let service = FakeSubscriberService()
        let leaseManager = LeaseManager(
            subscription: "projects/p/subscriptions/s",
            service: service,
            shutdownBehavior: .waitForProcessing,
            maxLease: .seconds(60),
            maxLeaseExtension: .seconds(20),
            maxOutstandingMessages: 10,
            maxOutstandingBytes: 1024,
            flushInterval: .milliseconds(1)
        )
        await leaseManager.start()

        let handler = await leaseManager.register(
            makeReceivedMessage("ack-1", body: "hello"), exactlyOnce: true)
        guard case .exactlyOnce(let exactlyOnce) = handler else {
            Issue.record("expected an exactly-once handler")
            return
        }
        exactlyOnce.ack()
        try await service.acknowledgementAttempted.value()

        // The first RPC's deadline is the remaining elapsed-time budget, so it
        // reveals the configured default without waiting one out.
        let timeout = try #require(await service.acknowledgeTimeouts.first)
        #expect(timeout > .seconds(599))
        #expect(timeout <= .seconds(600))

        await leaseManager.shutdown()
        try await exactlyOnce.waitForConfirmation()
    }

    @Test("Subscribing after shutdown fails instead of finishing silently")
    func subscribeAfterShutdownFails() async throws {
        let service = FakeSubscriberService()
        let subscriber = Subscriber(service: service, configuration: ClientConfiguration())
        await subscriber.shutdown()

        let stream = subscriber.subscribe("projects/p/subscriptions/s").build()
        await #expect(throws: PubSubServiceError.self) {
            _ = try await stream.next()
        }
    }
}
