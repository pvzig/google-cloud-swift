import Auth
import Foundation

protocol SubscriberRPC: Sendable {
    func streamingPull(
        subscription: String,
        streamAckDeadlineSeconds: Int32,
        maxOutstandingMessages: Int,
        maxOutstandingBytes: Int,
        clientID: String,
        onConnected: @escaping @Sendable () async -> Void,
        onResponse:
            @escaping @Sendable (Google_Pubsub_V1_StreamingPullResponse) async throws -> Void
    ) async throws
    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws
    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws
    func shutdown() async
}

extension SubscriberRPC {
    func shutdown() async {}
}

struct LiveSubscriberRPC: SubscriberRPC {
    let connection: ServiceConnection

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
        var request = Google_Pubsub_V1_StreamingPullRequest()
        request.subscription = subscription
        request.streamAckDeadlineSeconds = streamAckDeadlineSeconds
        request.maxOutstandingMessages = Int64(maxOutstandingMessages)
        request.maxOutstandingBytes = Int64(maxOutstandingBytes)
        request.clientID = clientID
        let initialRequest = request

        let client = try await connection.subscriberClient()
        try await client.streamingPull(
            metadata: ServiceConnection.routingMetadata(["subscription": subscription]),
            options: connection.streamingCallOptions(),
            requestProducer: { writer in
                try await writer.write(initialRequest)
                await onConnected()
                while !Task.isCancelled {
                    try await Task.sleep(for: .seconds(30))
                    try await writer.write(Google_Pubsub_V1_StreamingPullRequest())
                }
            },
            onResponse: { response in
                // Awaiting the consumer for each response carries grpc-swift/NIO's
                // inbound watermark backpressure all the way to lease registration.
                for try await message in response.messages {
                    try await onResponse(message)
                }
            }
        )
    }

    func acknowledge(
        subscription: String,
        ackIDs: [String],
        timeout: Duration?
    ) async throws {
        var request = Google_Pubsub_V1_AcknowledgeRequest()
        request.subscription = subscription
        request.ackIds = ackIDs
        let client = try await connection.subscriberClient()
        let _: Empty = try await client.acknowledge(
            request,
            metadata: ServiceConnection.routingMetadata(["subscription": subscription]),
            options: connection.callOptions(timeout: timeout)
        )
    }

    func modifyAckDeadline(
        subscription: String,
        ackIDs: [String],
        ackDeadlineSeconds: Int32,
        timeout: Duration?
    ) async throws {
        var request = Google_Pubsub_V1_ModifyAckDeadlineRequest()
        request.subscription = subscription
        request.ackIds = ackIDs
        request.ackDeadlineSeconds = ackDeadlineSeconds
        let client = try await connection.subscriberClient()
        let _: Empty = try await client.modifyAckDeadline(
            request,
            metadata: ServiceConnection.routingMetadata(["subscription": subscription]),
            options: connection.callOptions(timeout: timeout)
        )
    }

    func shutdown() async {
        await connection.shutdown()
    }
}

public enum ShutdownBehavior: Sendable {
    case waitForProcessing
    case nackImmediately
}

public struct Subscriber: Sendable {
    private let service: any SubscriberRPC
    private let configuration: ClientConfiguration
    private let streamRegistry: SubscriberStreamRegistry

    init(
        service: any SubscriberRPC,
        configuration: ClientConfiguration,
        streamRegistry: SubscriberStreamRegistry = SubscriberStreamRegistry()
    ) {
        self.service = service
        self.configuration = configuration
        self.streamRegistry = streamRegistry
    }

    public static func builder() -> SubscriberBuilder {
        SubscriberBuilder()
    }

    public func subscribe(_ subscription: String) -> Subscribe {
        Subscribe(
            service: service,
            subscription: subscription,
            retryPolicy: configuration.retryPolicy,
            streamRegistry: streamRegistry
        )
    }

    public func shutdown() async {
        await streamRegistry.shutdownAll()
        await service.shutdown()
    }
}

public struct SubscriberBuilder: Sendable, ConfigurableClientBuilder {
    public var configuration: ClientConfiguration

    public init(configuration: ClientConfiguration = ClientConfiguration()) {
        self.configuration = configuration
    }

    public func build() async throws -> Subscriber {
        let connection = try ServiceConnection(configuration: configuration)
        return Subscriber(
            service: LiveSubscriberRPC(connection: connection),
            configuration: configuration
        )
    }
}

public struct Subscribe: Sendable {
    let service: any SubscriberRPC
    let subscription: String
    let retryPolicy: RetryPolicy
    let clientID: String
    let streamRegistry: SubscriberStreamRegistry
    private(set) var maxLease: Duration
    private(set) var maxLeaseExtension: Duration
    private(set) var maxOutstandingMessages: Int
    private(set) var maxOutstandingBytes: Int
    private(set) var shutdownBehavior: ShutdownBehavior
    private(set) var shutdownGracePeriod: Duration

    init(
        service: any SubscriberRPC,
        subscription: String,
        retryPolicy: RetryPolicy,
        clientID: String = UUID().uuidString,
        maxLease: Duration = .seconds(60 * 60),
        maxLeaseExtension: Duration = .seconds(60),
        maxOutstandingMessages: Int = 1_000,
        maxOutstandingBytes: Int = 100 * 1_024 * 1_024,
        shutdownBehavior: ShutdownBehavior = .waitForProcessing,
        shutdownGracePeriod: Duration = .seconds(30),
        streamRegistry: SubscriberStreamRegistry = SubscriberStreamRegistry()
    ) {
        self.service = service
        self.subscription = subscription
        self.retryPolicy = retryPolicy
        self.clientID = clientID
        self.streamRegistry = streamRegistry
        self.maxLease = maxLease
        self.maxLeaseExtension = maxLeaseExtension
        self.maxOutstandingMessages = maxOutstandingMessages
        self.maxOutstandingBytes = maxOutstandingBytes
        self.shutdownBehavior = shutdownBehavior
        self.shutdownGracePeriod = shutdownGracePeriod
    }

    public func build() -> MessageStream {
        MessageStream(subscribe: self)
    }

    public func setMaxLease(_ duration: Duration) -> Self {
        var copy = self
        copy.maxLease = duration
        return copy
    }

    public func setMaxLeaseExtension(_ duration: Duration) -> Self {
        var copy = self
        copy.maxLeaseExtension = min(max(duration, .seconds(10)), .seconds(10 * 60))
        return copy
    }

    public func setMaxOutstandingMessages(_ value: Int) -> Self {
        var copy = self
        copy.maxOutstandingMessages = value
        return copy
    }

    public func setMaxOutstandingBytes(_ value: Int) -> Self {
        var copy = self
        copy.maxOutstandingBytes = value
        return copy
    }

    public func setShutdownBehavior(_ behavior: ShutdownBehavior) -> Self {
        var copy = self
        copy.shutdownBehavior = behavior
        return copy
    }

    /// Bounds each shutdown phase: `.waitForProcessing`, the pending-ack drain,
    /// and the final nack drain each receive this window. Giving later phases a
    /// fresh window ensures a consumer that uses the complete processing grace
    /// cannot starve the RPCs that settle or release its remaining leases.
    public func setShutdownGracePeriod(_ duration: Duration) -> Self {
        var copy = self
        copy.shutdownGracePeriod = max(.zero, duration)
        return copy
    }
}
