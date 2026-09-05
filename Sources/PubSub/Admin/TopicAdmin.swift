import Auth
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix

public struct TopicAdmin: Sendable {
    private let connection: ServiceConnection
    private let executor:
        AdminExecutor<Google_Pubsub_V1_Publisher.Client<HTTP2ClientTransport.Posix>>

    init(connection: ServiceConnection, retryPolicy: RetryPolicy) {
        self.connection = connection
        self.executor = AdminExecutor(connection: connection, retryPolicy: retryPolicy) {
            try await $0.publisherClient()
        }
    }

    public static func builder() -> TopicAdminBuilder {
        TopicAdminBuilder()
    }

    public func createTopic(_ topic: Topic) async throws -> Topic {
        try await executor.execute(routing: ["name": topic.name]) { client, metadata, options in
            try await client.createTopic(topic, metadata: metadata, options: options)
        }
    }

    public func updateTopic(_ request: Google_Pubsub_V1_UpdateTopicRequest) async throws -> Topic {
        try await executor.execute(routing: ["topic.name": request.topic.name], idempotent: false) {
            client, metadata, options in
            try await client.updateTopic(request, metadata: metadata, options: options)
        }
    }

    public func getTopic(name: String) async throws -> Topic {
        var request = Google_Pubsub_V1_GetTopicRequest()
        request.topic = name
        let getRequest = request
        return try await executor.execute(routing: ["topic": name]) { client, metadata, options in
            try await client.getTopic(getRequest, metadata: metadata, options: options)
        }
    }

    public func listTopics(_ request: Google_Pubsub_V1_ListTopicsRequest) async throws
        -> Google_Pubsub_V1_ListTopicsResponse
    {
        try await executor.execute(routing: ["project": request.project]) {
            client, metadata, options in
            try await client.listTopics(request, metadata: metadata, options: options)
        }
    }

    public func listTopicSubscriptions(_ request: Google_Pubsub_V1_ListTopicSubscriptionsRequest)
        async throws -> Google_Pubsub_V1_ListTopicSubscriptionsResponse
    {
        try await executor.execute(routing: ["topic": request.topic]) { client, metadata, options in
            try await client.listTopicSubscriptions(request, metadata: metadata, options: options)
        }
    }

    public func listTopicSnapshots(_ request: Google_Pubsub_V1_ListTopicSnapshotsRequest)
        async throws
        -> Google_Pubsub_V1_ListTopicSnapshotsResponse
    {
        try await executor.execute(routing: ["topic": request.topic]) { client, metadata, options in
            try await client.listTopicSnapshots(request, metadata: metadata, options: options)
        }
    }

    public func deleteTopic(name: String) async throws {
        var request = Google_Pubsub_V1_DeleteTopicRequest()
        request.topic = name
        let deleteRequest = request
        try await executor.execute(routing: ["topic": name]) { client, metadata, options in
            let _: Empty = try await client.deleteTopic(
                deleteRequest, metadata: metadata, options: options)
        }
    }

    public func detachSubscription(_ request: Google_Pubsub_V1_DetachSubscriptionRequest)
        async throws
        -> Google_Pubsub_V1_DetachSubscriptionResponse
    {
        try await executor.execute(
            routing: ["subscription": request.subscription], idempotent: false
        ) {
            client, metadata, options in
            try await client.detachSubscription(request, metadata: metadata, options: options)
        }
    }

    public func shutdown() async {
        await connection.shutdown()
    }

}

public struct TopicAdminBuilder: Sendable, ConfigurableClientBuilder {
    public var configuration: ClientConfiguration

    public init(configuration: ClientConfiguration = ClientConfiguration()) {
        self.configuration = configuration
    }

    public func build() async throws -> TopicAdmin {
        TopicAdmin(
            connection: try ServiceConnection(configuration: configuration),
            retryPolicy: configuration.retryPolicy
        )
    }
}
