import Auth
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix

public struct SubscriptionAdmin: Sendable {
    private let connection: ServiceConnection
    private let executor:
        AdminExecutor<Google_Pubsub_V1_Subscriber.Client<HTTP2ClientTransport.Posix>>

    init(connection: ServiceConnection, retryPolicy: RetryPolicy) {
        self.connection = connection
        self.executor = AdminExecutor(connection: connection, retryPolicy: retryPolicy) {
            try await $0.subscriberClient()
        }
    }

    public static func builder() -> SubscriptionAdminBuilder {
        SubscriptionAdminBuilder()
    }

    public func createSubscription(_ subscription: Subscription) async throws -> Subscription {
        try await executor.execute(routing: ["name": subscription.name]) {
            client, metadata, options in
            try await client.createSubscription(subscription, metadata: metadata, options: options)
        }
    }

    public func getSubscription(name: String) async throws -> Subscription {
        var request = Google_Pubsub_V1_GetSubscriptionRequest()
        request.subscription = name
        let getRequest = request
        return try await executor.execute(routing: ["subscription": name]) {
            client, metadata, options in
            try await client.getSubscription(getRequest, metadata: metadata, options: options)
        }
    }

    public func updateSubscription(_ request: Google_Pubsub_V1_UpdateSubscriptionRequest)
        async throws
        -> Subscription
    {
        try await executor.execute(
            routing: ["subscription.name": request.subscription.name], idempotent: false
        ) { client, metadata, options in
            try await client.updateSubscription(request, metadata: metadata, options: options)
        }
    }

    public func listSubscriptions(_ request: Google_Pubsub_V1_ListSubscriptionsRequest) async throws
        -> Google_Pubsub_V1_ListSubscriptionsResponse
    {
        try await executor.execute(routing: ["project": request.project]) {
            client, metadata, options in
            try await client.listSubscriptions(request, metadata: metadata, options: options)
        }
    }

    public func deleteSubscription(name: String) async throws {
        var request = Google_Pubsub_V1_DeleteSubscriptionRequest()
        request.subscription = name
        let deleteRequest = request
        try await executor.execute(routing: ["subscription": name]) { client, metadata, options in
            let _: Empty = try await client.deleteSubscription(
                deleteRequest, metadata: metadata, options: options)
        }
    }

    public func modifyPushConfig(_ request: Google_Pubsub_V1_ModifyPushConfigRequest) async throws {
        try await executor.execute(
            routing: ["subscription": request.subscription], idempotent: false
        ) {
            client, metadata, options in
            let _: Empty = try await client.modifyPushConfig(
                request, metadata: metadata, options: options)
        }
    }

    public func listSnapshots(_ request: Google_Pubsub_V1_ListSnapshotsRequest) async throws
        -> Google_Pubsub_V1_ListSnapshotsResponse
    {
        try await executor.execute(routing: ["project": request.project]) {
            client, metadata, options in
            try await client.listSnapshots(request, metadata: metadata, options: options)
        }
    }

    public func createSnapshot(_ request: Google_Pubsub_V1_CreateSnapshotRequest) async throws
        -> Google_Pubsub_V1_Snapshot
    {
        try await executor.execute(routing: ["name": request.name]) { client, metadata, options in
            try await client.createSnapshot(request, metadata: metadata, options: options)
        }
    }

    public func updateSnapshot(_ request: Google_Pubsub_V1_UpdateSnapshotRequest) async throws
        -> Google_Pubsub_V1_Snapshot
    {
        try await executor.execute(
            routing: ["snapshot.name": request.snapshot.name], idempotent: false
        ) { client, metadata, options in
            try await client.updateSnapshot(request, metadata: metadata, options: options)
        }
    }

    public func getSnapshot(name: String) async throws -> Google_Pubsub_V1_Snapshot {
        var request = Google_Pubsub_V1_GetSnapshotRequest()
        request.snapshot = name
        let getRequest = request
        return try await executor.execute(routing: ["snapshot": name]) {
            client, metadata, options in
            try await client.getSnapshot(getRequest, metadata: metadata, options: options)
        }
    }

    public func deleteSnapshot(name: String) async throws {
        var request = Google_Pubsub_V1_DeleteSnapshotRequest()
        request.snapshot = name
        let deleteRequest = request
        try await executor.execute(routing: ["snapshot": name]) { client, metadata, options in
            let _: Empty = try await client.deleteSnapshot(
                deleteRequest, metadata: metadata, options: options)
        }
    }

    public func seek(_ request: Google_Pubsub_V1_SeekRequest) async throws
        -> Google_Pubsub_V1_SeekResponse
    {
        try await executor.execute(
            routing: ["subscription": request.subscription], idempotent: false
        ) {
            client, metadata, options in
            try await client.seek(request, metadata: metadata, options: options)
        }
    }

    public func shutdown() async {
        await connection.shutdown()
    }

}

public struct SubscriptionAdminBuilder: Sendable, ConfigurableClientBuilder {
    public var configuration: ClientConfiguration

    public init(configuration: ClientConfiguration = ClientConfiguration()) {
        self.configuration = configuration
    }

    public func build() async throws -> SubscriptionAdmin {
        SubscriptionAdmin(
            connection: try ServiceConnection(configuration: configuration),
            retryPolicy: configuration.retryPolicy
        )
    }
}
