import GRPCCore

struct AdminExecutor<Client: Sendable>: Sendable {
    private let connection: ServiceConnection
    private let retryPolicy: RetryPolicy
    private let client: @Sendable (ServiceConnection) async throws -> Client

    init(
        connection: ServiceConnection,
        retryPolicy: RetryPolicy,
        client: @escaping @Sendable (ServiceConnection) async throws -> Client
    ) {
        self.connection = connection
        self.retryPolicy = retryPolicy.withDefaultBudget(
            maxAttempts: 10,
            maxElapsedTime: .seconds(60)
        )
        self.client = client
    }

    /// Applies the common admin retry, deadline, routing, and error-mapping
    /// contract. PATCH/POST-backed operations pass `idempotent: false` so only
    /// UNAVAILABLE is replayed under AIP-194.
    func execute<T: Sendable>(
        routing: KeyValuePairs<String, String>,
        idempotent: Bool = true,
        _ operation: @Sendable (Client, Metadata, CallOptions) async throws -> T
    ) async throws -> T {
        let metadata = ServiceConnection.routingMetadata(routing)
        return try await withRetry(policy: retryPolicy, idempotent: idempotent) { remainingTime in
            let client = try await client(connection)
            return try await operation(
                client,
                metadata,
                connection.callOptions(timeout: remainingTime)
            )
        }
    }
}
