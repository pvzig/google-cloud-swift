import Auth
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix

public struct SchemaService: Sendable {
    private let connection: ServiceConnection
    private let executor:
        AdminExecutor<Google_Pubsub_V1_SchemaService.Client<HTTP2ClientTransport.Posix>>

    init(connection: ServiceConnection, retryPolicy: RetryPolicy) {
        self.connection = connection
        self.executor = AdminExecutor(connection: connection, retryPolicy: retryPolicy) {
            try await $0.schemaClient()
        }
    }

    public static func builder() -> SchemaServiceBuilder {
        SchemaServiceBuilder()
    }

    public func createSchema(_ request: Google_Pubsub_V1_CreateSchemaRequest) async throws -> Schema
    {
        try await executor.execute(routing: ["parent": request.parent], idempotent: false) {
            client, metadata, options in
            try await client.createSchema(request, metadata: metadata, options: options)
        }
    }

    public func getSchema(_ request: Google_Pubsub_V1_GetSchemaRequest) async throws -> Schema {
        try await executor.execute(routing: ["name": request.name]) { client, metadata, options in
            try await client.getSchema(request, metadata: metadata, options: options)
        }
    }

    public func listSchemas(_ request: Google_Pubsub_V1_ListSchemasRequest) async throws
        -> Google_Pubsub_V1_ListSchemasResponse
    {
        try await executor.execute(routing: ["parent": request.parent]) {
            client, metadata, options in
            try await client.listSchemas(request, metadata: metadata, options: options)
        }
    }

    public func deleteSchema(_ request: Google_Pubsub_V1_DeleteSchemaRequest) async throws {
        try await executor.execute(routing: ["name": request.name]) { client, metadata, options in
            let _: Empty = try await client.deleteSchema(
                request, metadata: metadata, options: options)
        }
    }

    public func validateSchema(_ request: Google_Pubsub_V1_ValidateSchemaRequest) async throws
        -> Google_Pubsub_V1_ValidateSchemaResponse
    {
        try await executor.execute(routing: ["parent": request.parent], idempotent: false) {
            client, metadata, options in
            try await client.validateSchema(request, metadata: metadata, options: options)
        }
    }

    public func validateMessage(_ request: Google_Pubsub_V1_ValidateMessageRequest) async throws
        -> Google_Pubsub_V1_ValidateMessageResponse
    {
        try await executor.execute(routing: ["parent": request.parent], idempotent: false) {
            client, metadata, options in
            try await client.validateMessage(request, metadata: metadata, options: options)
        }
    }

    public func commitSchema(_ request: Google_Pubsub_V1_CommitSchemaRequest) async throws -> Schema
    {
        try await executor.execute(routing: ["name": request.name], idempotent: false) {
            client, metadata, options in
            try await client.commitSchema(request, metadata: metadata, options: options)
        }
    }

    public func rollbackSchema(_ request: Google_Pubsub_V1_RollbackSchemaRequest) async throws
        -> Schema
    {
        try await executor.execute(routing: ["name": request.name], idempotent: false) {
            client, metadata, options in
            try await client.rollbackSchema(request, metadata: metadata, options: options)
        }
    }

    public func shutdown() async {
        await connection.shutdown()
    }

}

public struct SchemaServiceBuilder: Sendable, ConfigurableClientBuilder {
    public var configuration: ClientConfiguration

    public init(configuration: ClientConfiguration = ClientConfiguration()) {
        self.configuration = configuration
    }

    public func build() async throws -> SchemaService {
        SchemaService(
            connection: try ServiceConnection(configuration: configuration),
            retryPolicy: configuration.retryPolicy
        )
    }
}
