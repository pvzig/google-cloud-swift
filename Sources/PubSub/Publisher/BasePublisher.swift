import Auth
import Foundation

public struct BasePublisher: Sendable {
    private let service: any PublisherRPC
    private let configuration: ClientConfiguration
    private let publisherRegistry: PublisherRegistry

    init(
        service: any PublisherRPC,
        configuration: ClientConfiguration,
        publisherRegistry: PublisherRegistry = PublisherRegistry()
    ) {
        self.service = service
        self.configuration = configuration
        self.publisherRegistry = publisherRegistry
    }

    public static func builder() -> BasePublisherBuilder {
        BasePublisherBuilder()
    }

    public func publisher(_ topic: String) -> PublisherPartialBuilder {
        // Per-topic publishers share this BasePublisher's connection and must not
        // close it on their own shutdown(); only BasePublisher.shutdown() does.
        PublisherPartialBuilder(
            service: service,
            topic: topic,
            configuration: configuration,
            ownsService: false,
            publisherRegistry: publisherRegistry
        )
    }

    public func shutdown() async {
        await publisherRegistry.shutdownAll()
        await service.shutdown()
    }
}

public struct BasePublisherBuilder: Sendable, ConfigurableClientBuilder {
    public var configuration: ClientConfiguration

    public init(configuration: ClientConfiguration = ClientConfiguration()) {
        self.configuration = configuration
    }

    public func build() async throws -> BasePublisher {
        let connection = try ServiceConnection(configuration: configuration)
        let service = LivePublisherRPC(connection: connection)
        return BasePublisher(service: service, configuration: configuration)
    }
}
