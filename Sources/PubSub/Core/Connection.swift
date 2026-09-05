import Auth
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2Posix

actor ServiceConnection {
    static let maxRequestMessageBytes = 10_000_000
    static let maxResponseMessageBytes = 20_000_000
    // grpc-swift-nio-transport 2.9 derives the client-side inbound deframer from
    // maxRequestMessageBytes and does not read maxResponseMessageBytes. Feed the
    // larger inbound limit through both fields until the transport fixes that
    // mismatch; publisher admission independently enforces the 10 MB outbound cap.
    static let transportPayloadLimitBytes = maxResponseMessageBytes

    private let configuration: ClientConfiguration
    private let authorization: Authorization?
    private let ownsAuthorization: Bool
    private let grpcClients: [GRPCClient<HTTP2ClientTransport.Posix>]
    private var runTasks: [Task<Void, Error>] = []
    private var nextClientIndex = 0
    private var isShutdown = false

    init(
        configuration: ClientConfiguration,
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) throws {
        self.configuration = configuration

        let target = try ConnectionTarget(configuration: configuration, environment: environment)
        let clientCount = configuration.grpcSubchannelCount
        switch target.credentialsMode {
        case .automatic:
            let authorization = Authorization(
                scopes: [
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/pubsub",
                ]
            )
            self.authorization = authorization
            self.ownsAuthorization = true
            self.grpcClients = try Self.makeClients(
                count: clientCount,
                target: target,
                plaintext: target.plaintext,
                authorization: authorization
            )
        case .anonymous:
            self.authorization = nil
            self.ownsAuthorization = false
            self.grpcClients = try Self.makeClients(
                count: clientCount,
                target: target,
                plaintext: target.plaintext,
                authorization: nil
            )
        case .custom(let authorization):
            self.authorization = authorization
            self.ownsAuthorization = false
            self.grpcClients = try Self.makeClients(
                count: clientCount,
                target: target,
                plaintext: target.plaintext,
                authorization: authorization
            )
        }
    }

    func start() async throws {
        guard !isShutdown else {
            throw PubSubServiceError(message: "client connection is shut down")
        }

        if runTasks.isEmpty {
            runTasks = grpcClients.map { grpcClient in
                Task {
                    try await grpcClient.runConnections()
                }
            }
        }
    }

    func shutdown() async {
        guard !isShutdown else {
            return
        }

        isShutdown = true
        for grpcClient in grpcClients {
            grpcClient.beginGracefulShutdown()
        }

        let tasks = runTasks
        runTasks.removeAll(keepingCapacity: false)
        for task in tasks {
            _ = await task.result
        }

        if ownsAuthorization {
            try? await authorization?.shutdown()
        }
    }

    var grpcClientCount: Int {
        grpcClients.count
    }

    /// Builds the `x-goog-request-params` routing header. Pub/Sub's service
    /// config routes on the resource name, and regional endpoints plus
    /// server-side StreamingPull affinity depend on it; without the header the
    /// service can only fall back to the global default.
    nonisolated static func routingMetadata(_ parameters: KeyValuePairs<String, String>)
        -> Metadata
    {
        var metadata = Metadata()
        let value = parameters.map {
            "\($0.key)=\(percentEncodeRoutingValue($0.value))"
        }.joined(separator: "&")
        metadata.addString(value, forKey: "x-goog-request-params")
        return metadata
    }

    /// Encodes a routing value as an RFC 3986 query component. Encoding only
    /// unreserved bytes keeps separators such as `+`, `%`, `&`, and `=` from
    /// changing the resource name when Google's router parses the header.
    private nonisolated static func percentEncodeRoutingValue(_ value: String) -> String {
        let hexadecimal = Array("0123456789ABCDEF".utf8)
        var encoded: [UInt8] = []
        encoded.reserveCapacity(value.utf8.count)

        for byte in value.utf8 {
            let isUnreserved =
                (byte >= 65 && byte <= 90)
                || (byte >= 97 && byte <= 122)
                || (byte >= 48 && byte <= 57)
                || byte == 45
                || byte == 46
                || byte == 95
                || byte == 126
            if isUnreserved {
                encoded.append(byte)
            } else {
                encoded.append(37)
                encoded.append(hexadecimal[Int(byte >> 4)])
                encoded.append(hexadecimal[Int(byte & 0x0F)])
            }
        }

        return String(decoding: encoded, as: UTF8.self)
    }

    // Reads only immutable configuration, so RPC sites don't pay an actor hop
    // per call just to build a constant.
    nonisolated func callOptions(timeout: Duration? = nil) -> CallOptions {
        var options = CallOptions.defaults
        options.timeout =
            timeout.map { min($0, configuration.callTimeout) } ?? configuration.callTimeout
        options.maxRequestMessageBytes = Self.transportPayloadLimitBytes
        options.maxResponseMessageBytes = Self.maxResponseMessageBytes
        return options
    }

    /// Long-lived streaming calls need Pub/Sub's payload limits without the
    /// unary call deadline, which would close a healthy stream periodically.
    nonisolated func streamingCallOptions() -> CallOptions {
        var options = CallOptions.defaults
        options.maxRequestMessageBytes = Self.transportPayloadLimitBytes
        options.maxResponseMessageBytes = Self.maxResponseMessageBytes
        return options
    }

    func publisherClient() async throws
        -> Google_Pubsub_V1_Publisher.Client<HTTP2ClientTransport.Posix>
    {
        try await start()
        return Google_Pubsub_V1_Publisher.Client(wrapping: nextGRPCClient())
    }

    func subscriberClient() async throws
        -> Google_Pubsub_V1_Subscriber.Client<HTTP2ClientTransport.Posix>
    {
        try await start()
        return Google_Pubsub_V1_Subscriber.Client(wrapping: nextGRPCClient())
    }

    func schemaClient() async throws
        -> Google_Pubsub_V1_SchemaService.Client<HTTP2ClientTransport.Posix>
    {
        try await start()
        return Google_Pubsub_V1_SchemaService.Client(wrapping: nextGRPCClient())
    }

    private func nextGRPCClient() -> GRPCClient<HTTP2ClientTransport.Posix> {
        let client = grpcClients[nextClientIndex]
        nextClientIndex = (nextClientIndex + 1) % grpcClients.count
        return client
    }

    private static func makeClients(
        count: Int,
        target: ConnectionTarget,
        plaintext: Bool,
        authorization: Authorization?
    ) throws -> [GRPCClient<HTTP2ClientTransport.Posix>] {
        try (0..<count).map { _ in
            let payloadLimit = PayloadLimitInterceptor(maximumBytes: Self.maxRequestMessageBytes)
            if let authorization {
                return GRPCClient(
                    transport: try target.transport(plaintext: plaintext),
                    interceptors: [
                        payloadLimit,
                        AuthorizationClientInterceptor(authorization: authorization),
                    ]
                )
            }

            return GRPCClient(
                transport: try target.transport(plaintext: plaintext),
                interceptors: [payloadLimit]
            )
        }
    }
}

private struct ConnectionTarget {
    let host: String
    let port: Int
    let plaintext: Bool
    let credentialsMode: CredentialsMode

    init(configuration: ClientConfiguration, environment: [String: String]) throws {
        if let emulatorHost = configuration.emulatorHost.nonEmpty
            ?? environment["PUBSUB_EMULATOR_HOST"].nonEmpty
        {
            let parsed = try Self.parseAuthority(
                emulatorHost,
                defaultScheme: "http",
                requirePort: true,
                label: "PUBSUB_EMULATOR_HOST"
            )
            self.host = parsed.host
            self.port = parsed.port
            // The emulator is always plaintext, whatever scheme was written.
            self.plaintext = true
            self.credentialsMode = .anonymous
            return
        }

        if let endpoint = configuration.endpoint {
            let parsed = try Self.parseAuthority(
                endpoint,
                defaultScheme: "https",
                requirePort: false,
                label: "endpoint"
            )
            self.host = parsed.host
            self.port = parsed.port
            self.plaintext = parsed.plaintext
            self.credentialsMode = configuration.credentialsMode
            return
        }

        self.host = "pubsub.googleapis.com"
        self.port = 443
        self.plaintext = false
        self.credentialsMode = configuration.credentialsMode
    }

    func transport(plaintext: Bool) throws -> HTTP2ClientTransport.Posix {
        try .http2NIOPosix(
            target: .dns(host: host, port: port),
            transportSecurity: plaintext ? .plaintext : .tls
        )
    }

    /// Single validated parser for both `PUBSUB_EMULATOR_HOST` and custom
    /// endpoints, so neither can smuggle userinfo, a path, a query, or an
    /// out-of-range TCP port into the transport.
    private static func parseAuthority(
        _ value: String,
        defaultScheme: String,
        requirePort: Bool,
        label: String
    ) throws -> (host: String, port: Int, plaintext: Bool) {
        let valueWithScheme = value.contains("://") ? value : "\(defaultScheme)://\(value)"
        guard
            let components = URLComponents(string: valueWithScheme),
            components.url != nil,
            let scheme = components.scheme?.lowercased(),
            scheme == "http" || scheme == "https",
            components.user == nil,
            components.password == nil,
            let host = components.host,
            !host.isEmpty,
            components.path.isEmpty || components.path == "/",
            components.query == nil,
            components.fragment == nil,
            !requirePort || components.port != nil
        else {
            throw PubSubServiceError(message: "invalid \(label): \(value)")
        }

        let port = components.port ?? (scheme == "http" ? 80 : 443)
        guard (1...65_535).contains(port) else {
            throw PubSubServiceError(message: "invalid \(label): \(value)")
        }

        return (host, port, scheme == "http")
    }
}

extension Optional where Wrapped == String {
    fileprivate var nonEmpty: String? {
        flatMap { $0.isEmpty ? nil : $0 }
    }
}
