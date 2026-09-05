import Foundation

public actor ImpersonatedProvider: Provider {
    private let sourceProvider: any Provider
    private let impersonationURL: URL
    private let delegates: [String]
    private let lifetime: Int

    public init(
        sourceProvider: any Provider,
        impersonationURL: URL,
        delegates: [String] = [],
        lifetime: Int = 3600
    ) {
        self.sourceProvider = sourceProvider
        self.impersonationURL = impersonationURL
        self.delegates = delegates
        self.lifetime = lifetime
    }

    init(credentialsData: Data) throws {
        let file = try JSONDecoder().decode(ImpersonatedCredentialsFile.self, from: credentialsData)
        guard let url = URL(string: file.serviceAccountImpersonationURL) else {
            throw CredentialsError.parsing("invalid service_account_impersonation_url")
        }
        let sourceData = try JSONEncoder().encode(file.sourceCredentials)
        self.sourceProvider = try DefaultProvider.provider(from: sourceData)
        self.impersonationURL = url
        self.delegates = file.delegates ?? []
        self.lifetime = file.lifetimeSeconds
    }

    public func createSession(scopes: [Scope]) async throws -> Session {
        let requestedScopes =
            scopes.isEmpty ? [Scope(rawValue: AuthConstants.defaultScope)] : scopes
        // Upstream passes no scopes to the source credential: service-account
        // sources fall back to their cloud-platform default and user-account
        // sources use their refresh token's full grant. Requesting a narrower
        // scope here (e.g. auth/iam) can fail refresh tokens not granted it.
        let sourceSession = try await sourceProvider.createSession(scopes: [])
        let request = GenerateAccessTokenRequest(
            delegates: delegates,
            scope: requestedScopes.map(\.rawValue),
            lifetime: "\(lifetime)s"
        )
        let response: GenerateAccessTokenResponse = try await AuthHTTP.postJSON(
            url: impersonationURL,
            body: request,
            bearerToken: sourceSession.accessToken
        )
        return Session(
            accessToken: response.accessToken,
            expiration: .absolute(response.expireTime)
        )
    }

    public func shutdown() async throws {
        // The process-wide default provider is shared by every automatic client;
        // an impersonated credential does not own it and must not clear it.
        if Self.ownsSourceProvider(sourceProvider) == false {
            return
        }

        try await sourceProvider.shutdown()
    }

    nonisolated static func ownsSourceProvider(_ provider: any Provider) -> Bool {
        provider is DefaultProvider == false
    }
}

private struct ImpersonatedCredentialsFile: Decodable {
    let serviceAccountImpersonationURL: String
    let sourceCredentials: JSONValue
    let delegates: [String]?
    private let lifetime: String?

    var lifetimeSeconds: Int {
        guard let lifetime else {
            return 3600
        }
        let seconds = lifetime.hasSuffix("s") ? lifetime.dropLast() : lifetime[...]
        return Int(seconds) ?? 3600
    }

    enum CodingKeys: String, CodingKey {
        case serviceAccountImpersonationURL = "service_account_impersonation_url"
        case sourceCredentials = "source_credentials"
        case delegates
        case lifetime
    }
}

private struct GenerateAccessTokenRequest: Encodable {
    let delegates: [String]
    let scope: [String]
    let lifetime: String
}

private struct GenerateAccessTokenResponse: Decodable {
    let accessToken: String
    let expireTime: Date

    enum CodingKeys: String, CodingKey {
        case accessToken
        case expireTime
    }

    init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        accessToken = try container.decode(String.self, forKey: .accessToken)
        let rawExpireTime = try container.decode(String.self, forKey: .expireTime)
        // Protobuf Timestamp JSON includes fractional digits whenever nanos != 0,
        // which the default ISO8601DateFormatter options reject.
        let formatter = ISO8601DateFormatter()
        var date = formatter.date(from: rawExpireTime)
        if date == nil {
            formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
            date = formatter.date(from: rawExpireTime)
        }
        guard let date else {
            throw DecodingError.dataCorruptedError(
                forKey: .expireTime,
                in: container,
                debugDescription: "invalid RFC3339 expireTime"
            )
        }
        expireTime = date
    }
}

private enum JSONValue: Codable, Sendable {
    case string(String)
    case number(Double)
    case bool(Bool)
    case object([String: JSONValue])
    case array([JSONValue])
    case null

    init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(Bool.self) {
            self = .bool(value)
        } else if let value = try? container.decode(Double.self) {
            self = .number(value)
        } else if let value = try? container.decode(String.self) {
            self = .string(value)
        } else if let value = try? container.decode([JSONValue].self) {
            self = .array(value)
        } else {
            self = .object(try container.decode([String: JSONValue].self))
        }
    }

    func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .string(let value):
            try container.encode(value)
        case .number(let value):
            try container.encode(value)
        case .bool(let value):
            try container.encode(value)
        case .object(let value):
            try container.encode(value)
        case .array(let value):
            try container.encode(value)
        case .null:
            try container.encodeNil()
        }
    }
}
