import Foundation

public struct UserAccountCredentials: Sendable, Decodable, Equatable {
    public let type: String?
    public let clientID: String
    public let clientSecret: String
    public let refreshToken: String
    public let quotaProjectID: String?
    public let tokenURI: String?

    public init(
        type: String? = "authorized_user",
        clientID: String,
        clientSecret: String,
        refreshToken: String,
        quotaProjectID: String? = nil,
        tokenURI: String? = nil
    ) {
        self.type = type
        self.clientID = clientID
        self.clientSecret = clientSecret
        self.refreshToken = refreshToken
        self.quotaProjectID = quotaProjectID
        self.tokenURI = tokenURI
    }

    enum CodingKeys: String, CodingKey {
        case type
        case clientID = "client_id"
        case clientSecret = "client_secret"
        case refreshToken = "refresh_token"
        case quotaProjectID = "quota_project_id"
        case tokenURI = "token_uri"
    }
}

public struct UserAccountProvider: Provider, TokenProvider {
    private let credentials: UserAccountCredentials
    private let tokenURL: URL

    public init(credentials: UserAccountCredentials) throws {
        self.credentials = credentials
        guard let url = URL(string: credentials.tokenURI ?? AuthConstants.oauth2TokenServerURL)
        else {
            throw CredentialsError.parsing("invalid user account token URI")
        }
        self.tokenURL = url
    }

    public func createSession(scopes: [Scope]) async throws -> Session {
        try await token(scopes: scopes).session
    }

    public var quotaProjectID: String? {
        get async {
            ProcessInfo.processInfo.environment[AuthConstants.googleCloudQuotaProjectVariable]
                ?? credentials.quotaProjectID
        }
    }

    public func token(scopes: [Scope]) async throws -> Token {
        // A refresh token can only reproduce its existing grant. RFC 6749 does not
        // allow a client library to widen that grant by attaching requested API
        // scopes to the refresh_token exchange.
        let response: OAuthTokenResponse = try await AuthHTTP.postForm(
            url: tokenURL,
            parameters: refreshParameters
        )
        return response.token
    }

    public func buildCredentials() -> Credentials {
        Credentials(
            provider: AccessTokenCredentialsProvider(
                tokenProvider: self,
                quotaProjectID: ProcessInfo.processInfo.environment[
                    AuthConstants.googleCloudQuotaProjectVariable
                ] ?? credentials.quotaProjectID
            )
        )
    }

    public func shutdown() async throws {}

    var refreshParameters: [String: String] {
        [
            "client_id": credentials.clientID,
            "client_secret": credentials.clientSecret,
            "grant_type": "refresh_token",
            "refresh_token": credentials.refreshToken,
        ]
    }
}

struct OAuthTokenResponse: Decodable {
    let accessToken: String
    let tokenType: String
    let expiresIn: Int?
    let metadata: [String: String]?

    enum CodingKeys: String, CodingKey {
        case accessToken = "access_token"
        case tokenType = "token_type"
        case expiresIn = "expires_in"
        case metadata
    }

    var token: Token {
        Token(
            token: accessToken,
            tokenType: tokenType,
            expiresAt: expiresIn.map { Date().addingTimeInterval(TimeInterval($0)) },
            metadata: metadata
        )
    }
}
