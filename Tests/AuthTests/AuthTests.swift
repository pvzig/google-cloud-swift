import Foundation
import GRPCCore
import Testing

@testable import Auth

private actor CountingProvider: Provider {
    private(set) var calls = 0
    private let expiration: Session.Expiration
    private let quotaProject: String?

    init(
        expiration: Session.Expiration = .absolute(Date().addingTimeInterval(3_600)),
        quotaProjectID: String? = nil
    ) {
        self.expiration = expiration
        self.quotaProject = quotaProjectID
    }

    func createSession(scopes: [Scope]) async throws -> Session {
        calls += 1
        return Session(accessToken: "token-\(calls)", expiration: expiration)
    }

    var quotaProjectID: String? {
        quotaProject
    }
}

private actor CountingTokenProvider: TokenProvider {
    private(set) var calls = 0

    func token(scopes: [Scope]) async throws -> Token {
        calls += 1
        let scopeLabel = scopes.map(\.rawValue).joined(separator: ",")
        return Token(token: "\(scopeLabel)-\(calls)", expiresAt: Date().addingTimeInterval(3_600))
    }
}

/// Issues tokens that are valid but inside the refresh slack window, and
/// fails the second request with a transient error.
private actor FlakyTokenProvider: TokenProvider {
    private var calls = 0

    func token(scopes: [Scope]) async throws -> Token {
        calls += 1
        if calls == 2 {
            throw CredentialsError.httpStatus(503, "transient")
        }
        return Token(token: "token-\(calls)", expiresAt: Date().addingTimeInterval(0.5))
    }
}

private actor PermanentlyFailingTokenProvider: TokenProvider {
    private var calls = 0

    func token(scopes: [Scope]) async throws -> Token {
        calls += 1
        if calls > 1 {
            throw CredentialsError.parsing("revoked refresh token")
        }
        return Token(token: "token-1", expiresAt: Date().addingTimeInterval(0.5))
    }
}

private actor FailingSessionProvider: Provider {
    private let failure: CredentialsError
    private var calls = 0

    init(failure: CredentialsError) {
        self.failure = failure
    }

    func createSession(scopes: [Scope]) async throws -> Session {
        calls += 1
        if calls > 1 {
            throw failure
        }
        return Session(
            accessToken: "token-1",
            expiration: .absolute(Date().addingTimeInterval(0.5))
        )
    }
}

private actor MutableQuotaProvider: Provider {
    private var quotaProject = "quota-1"

    func createSession(scopes: [Scope]) async throws -> Session {
        Session(accessToken: "token", expiration: .never)
    }

    var quotaProjectID: String? {
        quotaProject
    }

    func setQuotaProjectID(_ value: String) {
        quotaProject = value
    }
}

@Suite("Auth")
struct AuthTests {
    @Test("Authorization caches non-expired provider sessions")
    func authorizationCachesSession() async throws {
        let provider = CountingProvider()
        let authorization = Authorization(scopes: ["scope-1"], provider: provider)

        #expect(try await authorization.accessToken() == "token-1")
        #expect(try await authorization.accessToken() == "token-1")
        #expect(await provider.calls == 1)
    }

    @Test("Authorization refreshes expired provider sessions")
    func authorizationRefreshesExpiredSession() async throws {
        let provider = CountingProvider(expiration: .always)
        let authorization = Authorization(scopes: ["scope-1"], provider: provider)

        #expect(try await authorization.accessToken() == "token-1")
        #expect(try await authorization.accessToken() == "token-2")
        #expect(await provider.calls == 2)
    }

    @Test("Short-lived authorization sessions use proportional refresh slack")
    func shortLivedAuthorizationSessionDoesNotRefreshEveryCall() async throws {
        let provider = CountingProvider(expiration: .absolute(Date().addingTimeInterval(60)))
        let authorization = Authorization(scopes: ["scope-1"], provider: provider)

        #expect(try await authorization.accessToken() == "token-1")
        #expect(try await authorization.accessToken() == "token-1")
        #expect(await provider.calls == 1)
    }

    @Test("Authorization serves a valid session after a transient refresh failure")
    func authorizationFailsOpenOnlyForTransientRefreshFailure() async throws {
        let provider = FailingSessionProvider(failure: .httpStatus(503, "unavailable"))
        let authorization = Authorization(scopes: ["scope-1"], provider: provider)

        #expect(try await authorization.accessToken() == "token-1")
        try await Task.sleep(for: .milliseconds(300))
        #expect(try await authorization.accessToken() == "token-1")
    }

    @Test("Authorization surfaces a permanent refresh failure")
    func authorizationDoesNotMaskPermanentRefreshFailure() async throws {
        let provider = FailingSessionProvider(failure: .parsing("revoked credential"))
        let authorization = Authorization(scopes: ["scope-1"], provider: provider)

        #expect(try await authorization.accessToken() == "token-1")
        try await Task.sleep(for: .milliseconds(300))
        await #expect(throws: CredentialsError.self) {
            _ = try await authorization.accessToken()
        }
    }

    @Test("Session-mode headers carry the credential's quota project")
    func sessionHeadersIncludeQuotaProject() async throws {
        let provider = CountingProvider(quotaProjectID: "quota-project")
        let authorization = Authorization(scopes: ["scope-1"], provider: provider)

        let headers = try await authorization.headers()
        #expect(headers.values["authorization"] == "Bearer token-1")
        #expect(headers.values["x-goog-user-project"] == "quota-project")
    }

    @Test("Session-mode headers do not retain a stale quota project")
    func sessionHeadersRefreshQuotaProject() async throws {
        let provider = MutableQuotaProvider()
        let authorization = Authorization(scopes: ["scope-1"], provider: provider)

        #expect(try await authorization.headers().values["x-goog-user-project"] == "quota-1")
        await provider.setQuotaProjectID("quota-2")
        #expect(try await authorization.headers().values["x-goog-user-project"] == "quota-2")
    }

    @Test("Token cache separates tokens by requested scopes")
    func tokenCacheSeparatesScopes() async throws {
        let provider = CountingTokenProvider()
        let credentials = Credentials(
            provider: AccessTokenCredentialsProvider(tokenProvider: provider)
        )

        let firstScopeToken = try await credentials.accessToken(scopes: ["scope-1"]).token
        let secondScopeToken = try await credentials.accessToken(scopes: ["scope-2"]).token
        let cachedFirstScopeToken = try await credentials.accessToken(scopes: ["scope-1"]).token

        #expect(firstScopeToken == "scope-1-1")
        #expect(secondScopeToken == "scope-2-2")
        #expect(cachedFirstScopeToken == firstScopeToken)
        #expect(await provider.calls == 2)
    }

    @Test("Token cache serves the still-valid cached token when a refresh fails")
    func tokenCacheFailsOpenDuringSlack() async throws {
        let credentials = Credentials(
            provider: AccessTokenCredentialsProvider(tokenProvider: FlakyTokenProvider())
        )

        #expect(try await credentials.accessToken(scopes: ["scope-1"]).token == "token-1")
        try await Task.sleep(for: .milliseconds(300))
        #expect(try await credentials.accessToken(scopes: ["scope-1"]).token == "token-1")
        #expect(try await credentials.accessToken(scopes: ["scope-1"]).token == "token-3")
    }

    @Test("Token cache does not mask permanent refresh failures")
    func tokenCacheSurfacesPermanentRefreshFailure() async throws {
        let credentials = Credentials(
            provider: AccessTokenCredentialsProvider(
                tokenProvider: PermanentlyFailingTokenProvider())
        )

        #expect(try await credentials.accessToken(scopes: ["scope-1"]).token == "token-1")
        try await Task.sleep(for: .milliseconds(300))
        await #expect(throws: CredentialsError.self) {
            _ = try await credentials.accessToken(scopes: ["scope-1"])
        }
    }

    @Test("Authorization interceptor maps credential failures to stable RPC codes")
    func authorizationInterceptorMapsCredentialFailures() {
        let permanent = AuthorizationClientInterceptor.rpcError(
            for: .parsing("missing private key"))
        #expect(permanent.code == .unauthenticated)
        #expect(permanent.message.contains("missing private key"))

        let transient = AuthorizationClientInterceptor.rpcError(
            for: .httpStatus(503, "unavailable"))
        #expect(transient.code == .unavailable)
    }

    @Test(
        "Malformed ADC credentials produce permanent authentication failures",
        arguments: [
            "not JSON",
            "{}",
            #"{"type":"authorized_user"}"#,
            #"{"type":"service_account","client_email":"client@example.com","private_key_id":"key-id","private_key":"not a PEM key"}"#,
            #"{"type":"impersonated_service_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken","source_credentials":{"type":"authorized_user"}}"#,
            #"{"type":"impersonated_service_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken","source_credentials":{"type":"service_account","client_email":"client@example.com","private_key_id":"key-id","private_key":"not a PEM key"}}"#,
        ]
    )
    func malformedADCCredentialsFailPermanently(_ json: String) {
        do {
            _ = try DefaultProvider.provider(from: Data(json.utf8))
            Issue.record("expected malformed credentials to fail")
        } catch let error as CredentialsError {
            guard case .parsing(let message) = error else {
                Issue.record("expected a credential parsing error, got \(error)")
                return
            }
            #expect(message.isEmpty == false)
            #expect(error.isTransient == false)
            #expect(AuthorizationClientInterceptor.rpcError(for: error).code == .unauthenticated)
        } catch {
            Issue.record("expected a permanent credential error, got \(error)")
        }
    }

    @Test("ADC parsing preserves unsupported credential errors")
    func adcParsingPreservesUnsupportedCredentialError() {
        #expect(throws: CredentialsError.unsupported("unknown credential type 'unrecognized'")) {
            _ = try DefaultProvider.provider(from: Data(#"{"type":"unrecognized"}"#.utf8))
        }
    }

    @Test("API key credentials create x-goog-api-key headers")
    func apiKeyHeaders() async throws {
        let credentials = APIKeyCredentials.Builder("test-api-key").build()
        let headers = try await credentials.headers()

        #expect(headers.values == ["x-goog-api-key": "test-api-key"])
    }

    @Test("Service account keys decode Google ADC field names")
    func serviceAccountKeyDecoding() throws {
        let data = Data(
            """
            {
              "type": "service_account",
              "client_email": "client@example.com",
              "private_key_id": "key-id",
              "private_key": "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n",
              "project_id": "project-id",
              "universe_domain": "googleapis.com"
            }
            """.utf8
        )

        let key = try JSONDecoder().decode(ServiceAccountKey.self, from: data)

        #expect(key.clientEmail == "client@example.com")
        #expect(key.privateKeyID == "key-id")
        #expect(key.projectID == "project-id")
        #expect(key.universeDomain == "googleapis.com")
    }

    @Test("Service-account provider rejects an invalid private key")
    func serviceAccountProviderRejectsInvalidPrivateKey() {
        let key = ServiceAccountKey(
            clientEmail: "client@example.com",
            privateKeyID: "key-id",
            privateKey: "not a PEM key"
        )

        #expect(throws: (any Error).self) {
            _ = try ServiceAccountProvider(
                credentials: key,
                accessSpecifier: .fromScopes(["scope-1"])
            )
        }
    }

    @Test("User-account refresh grants do not request new scopes")
    func userAccountRefreshDoesNotSendScopes() throws {
        let provider = try UserAccountProvider(
            credentials: UserAccountCredentials(
                clientID: "client-id",
                clientSecret: "client-secret",
                refreshToken: "refresh-token"
            )
        )

        #expect(provider.refreshParameters["grant_type"] == "refresh_token")
        #expect(provider.refreshParameters["scope"] == nil)
    }

    @Test("Impersonation does not own the process-wide default provider")
    func impersonationPreservesDefaultProviderOwnership() {
        #expect(ImpersonatedProvider.ownsSourceProvider(DefaultProvider.shared) == false)
        #expect(ImpersonatedProvider.ownsSourceProvider(CountingProvider()))
    }

    @Test("OAuth refresh parameters use form encoding")
    func oauthRefreshFormEncoding() async throws {
        let url = try #require(URL(string: "https://oauth2.example/token"))
        let request = AuthHTTP.formRequest(
            url: url,
            parameters: [
                "client_secret": "a+b&c",
                "grant_type": "refresh_token",
                "scope": "scope-one scope-two",
            ]
        )

        #expect(request.method == .POST)
        #expect(request.headers.first(name: "Content-Type") == "application/x-www-form-urlencoded")

        let bodyBytes = try #require(request.body)
        let collected = try await bodyBytes.collect(upTo: AuthHTTP.maxResponseBytes)
        let body = String(decoding: collected.readableBytesView, as: UTF8.self)
        #expect(body.contains("client_secret=a%2Bb%26c"))
        #expect(body.contains("grant_type=refresh_token"))
        #expect(body.contains("scope=scope-one+scope-two"))
    }

    @Test("Auth HTTP deadlines include response-body work")
    func authHTTPDeadlineBoundsTheCompleteOperation() async {
        await #expect(throws: CredentialsError.self) {
            let _: Int = try await AuthHTTP.withDeadline(.milliseconds(10)) {
                try await Task.sleep(for: .seconds(1))
                return 1
            }
        }
    }

    @Test(
        "Malformed metadata hosts produce credential errors",
        arguments: ["[", "metadata.google.internal:70000", "metadata.google.internal:0"]
    )
    func malformedMetadataHostThrows(_ host: String) async {
        let provider = MDSProvider(metadataHost: host)

        await #expect(throws: CredentialsError.self) {
            try await provider.token(scopes: [])
        }
    }

    @Test(
        "An empty GCE_METADATA_HOST falls back to the default metadata server",
        arguments: [[:], ["GCE_METADATA_HOST": ""]] as [[String: String]]
    )
    func emptyMetadataHostVariableFallsBack(_ environment: [String: String]) throws {
        let endpoint = try #require(MDSProvider.defaultEndpoint(environment: environment))
        #expect(endpoint.host() == AuthConstants.metadataHost)
    }

    @Test("A populated GCE_METADATA_HOST overrides the default metadata server")
    func metadataHostVariableOverridesDefault() throws {
        let endpoint = try #require(
            MDSProvider.defaultEndpoint(environment: ["GCE_METADATA_HOST": "169.254.169.254"]))
        #expect(endpoint.host() == "169.254.169.254")
    }
}
