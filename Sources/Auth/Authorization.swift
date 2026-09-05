import Foundation
import Logging

public actor Authorization {
    private enum HeaderMode: Sendable {
        case session
        case credentials
    }

    public let scopes: [Scope]
    private let credentials: Credentials
    private let headerMode: HeaderMode
    private let sessionProvider: (any Provider)?
    private struct CachedSession {
        let session: Session
        let refreshAfter: Date
    }

    private static let logger = Logger(label: "google-cloud-swift.auth.authorization")

    private var cachedSession: CachedSession?
    private var sessionRefreshTask: Task<Session, Error>?

    public init(
        scopes: [Scope],
        provider: any Provider = DefaultProvider.shared
    ) {
        self.scopes = scopes
        self.credentials = Credentials(
            provider: ProviderBackedCredentialsProvider(provider: provider))
        self.headerMode = .session
        self.sessionProvider = provider
    }

    public init(scopes: [Scope], credentials: Credentials) {
        self.scopes = scopes
        self.credentials = credentials
        self.headerMode = .credentials
        self.sessionProvider = nil
    }

    public func accessToken() async throws -> String {
        try await getSession().accessToken
    }

    public func headers() async throws -> AuthHeaders {
        switch headerMode {
        case .session:
            let token = try await accessToken()
            guard !token.isEmpty else {
                return AuthHeaders()
            }
            var values = ["authorization": "Bearer \(token)"]
            if let quotaProjectID = await resolveQuotaProjectID() {
                values[AuthConstants.quotaProjectHeader] = quotaProjectID
            }
            return AuthHeaders(values: values)
        case .credentials:
            return try await credentials.headers(scopes: scopes)
        }
    }

    public func getSession() async throws -> Session {
        let now = Date()
        if let cachedSession, now < cachedSession.refreshAfter {
            return cachedSession.session
        }

        if let sessionRefreshTask {
            return try await resolveRefresh(sessionRefreshTask, fallback: cachedSession)
        }

        Self.logger.debug("Refreshing authorization session")
        let refreshTask = Task { [credentials, scopes] in
            try await credentials.accessToken(scopes: scopes).session
        }
        sessionRefreshTask = refreshTask
        return try await resolveRefresh(refreshTask, fallback: cachedSession)
    }

    private func resolveRefresh(
        _ refreshTask: Task<Session, Error>,
        fallback: CachedSession?
    ) async throws -> Session {
        do {
            let session = try await refreshTask.value
            if sessionRefreshTask == refreshTask {
                let receivedAt = Date()
                cachedSession = CachedSession(
                    session: session,
                    refreshAfter: session.refreshAfter(receivedAt: receivedAt)
                )
                sessionRefreshTask = nil
            }
            return session
        } catch {
            if sessionRefreshTask == refreshTask {
                sessionRefreshTask = nil
            }
            if let credentialError = error as? CredentialsError,
                credentialError.isTransient,
                let fallback,
                fallback.session.isExpired == false
            {
                Self.logger.warning(
                    "Authorization refresh failed open with a still-valid session",
                    metadata: ["error": "\(credentialError)"]
                )
                return fallback.session
            }
            throw error
        }
    }

    public func shutdown() async throws {
        sessionRefreshTask?.cancel()
        sessionRefreshTask = nil
        cachedSession = nil
        try await credentials.shutdown()
    }

    private func resolveQuotaProjectID() async -> String? {
        guard let sessionProvider else {
            return nil
        }

        // DefaultProvider can be bootstrapped with a different underlying provider
        // while this Authorization remains alive. Resolve the value alongside each
        // header so it cannot outlive the provider that supplied the token.
        return await sessionProvider.quotaProjectID
    }
}
