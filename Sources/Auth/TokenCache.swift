import Foundation
import Logging

public protocol TokenProvider: Sendable {
    func token(scopes: [Scope]) async throws -> Token
    func shutdown() async throws
}

extension TokenProvider {
    public func shutdown() async throws {}
}

actor TokenCache {
    private struct CachedToken {
        let token: Token
        let refreshAfter: Date
    }

    private static let logger = Logger(label: "google-cloud-swift.auth.token-cache")

    private let provider: any TokenProvider
    private var cachedTokens: [ScopeCacheKey: CachedToken] = [:]
    private var refreshTasks: [ScopeCacheKey: Task<Token, Error>] = [:]

    init(provider: any TokenProvider) {
        self.provider = provider
    }

    func token(scopes: [Scope]) async throws -> Token {
        let key = ScopeCacheKey(scopes: scopes)
        if let cachedToken = cachedTokens[key], Date() < cachedToken.refreshAfter {
            return cachedToken.token
        }

        if let refreshTask = refreshTasks[key] {
            do {
                return try await refreshTask.value
            } catch {
                return try cachedTokenOrRethrow(key: key, error: error)
            }
        }

        Self.logger.debug("Refreshing access token")
        let task = Task { [provider] in
            try await provider.token(scopes: scopes)
        }
        refreshTasks[key] = task

        do {
            let token = try await task.value
            let receivedAt = Date()
            cachedTokens[key] = CachedToken(
                token: token,
                refreshAfter: token.refreshAfter(receivedAt: receivedAt)
            )
            if refreshTasks[key] == task {
                refreshTasks[key] = nil
            }
            return token
        } catch {
            if refreshTasks[key] == task {
                refreshTasks[key] = nil
            }
            return try cachedTokenOrRethrow(key: key, error: error)
        }
    }

    /// A refresh inside the early-refresh slack window fails open: the cached
    /// token is still valid for up to four minutes, so a transient refresh
    /// error should not fail calls that the old token could still serve.
    private func cachedTokenOrRethrow(key: ScopeCacheKey, error: any Error) throws -> Token {
        if let credentialError = error as? CredentialsError,
            credentialError.isTransient,
            let cachedToken = cachedTokens[key]?.token,
            cachedToken.isExpired == false
        {
            Self.logger.warning(
                "Token refresh failed open with a still-valid token",
                metadata: ["error": "\(credentialError)"]
            )
            return cachedToken
        }

        throw error
    }

    func shutdown() async throws {
        for refreshTask in refreshTasks.values {
            refreshTask.cancel()
        }
        refreshTasks = [:]
        cachedTokens = [:]
        try await provider.shutdown()
    }
}

private struct ScopeCacheKey: Hashable {
    let rawScopes: [String]

    init(scopes: [Scope]) {
        self.rawScopes = scopes.map(\.rawValue)
    }
}
