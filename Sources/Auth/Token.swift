import Foundation

public struct Token: Sendable, Equatable, CustomDebugStringConvertible {
    public let token: String
    public let tokenType: String
    public let expiresAt: Date?
    public let metadata: [String: String]?

    public init(
        token: String,
        tokenType: String = "Bearer",
        expiresAt: Date? = nil,
        metadata: [String: String]? = nil
    ) {
        self.token = token
        self.tokenType = tokenType
        self.expiresAt = expiresAt
        self.metadata = metadata
    }

    public var debugDescription: String {
        "Token(token: \"[censored]\", tokenType: \"\(tokenType)\", expiresAt: \(String(describing: expiresAt)), metadata: \(String(describing: metadata)))"
    }

    var isExpired: Bool {
        guard let expiresAt else {
            return false
        }
        return expiresAt <= Date()
    }

    var shouldRefresh: Bool {
        guard let expiresAt else {
            return false
        }
        let refreshSlack: TimeInterval = 240
        return expiresAt.timeIntervalSinceNow <= refreshSlack
    }

    func refreshAfter(receivedAt: Date) -> Date {
        guard let expiresAt else {
            return .distantFuture
        }

        let remainingLifetime = max(0, expiresAt.timeIntervalSince(receivedAt))
        let refreshSlack = min(240, remainingLifetime / 2)
        return expiresAt.addingTimeInterval(-refreshSlack)
    }
}

public struct Session: Sendable, Equatable {
    public let accessToken: String
    public let expiration: Expiration

    public init(accessToken: String, expiration: Expiration) {
        self.accessToken = accessToken
        self.expiration = expiration
    }

    public var isExpired: Bool {
        switch expiration {
        case .absolute(let date):
            return date <= Date()
        case .always:
            return true
        case .never:
            return false
        }
    }

    /// Mirrors `Token.shouldRefresh`: refresh ahead of expiry so a token can't
    /// expire while an RPC carrying it is in flight.
    public var shouldRefresh: Bool {
        switch expiration {
        case .absolute(let date):
            let refreshSlack: TimeInterval = 240
            return date.timeIntervalSinceNow <= refreshSlack
        case .always:
            return true
        case .never:
            return false
        }
    }

    func refreshAfter(receivedAt: Date) -> Date {
        switch expiration {
        case .absolute(let date):
            let remainingLifetime = max(0, date.timeIntervalSince(receivedAt))
            let refreshSlack = min(240, remainingLifetime / 2)
            return date.addingTimeInterval(-refreshSlack)
        case .always:
            return receivedAt
        case .never:
            return .distantFuture
        }
    }
}

extension Session {
    public enum Expiration: Sendable, Equatable {
        case absolute(Date)
        case never
        case always
    }
}

extension Token {
    var session: Session {
        Session(
            accessToken: token,
            expiration: expiresAt.map(Session.Expiration.absolute) ?? .never
        )
    }
}
