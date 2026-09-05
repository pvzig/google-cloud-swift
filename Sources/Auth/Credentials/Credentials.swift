import Foundation

public protocol Provider: Sendable {
  func createSession(scopes: [Scope]) async throws -> Session
  func shutdown() async throws
  /// The quota project to bill (sent as `x-goog-user-project`), when the
  /// underlying credential carries one (e.g. user-account ADC).
  var quotaProjectID: String? { get async }
}

extension Provider {
  public func shutdown() async throws {}

  public var quotaProjectID: String? {
    get async { nil }
  }
}

public protocol CredentialsProvider: Sendable {
  func headers(scopes: [Scope]) async throws -> AuthHeaders
  func accessToken(scopes: [Scope]) async throws -> Token
  func universeDomain() async -> String?
  func shutdown() async throws
}

extension CredentialsProvider {
  public func universeDomain() async -> String? {
    AuthConstants.defaultUniverseDomain
  }

  public func shutdown() async throws {}
}

public struct Credentials: Sendable {
  private let provider: any CredentialsProvider

  public init(provider: any CredentialsProvider) {
    self.provider = provider
  }

  public func headers(scopes: [Scope] = []) async throws -> AuthHeaders {
    try await provider.headers(scopes: scopes)
  }

  public func accessToken(scopes: [Scope] = []) async throws -> Token {
    try await provider.accessToken(scopes: scopes)
  }

  public func universeDomain() async -> String? {
    await provider.universeDomain()
  }

  public func shutdown() async throws {
    try await provider.shutdown()
  }
}

struct AccessTokenCredentialsProvider: CredentialsProvider {
  let cache: TokenCache
  let quotaProjectID: String?
  let universeDomainValue: String?

  init(
    tokenProvider: any TokenProvider,
    quotaProjectID: String? = nil,
    universeDomain: String? = AuthConstants.defaultUniverseDomain
  ) {
    self.cache = TokenCache(provider: tokenProvider)
    self.quotaProjectID = quotaProjectID
    self.universeDomainValue = universeDomain
  }

  func headers(scopes: [Scope]) async throws -> AuthHeaders {
    try await AuthHeadersBuilder.bearer(
      token: accessToken(scopes: scopes),
      quotaProjectID: quotaProjectID
    )
  }

  func accessToken(scopes: [Scope]) async throws -> Token {
    try await cache.token(scopes: scopes)
  }

  func universeDomain() async -> String? {
    universeDomainValue
  }

  func shutdown() async throws {
    try await cache.shutdown()
  }
}

struct ProviderBackedCredentialsProvider: CredentialsProvider {
  let provider: any Provider

  func headers(scopes: [Scope]) async throws -> AuthHeaders {
    let session = try await provider.createSession(scopes: scopes)
    var values = ["authorization": "Bearer \(session.accessToken)"]
    if let quotaProjectID = await provider.quotaProjectID {
      values[AuthConstants.quotaProjectHeader] = quotaProjectID
    }
    return AuthHeaders(values: values)
  }

  func accessToken(scopes: [Scope]) async throws -> Token {
    let session = try await provider.createSession(scopes: scopes)
    let expiresAt: Date?
    switch session.expiration {
    case .absolute(let date):
      expiresAt = date
    case .always:
      expiresAt = .distantPast
    case .never:
      expiresAt = nil
    }
    return Token(token: session.accessToken, expiresAt: expiresAt)
  }

  func shutdown() async throws {
    // The shared DefaultProvider singleton is owned by the process, not by
    // any single client: shutting it down here would wipe its resolved
    // credentials (and any bootstrapped override) for every other client.
    if provider is DefaultProvider {
      return
    }

    try await provider.shutdown()
  }
}
