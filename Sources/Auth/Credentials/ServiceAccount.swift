import Foundation
import JWTKit

public struct ServiceAccountKey: Sendable, Decodable, Equatable {
  public let type: String?
  public let clientEmail: String
  public let privateKeyID: String
  public let privateKey: String
  public let projectID: String?
  public let universeDomain: String?

  public init(
    type: String? = "service_account",
    clientEmail: String,
    privateKeyID: String,
    privateKey: String,
    projectID: String? = nil,
    universeDomain: String? = nil
  ) {
    self.type = type
    self.clientEmail = clientEmail
    self.privateKeyID = privateKeyID
    self.privateKey = privateKey
    self.projectID = projectID
    self.universeDomain = universeDomain
  }

  enum CodingKeys: String, CodingKey {
    case type
    case clientEmail = "client_email"
    case privateKeyID = "private_key_id"
    case privateKey = "private_key"
    case projectID = "project_id"
    case universeDomain = "universe_domain"
  }
}

public enum ServiceAccountAccessSpecifier: Sendable, Equatable {
  case audience(String)
  case scopes([Scope])

  public static func fromAudience(_ audience: String) -> Self {
    .audience(audience)
  }

  public static func fromScopes(_ scopes: [Scope]) -> Self {
    .scopes(scopes)
  }
}

public actor ServiceAccountProvider: Provider, TokenProvider {
  private let credentials: ServiceAccountKey
  private let accessSpecifier: ServiceAccountAccessSpecifier?
  private let keysTask: Task<JWTKeyCollection, Error>

  public init(
    credentials: ServiceAccountKey,
    accessSpecifier: ServiceAccountAccessSpecifier? = nil
  ) throws {
    self.credentials = credentials
    self.accessSpecifier = accessSpecifier
    let privateKey = try Insecure.RSA.PrivateKey(pem: credentials.privateKey)
    self.keysTask = Task {
      let keys = JWTKeyCollection()
      await keys.add(
        rsa: privateKey,
        digestAlgorithm: .sha256,
        kid: .init(string: credentials.privateKeyID)
      )
      return keys
    }
  }

  public init(credentialsURL: URL) throws {
    let data = try Data(contentsOf: credentialsURL)
    let key = try JSONDecoder().decode(ServiceAccountKey.self, from: data)
    try self.init(credentials: key)
  }

  public func createSession(scopes: [Scope]) async throws -> Session {
    try await token(scopes: scopes).session
  }

  public func token(scopes: [Scope]) async throws -> Token {
    // Mirrors upstream jws.rs: iat is backdated by the 10s clock-skew fudge
    // and the token is valid for the default 3600s timeout from now, so the
    // assertion carries exp - iat = 3620s (proven accepted in production by
    // the upstream Rust client).
    let now = Date()
    let issuedAt = Int(now.addingTimeInterval(-10).timeIntervalSince1970)
    let expiresAt = now.addingTimeInterval(3_610)
    let expiration = Int(expiresAt.timeIntervalSince1970)
    let access = resolvedAccessSpecifier(scopes: scopes)

    let payload = ServiceAccountJWTPayload(
      issuer: credentials.clientEmail,
      scope: access.scope,
      audience: access.audience,
      issuedAt: issuedAt,
      expiration: expiration,
      subject: credentials.clientEmail
    )
    let header: JWTHeader = ["typ": "JWT", "alg": "RS256"]
    let keys = try await keysTask.value
    let assertion = try await keys.sign(
      payload,
      kid: .init(string: credentials.privateKeyID),
      header: header
    )
    return Token(token: assertion, tokenType: "Bearer", expiresAt: expiresAt)
  }

  public func buildCredentials(quotaProjectID: String? = nil) -> Credentials {
    Credentials(
      provider: AccessTokenCredentialsProvider(
        tokenProvider: self,
        quotaProjectID: quotaProjectID,
        universeDomain: credentials.universeDomain ?? AuthConstants.defaultUniverseDomain
      )
    )
  }

  public func shutdown() async throws {}

  private func resolvedAccessSpecifier(scopes: [Scope]) -> (scope: String?, audience: String?) {
    switch accessSpecifier {
    case .audience(let audience):
      return (nil, audience)
    case .scopes(let scopes):
      return (scopes.map(\.rawValue).joined(separator: " "), nil)
    case nil:
      let effectiveScopes = scopes.isEmpty ? [Scope(rawValue: AuthConstants.defaultScope)] : scopes
      return (effectiveScopes.map(\.rawValue).joined(separator: " "), nil)
    }
  }
}

public enum ServiceAccountCredentials {
  public struct Builder: Sendable {
    private var key: ServiceAccountKey
    private var accessSpecifier: ServiceAccountAccessSpecifier?
    private var quotaProjectID: String?

    public init(_ key: ServiceAccountKey) {
      self.key = key
    }

    public func withAccessSpecifier(_ accessSpecifier: ServiceAccountAccessSpecifier) -> Self {
      var copy = self
      copy.accessSpecifier = accessSpecifier
      return copy
    }

    public func withQuotaProjectID(_ quotaProjectID: String) -> Self {
      var copy = self
      copy.quotaProjectID = quotaProjectID
      return copy
    }

    public func build() throws -> Credentials {
      let provider = try ServiceAccountProvider(
        credentials: key,
        accessSpecifier: accessSpecifier
      )
      return Credentials(
        provider: AccessTokenCredentialsProvider(
          tokenProvider: provider,
          quotaProjectID: quotaProjectID,
          universeDomain: key.universeDomain ?? AuthConstants.defaultUniverseDomain
        )
      )
    }
  }
}

private struct ServiceAccountJWTPayload: JWTPayload {
  let issuer: String
  let scope: String?
  let audience: String?
  let issuedAt: Int
  let expiration: Int
  let subject: String

  enum CodingKeys: String, CodingKey {
    case issuer = "iss"
    case scope
    case audience = "aud"
    case issuedAt = "iat"
    case expiration = "exp"
    case subject = "sub"
  }

  func verify(using algorithm: some JWTAlgorithm) async throws {}
}
