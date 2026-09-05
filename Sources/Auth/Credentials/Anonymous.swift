import Foundation

public struct AnonymousCredentialsProvider: CredentialsProvider {
  public init() {}

  public func headers(scopes: [Scope]) async throws -> AuthHeaders {
    AuthHeaders()
  }

  public func accessToken(scopes: [Scope]) async throws -> Token {
    Token(token: "", tokenType: "", expiresAt: nil)
  }

  public func universeDomain() async -> String? {
    nil
  }
}

public enum AnonymousCredentials {
  public struct Builder: Sendable {
    public init() {}

    public func build() -> Credentials {
      Credentials(provider: AnonymousCredentialsProvider())
    }
  }
}
