import Foundation

public struct APIKeyCredentialsProvider: CredentialsProvider {
  private let apiKey: String

  public init(apiKey: String) {
    self.apiKey = apiKey
  }

  public func headers(scopes: [Scope]) async throws -> AuthHeaders {
    AuthHeadersBuilder.apiKey(apiKey)
  }

  public func accessToken(scopes: [Scope]) async throws -> Token {
    Token(token: apiKey, tokenType: "", expiresAt: nil)
  }
}

public enum APIKeyCredentials {
  public struct Builder: Sendable {
    private let apiKey: String

    public init(_ apiKey: String) {
      self.apiKey = apiKey
    }

    public func build() -> Credentials {
      Credentials(provider: APIKeyCredentialsProvider(apiKey: apiKey))
    }
  }
}
