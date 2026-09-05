import Foundation

public struct ExternalAccountProvider: Provider {
  public init() {}

  public func createSession(scopes: [Scope]) async throws -> Session {
    throw CredentialsError.unsupported(
      "external_account credentials are part of the Rust auth surface, but this Swift port has not implemented STS exchange yet"
    )
  }
}
