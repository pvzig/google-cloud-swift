import Foundation

public struct MDSProvider: Provider, TokenProvider {
  private let endpoint: URL?

  public init(endpoint: URL? = nil) {
    self.endpoint = endpoint ?? Self.defaultEndpoint()
  }

  init(metadataHost: String) {
    self.endpoint = Self.metadataEndpoint(host: metadataHost)
  }

  public func createSession(scopes: [Scope]) async throws -> Session {
    try await token(scopes: scopes).session
  }

  public func token(scopes: [Scope]) async throws -> Token {
    guard let endpoint else {
      throw CredentialsError.loading("invalid metadata server URL")
    }

    var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false)
    if !scopes.isEmpty {
      components?.queryItems = [
        URLQueryItem(name: "scopes", value: scopes.map(\.rawValue).joined(separator: ","))
      ]
    }
    guard let url = components?.url else {
      throw CredentialsError.loading("invalid metadata server URL")
    }
    let response: OAuthTokenResponse = try await AuthHTTP.getJSON(
      url: url,
      headers: [AuthConstants.metadataFlavorHeader: AuthConstants.metadataFlavorValue]
    )
    return response.token
  }

  public func buildCredentials() -> Credentials {
    Credentials(provider: AccessTokenCredentialsProvider(tokenProvider: self))
  }

  public func shutdown() async throws {}

  static func defaultEndpoint(
    environment: [String: String] = ProcessInfo.processInfo.environment
  ) -> URL? {
    // A set-but-empty GCE_METADATA_HOST (`docker run -e GCE_METADATA_HOST`, or
    // an `export GCE_METADATA_HOST=`) is not an override: treating it as one
    // would skip the fallback and break all ADC on GCE.
    let host =
      environment[AuthConstants.gceMetadataHostVariable]
      .flatMap { $0.isEmpty ? nil : $0 }
      ?? AuthConstants.metadataHost
    return metadataEndpoint(host: host)
  }

  private static func metadataEndpoint(host: String) -> URL? {
    guard let components = URLComponents(string: "http://\(host)"),
      components.host?.isEmpty == false,
      components.user == nil,
      components.password == nil,
      components.path.isEmpty || components.path == "/",
      components.query == nil,
      components.fragment == nil,
      components.port.map { (1...65_535).contains($0) } ?? true,
      let baseURL = components.url
    else {
      return nil
    }

    return baseURL.appending(
      path: "computeMetadata/v1/instance/service-accounts/default/token")
  }
}
