import Foundation

public struct AuthHeaders: Sendable, Equatable {
  public var values: [String: String]

  public init(values: [String: String] = [:]) {
    self.values = values
  }
}

enum AuthHeadersBuilder {
  static func bearer(token: Token, quotaProjectID: String? = nil) -> AuthHeaders {
    var values = ["authorization": "\(token.tokenType) \(token.token)"]
    if let quotaProjectID {
      values[AuthConstants.quotaProjectHeader] = quotaProjectID
    }
    return AuthHeaders(values: values)
  }

  static func apiKey(_ apiKey: String) -> AuthHeaders {
    AuthHeaders(values: ["x-goog-api-key": apiKey])
  }
}
