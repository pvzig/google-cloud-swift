/// Machine-readable context attached to a Google API error.
public struct PubSubErrorInfo: Sendable, Equatable {
  public let reason: String
  public let domain: String
  public let metadata: [String: String]

  public init(reason: String, domain: String, metadata: [String: String] = [:]) {
    self.reason = reason
    self.domain = domain
    self.metadata = metadata
  }
}
