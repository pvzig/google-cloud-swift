import Foundation

enum AuthConstants {
  static let defaultScope = "https://www.googleapis.com/auth/cloud-platform"
  static let googleCloudQuotaProjectVariable = "GOOGLE_CLOUD_QUOTA_PROJECT"
  static let googleApplicationCredentialsVariable = "GOOGLE_APPLICATION_CREDENTIALS"
  static let gceMetadataHostVariable = "GCE_METADATA_HOST"
  static let jwtBearerGrantType = "urn:ietf:params:oauth:grant-type:jwt-bearer"
  static let oauth2TokenServerURL = "https://oauth2.googleapis.com/token"
  static let metadataHost = "metadata.google.internal"
  static let defaultUniverseDomain = "googleapis.com"
  static let metadataFlavorHeader = "Metadata-Flavor"
  static let metadataFlavorValue = "Google"
  static let quotaProjectHeader = "x-goog-user-project"
}

public struct Scope: Sendable, Hashable, ExpressibleByStringLiteral {
  public let rawValue: String

  public init(rawValue: String) {
    self.rawValue = rawValue
  }

  public init(stringLiteral value: StringLiteralType) {
    self.init(rawValue: value)
  }
}
