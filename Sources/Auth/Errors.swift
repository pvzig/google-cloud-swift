import Foundation

public enum CredentialsError: Error, Sendable, CustomStringConvertible, Equatable {
  case loading(String)
  case parsing(String)
  case unsupported(String)
  case httpStatus(Int, String)
  case message(String, isTransient: Bool)

  public var isTransient: Bool {
    switch self {
    case .httpStatus(let status, _):
      return status == 408 || status == 429 || (500..<600).contains(status)
    case .message(_, let isTransient):
      return isTransient
    case .loading, .parsing, .unsupported:
      return false
    }
  }

  public var description: String {
    switch self {
    case .loading(let message):
      return "failed to load credentials: \(message)"
    case .parsing(let message):
      return "failed to parse credentials: \(message)"
    case .unsupported(let message):
      return "unsupported credentials: \(message)"
    case .httpStatus(let status, let body):
      return "auth endpoint returned HTTP \(status): \(body)"
    case .message(let message, _):
      return message
    }
  }
}

extension CredentialsError {
  static func decoding(_ error: any Error) -> CredentialsError {
    .parsing(String(describing: error))
  }
}
