import Auth
import Foundation
import GRPCCore
import GRPCProtobuf

public struct PubSubServiceError: Error, Sendable, Equatable, CustomStringConvertible {
    public let code: RPCError.Code?
    public let message: String

    /// Exactly one of these is set: values supplied by a caller, or the RPC
    /// error the info is decoded from on demand.
    private let suppliedErrorInfo: [PubSubErrorInfo]?
    private let rpcError: RPCError?

    public init(
        code: RPCError.Code? = nil,
        message: String,
        errorInfo: [PubSubErrorInfo] = []
    ) {
        self.code = code
        self.message = message
        self.suppliedErrorInfo = errorInfo
        self.rpcError = nil
    }

    init(rpcError: RPCError) {
        if isCancellation(rpcError) {
            self.code = .cancelled
            self.message = rpcError.message.isEmpty ? "cancelled" : rpcError.message
        } else if let credentialsError = Self.credentialsError(in: rpcError) {
            self.code = credentialsError.isTransient ? .unavailable : .unauthenticated
            self.message =
                rpcError.message.isEmpty ? credentialsError.description : rpcError.message
        } else {
            self.code = rpcError.code
            let causeMessage = rpcError.cause.map { String(describing: $0) } ?? ""
            self.message = rpcError.message.isEmpty ? causeMessage : rpcError.message
        }
        self.suppliedErrorInfo = nil
        self.rpcError = rpcError
    }

    private static func credentialsError(in error: any Error) -> CredentialsError? {
        if let credentialsError = error as? CredentialsError {
            return credentialsError
        }
        guard let rpcError = error as? RPCError, let cause = rpcError.cause else {
            return nil
        }
        return credentialsError(in: cause)
    }

    /// Structured `google.rpc.ErrorInfo` values returned by the service.
    ///
    /// Decoded from the RPC status on read rather than at construction: most
    /// failures (stream reconnects, lease-extension retries, publish backoff)
    /// never consult it, and decoding a protobuf `Any` per failure to populate a
    /// field nobody reads is pure overhead.
    public var errorInfo: [PubSubErrorInfo] {
        if let suppliedErrorInfo {
            return suppliedErrorInfo
        }

        guard let rpcError, let status = try? rpcError.unpackGoogleRPCStatus() else {
            return []
        }

        return status.details.compactMap(\.errorInfo).map {
            PubSubErrorInfo(reason: $0.reason, domain: $0.domain, metadata: $0.metadata)
        }
    }

    /// Compares the decoded info rather than its representation, so an error
    /// built from an RPC status equals one built from the same explicit values.
    public static func == (lhs: PubSubServiceError, rhs: PubSubServiceError) -> Bool {
        lhs.code == rhs.code && lhs.message == rhs.message && lhs.errorInfo == rhs.errorInfo
    }

    public var description: String {
        if let code {
            return "\(code): \(message)"
        }

        return message
    }
}

public enum PublishError: Error, Sendable, Equatable, CustomStringConvertible {
    case orderingKeyPaused
    case service(PubSubServiceError)
    case shutdown

    public var description: String {
        switch self {
        case .orderingKeyPaused:
            return "publishing is paused for this ordering key"
        case .service(let error):
            return error.description
        case .shutdown:
            return "publisher shut down before the message was published"
        }
    }
}

public enum AckError: Error, Sendable, Equatable, CustomStringConvertible {
    case leaseExpired
    case service(PubSubServiceError)
    case shutdown(String)

    public var description: String {
        switch self {
        case .leaseExpired:
            return "the message's lease expired before it could be acknowledged"
        case .service(let error):
            return error.description
        case .shutdown(let message):
            return message
        }
    }
}

func isCancellation(_ error: any Error) -> Bool {
    if error is CancellationError {
        return true
    }
    guard let rpcError = error as? RPCError else {
        return false
    }
    return rpcError.code == .cancelled
        || rpcError.cause.map(isCancellation) == true
}

func asServiceError(_ error: any Error) -> PubSubServiceError {
    if let serviceError = error as? PubSubServiceError {
        return serviceError
    }

    if let rpcError = error as? RPCError {
        return PubSubServiceError(rpcError: rpcError)
    }

    if let credentialsError = error as? CredentialsError {
        return PubSubServiceError(
            code: credentialsError.isTransient ? .unavailable : .unauthenticated,
            message: credentialsError.description
        )
    }

    if error is CancellationError {
        return PubSubServiceError(code: .cancelled, message: "cancelled")
    }

    // Errors with no RPC status (connection resets, transport teardown) are
    // transient: upstream unconditionally resumes streams and retries on io
    // and transport errors, and UNKNOWN is in the default retryable set.
    return PubSubServiceError(code: .unknown, message: String(describing: error))
}
