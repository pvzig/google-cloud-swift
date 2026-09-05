import GRPCCore
import Logging

public struct AuthorizationClientInterceptor: ClientInterceptor {
    private static let logger = Logger(label: "google-cloud-swift.auth.interceptor")

    private let authorization: Authorization

    public init(authorization: Authorization) {
        self.authorization = authorization
    }

    public func intercept<Input: Sendable, Output: Sendable>(
        request: StreamingClientRequest<Input>,
        context: ClientContext,
        next: (StreamingClientRequest<Input>, ClientContext) async throws ->
            StreamingClientResponse<
                Output
            >
    ) async throws -> StreamingClientResponse<Output> {
        var request = request
        let headers: AuthHeaders
        do {
            headers = try await authorization.headers()
        } catch let error as CredentialsError {
            // Interceptors may throw arbitrary errors, but grpc-swift normalizes them
            // to an empty UNKNOWN status. Map credential failures here so permanent
            // configuration errors are not mistaken for retryable transport failures.
            let rpcError = Self.rpcError(for: error)
            Self.logger.warning(
                "Authorization failed",
                metadata: ["code": "\(rpcError.code)", "error": "\(error)"]
            )
            throw rpcError
        }
        for (key, value) in headers.values {
            request.metadata.addString(value, forKey: key)
        }
        return try await next(request, context)
    }

    static func rpcError(for error: CredentialsError) -> RPCError {
        RPCError(
            code: error.isTransient ? .unavailable : .unauthenticated,
            message: error.description,
            cause: error
        )
    }
}
