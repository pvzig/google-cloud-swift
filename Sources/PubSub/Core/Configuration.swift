import Auth
import Foundation
import GRPCCore

public enum CredentialsMode: Sendable {
    case automatic
    case anonymous
    case custom(Authorization)
}

public struct RetryPolicy: Sendable, Equatable {
    public var retryableCodes: Set<RPCError.Code>
    public var initialBackoff: Duration
    public var maxBackoff: Duration
    public var multiplier: Double
    public var maxAttempts: Int?
    public var maxElapsedTime: Duration?

    public init(
        // Matches upstream's Pub/Sub retryable set: UNKNOWN is transient (HTTP
        // 500s and transport failures often surface as it); CANCELLED is a
        // deliberate abort and must not be replayed.
        retryableCodes: Set<RPCError.Code> = [
            .aborted, .deadlineExceeded, .internalError, .resourceExhausted, .unavailable, .unknown,
        ],
        initialBackoff: Duration = .milliseconds(100),
        maxBackoff: Duration = .seconds(60),
        multiplier: Double = 2.0,
        maxAttempts: Int? = nil,
        maxElapsedTime: Duration? = nil
    ) {
        self.retryableCodes = retryableCodes
        self.initialBackoff = initialBackoff
        self.maxBackoff = maxBackoff
        self.multiplier = multiplier
        self.maxAttempts = maxAttempts
        self.maxElapsedTime = maxElapsedTime
    }

    /// `attempt` is the zero-based index of the attempt that just failed, so
    /// `attempt + 1` attempts have completed. `maxAttempts` bounds the total
    /// number of attempts: `maxAttempts: 1` means a single attempt, no retries.
    public func shouldRetry(code: RPCError.Code?, attempt: Int) -> Bool {
        if let maxAttempts, attempt + 1 >= maxAttempts {
            return false
        }

        guard let code else {
            return false
        }

        return retryableCodes.contains(code)
    }

    /// Full jitter: a uniform sample over (0, computedDelay], so clients that
    /// fail together don't retry in lockstep and re-stampede the service.
    ///
    /// Invalid negative backoffs are treated as zero, and a negative or
    /// non-finite multiplier is treated as `1`, so runtime configuration cannot
    /// turn retry calculation into a trap.
    public func delay(forAttempt attempt: Int) -> Duration {
        let seconds = baseDelaySeconds(forAttempt: attempt)
        guard seconds > 0 else {
            return .zero
        }

        let jittered = Double.random(in: 0...seconds)
        let maximumMilliseconds = Int64.max / 2
        let milliseconds = (jittered * 1_000).rounded(.up)
        guard milliseconds < Double(maximumMilliseconds) else {
            return .milliseconds(maximumMilliseconds)
        }

        return .milliseconds(Int64(milliseconds))
    }

    func baseDelaySeconds(forAttempt attempt: Int) -> Double {
        let initialSeconds = max(.zero, Self.seconds(in: initialBackoff))
        let maxSeconds = max(.zero, Self.seconds(in: maxBackoff))
        let normalizedMultiplier =
            multiplier.isFinite && multiplier >= 0
            ? multiplier
            : 1
        let cappedAttempts = attempt > 1 ? min(attempt - 1, 16) : 0
        let scaled = initialSeconds * pow(normalizedMultiplier, Double(cappedAttempts))
        let finiteScaled = scaled.isFinite ? scaled : maxSeconds
        return min(max(finiteScaled, .zero), maxSeconds)
    }

    /// Fills only unset budget fields, so explicit user configuration always
    /// wins over a call site's default budget.
    func withDefaultBudget(maxAttempts: Int? = nil, maxElapsedTime: Duration? = nil) -> RetryPolicy
    {
        var copy = self
        copy.maxAttempts = copy.maxAttempts ?? maxAttempts
        copy.maxElapsedTime = copy.maxElapsedTime ?? maxElapsedTime
        return copy
    }

    private static func seconds(in duration: Duration) -> Double {
        Double(duration.components.seconds)
            + Double(duration.components.attoseconds) / 1e18
    }
}

public struct ClientConfiguration: Sendable {
    public var endpoint: String?
    public var emulatorHost: String?
    public var credentialsMode: CredentialsMode
    private var validatedCallTimeout: Duration
    private var validatedGRPCSubchannelCount: Int
    public var retryPolicy: RetryPolicy

    /// Unary RPC timeout. Non-positive values are normalized to a positive floor
    /// so public mutation cannot create a deadline that traps in the transport.
    public var callTimeout: Duration {
        get { validatedCallTimeout }
        set { validatedCallTimeout = max(.milliseconds(1), newValue) }
    }

    /// Number of live gRPC clients in the connection pool. Values below one are
    /// normalized because an empty pool cannot service requests safely.
    public var grpcSubchannelCount: Int {
        get { validatedGRPCSubchannelCount }
        set { validatedGRPCSubchannelCount = max(1, newValue) }
    }

    public init(
        endpoint: String? = nil,
        emulatorHost: String? = nil,
        credentialsMode: CredentialsMode = .automatic,
        callTimeout: Duration = .seconds(60),
        grpcSubchannelCount: Int = 1,
        retryPolicy: RetryPolicy = RetryPolicy()
    ) {
        self.endpoint = endpoint
        self.emulatorHost = emulatorHost
        self.credentialsMode = credentialsMode
        self.validatedCallTimeout = max(.milliseconds(1), callTimeout)
        self.validatedGRPCSubchannelCount = max(1, grpcSubchannelCount)
        self.retryPolicy = retryPolicy
    }
}

/// Shared fluent configuration setters for every client builder, so each
/// builder exposes the same knobs without duplicating the implementations.
public protocol ConfigurableClientBuilder: Sendable {
    var configuration: ClientConfiguration { get set }
}

extension ConfigurableClientBuilder {
    public func withEndpoint(_ endpoint: String) -> Self {
        var copy = self
        copy.configuration.endpoint = endpoint
        return copy
    }

    public func useEmulator(host: String) -> Self {
        var copy = self
        copy.configuration.emulatorHost = host
        return copy
    }

    public func withCredentials(_ authorization: Authorization) -> Self {
        var copy = self
        copy.configuration.credentialsMode = .custom(authorization)
        return copy
    }

    public func useAnonymousCredentials() -> Self {
        var copy = self
        copy.configuration.credentialsMode = .anonymous
        return copy
    }

    public func withRetryPolicy(_ retryPolicy: RetryPolicy) -> Self {
        var copy = self
        copy.configuration.retryPolicy = retryPolicy
        return copy
    }

    public func withCallTimeout(_ timeout: Duration) -> Self {
        var copy = self
        copy.configuration.callTimeout = timeout
        return copy
    }

    public func withGRPCSubchannelCount(_ count: Int) -> Self {
        var copy = self
        copy.configuration.grpcSubchannelCount = max(1, count)
        return copy
    }
}
