import Foundation
import Logging

private let retryLogger = Logger(label: "google-cloud-swift.pubsub.retry")

/// Runs `operation`, retrying failures whose mapped code is retryable under
/// `policy`, with the policy's jittered exponential backoff between attempts
/// bounded by its `maxAttempts`/`maxElapsedTime` budget.
///
/// Non-idempotent operations follow AIP-194: they are retried only on
/// UNAVAILABLE (the request never reached the server), never on ambiguous
/// failures that may have committed server-side.
///
/// Errors are normalized to `PubSubServiceError` before being rethrown, so
/// callers see a single error vocabulary. Task cancellation is never retried
/// and propagates as `CancellationError`.
func withRetry<T: Sendable>(
    policy: RetryPolicy,
    idempotent: Bool = true,
    operation: @Sendable (Duration?) async throws -> T
) async throws -> T {
    let clock = ContinuousClock()
    let start = clock.now
    let deadline = policy.maxElapsedTime.map { start.advanced(by: $0) }
    var attempt = 0

    while true {
        try Task.checkCancellation()
        let remainingTime = deadline.map { max(.zero, clock.now.duration(to: $0)) }
        if attempt > 0, let remainingTime, remainingTime <= .zero {
            throw PubSubServiceError(
                code: .deadlineExceeded, message: "retry time budget exhausted")
        }

        do {
            return try await operation(remainingTime)
        } catch let error as CancellationError {
            throw error
        } catch {
            let serviceError = asServiceError(error)
            guard policy.shouldRetry(code: serviceError.code, attempt: attempt),
                idempotent || serviceError.code == .unavailable
            else {
                retryLogger.error(
                    "RPC failed without retry",
                    metadata: ["attempt": "\(attempt + 1)", "error": "\(serviceError)"]
                )
                throw serviceError
            }

            attempt += 1
            let delay = policy.delay(forAttempt: attempt)
            if let deadline {
                let remainingTime = clock.now.duration(to: deadline)
                guard remainingTime > .zero, delay < remainingTime else {
                    throw serviceError
                }
            }

            retryLogger.debug(
                "RPC failed; retrying",
                metadata: ["attempt": "\(attempt)", "error": "\(serviceError)"]
            )

            try await Task.sleep(for: delay)
        }
    }
}

func withRetry<T: Sendable>(
    policy: RetryPolicy,
    idempotent: Bool = true,
    operation: @Sendable () async throws -> T
) async throws -> T {
    try await withRetry(policy: policy, idempotent: idempotent) { _ in
        try await operation()
    }
}
