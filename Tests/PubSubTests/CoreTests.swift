import Auth
import Foundation
import GRPCCore
import GRPCProtobuf
import Testing

@testable import PubSub

private actor Counter {
    private(set) var value = 0

    @discardableResult
    func increment() -> Int {
        value += 1
        return value
    }
}

private actor ShutdownTrackingProvider: Provider {
    private(set) var shutdownCalls = 0

    func createSession(scopes: [Scope]) async throws -> Session {
        Session(accessToken: "token", expiration: .never)
    }

    func shutdown() async throws {
        shutdownCalls += 1
    }
}

private final class QueueElement: @unchecked Sendable {}

@Suite("Core")
struct CoreTests {
    @Test("ServiceConnection creates the configured gRPC subchannel pool")
    func serviceConnectionUsesConfiguredSubchannelCount() async throws {
        let connection = try ServiceConnection(
            configuration: ClientConfiguration(
                endpoint: "http://localhost:1",
                credentialsMode: .anonymous,
                grpcSubchannelCount: 3
            ),
            environment: [:]
        )

        #expect(await connection.grpcClientCount == 3)
        await connection.shutdown()
    }

    @Test("ServiceConnection configures Pub/Sub payload limits and caps retry timeouts")
    func serviceConnectionCallOptions() async throws {
        let connection = try ServiceConnection(
            configuration: ClientConfiguration(
                endpoint: "http://localhost:1",
                credentialsMode: .anonymous,
                callTimeout: .seconds(30)
            ),
            environment: [:]
        )

        let options = connection.callOptions(timeout: .seconds(60))
        #expect(options.timeout == .seconds(30))
        // grpc-swift-nio-transport 2.9 reads this field for its inbound deframer;
        // publisher admission separately enforces the 10 MB outbound limit.
        #expect(options.maxRequestMessageBytes == 20_000_000)
        #expect(options.maxResponseMessageBytes == 20_000_000)

        let streamingOptions = connection.streamingCallOptions()
        #expect(streamingOptions.timeout == nil)
        #expect(streamingOptions.maxRequestMessageBytes == 20_000_000)
        #expect(streamingOptions.maxResponseMessageBytes == 20_000_000)
        await connection.shutdown()
    }

    @Test(
        "ServiceConnection rejects malformed custom endpoints",
        arguments: [
            "https://",
            "host:not-a-port",
            "ftp://localhost:21",
            "https://localhost/path",
            "https://localhost:70000",
        ]
    )
    func serviceConnectionRejectsMalformedEndpoint(_ endpoint: String) {
        #expect(throws: PubSubServiceError.self) {
            _ = try ServiceConnection(
                configuration: ClientConfiguration(
                    endpoint: endpoint,
                    credentialsMode: .anonymous
                ),
                environment: [:]
            )
        }
    }

    @Test(
        "ServiceConnection rejects malformed emulator hosts",
        arguments: [
            "localhost",
            "localhost:not-a-port",
            "localhost:-1",
            "localhost:70000",
            "localhost:0",
            "localhost:8085/path",
            "user:password@localhost:8085",
        ]
    )
    func serviceConnectionRejectsMalformedEmulatorHost(_ emulatorHost: String) {
        #expect(throws: PubSubServiceError.self) {
            _ = try ServiceConnection(
                configuration: ClientConfiguration(emulatorHost: emulatorHost),
                environment: [:]
            )
        }
    }

    @Test("Routing metadata carries the resource fields Pub/Sub routes on")
    func routingMetadataFormat() {
        let single = ServiceConnection.routingMetadata([
            "subscription": "projects/p/subscriptions/s"
        ])
        #expect(
            Array(single[stringValues: "x-goog-request-params"])
                == ["subscription=projects%2Fp%2Fsubscriptions%2Fs"])

        let nested = ServiceConnection.routingMetadata(["topic.name": "projects/p/topics/t"])
        #expect(
            Array(nested[stringValues: "x-goog-request-params"])
                == ["topic.name=projects%2Fp%2Ftopics%2Ft"])

        let reserved = ServiceConnection.routingMetadata([
            "name": "projects/p/topics/a+b%&= café"
        ])
        #expect(
            Array(reserved[stringValues: "x-goog-request-params"])
                == ["name=projects%2Fp%2Ftopics%2Fa%2Bb%25%26%3D%20caf%C3%A9"])
    }

    @Test("ServiceConnection accepts a well-formed emulator host")
    func serviceConnectionAcceptsEmulatorHost() async throws {
        let connection = try ServiceConnection(
            configuration: ClientConfiguration(emulatorHost: "localhost:8085"),
            environment: [:]
        )
        await connection.shutdown()
    }

    @Test("An empty PUBSUB_EMULATOR_HOST is treated as unset")
    func emptyEmulatorEnvironmentFallsBackToConfiguredEndpoint() async throws {
        let connection = try ServiceConnection(
            configuration: ClientConfiguration(
                endpoint: "http://localhost:1",
                credentialsMode: .anonymous
            ),
            environment: ["PUBSUB_EMULATOR_HOST": ""]
        )
        await connection.shutdown()
    }

    @Test("Client configuration normalizes invalid values after public mutation")
    func clientConfigurationMutationRemainsValid() {
        var configuration = ClientConfiguration()
        configuration.callTimeout = .zero
        configuration.grpcSubchannelCount = 0

        #expect(configuration.callTimeout == .milliseconds(1))
        #expect(configuration.grpcSubchannelCount == 1)
    }

    @Test("ServiceConnection leaves caller-owned authorization running")
    func customAuthorizationRemainsCallerOwned() async throws {
        let provider = ShutdownTrackingProvider()
        let authorization = Authorization(scopes: [], provider: provider)
        let connection = try ServiceConnection(
            configuration: ClientConfiguration(
                endpoint: "http://localhost:1",
                credentialsMode: .custom(authorization)
            ),
            environment: [:]
        )

        await connection.shutdown()
        #expect(await provider.shutdownCalls == 0)

        try await authorization.shutdown()
        #expect(await provider.shutdownCalls == 1)
    }

    @Test("Public admin builders construct all service clients")
    func publicAdminBuildersConstructClients() async throws {
        let configuration = ClientConfiguration(
            emulatorHost: "localhost:1",
            credentialsMode: .anonymous
        )
        let topicAdmin = try await TopicAdminBuilder(configuration: configuration).build()
        let subscriptionAdmin = try await SubscriptionAdminBuilder(configuration: configuration)
            .build()
        let schemaService = try await SchemaServiceBuilder(configuration: configuration).build()

        await topicAdmin.shutdown()
        await subscriptionAdmin.shutdown()
        await schemaService.shutdown()
    }

    @Test("RetryPolicy maxAttempts bounds total attempts, not retries")
    func retryPolicyMaxAttemptsIsTotalAttempts() {
        let singleAttempt = RetryPolicy(maxAttempts: 1)
        #expect(singleAttempt.shouldRetry(code: .unavailable, attempt: 0) == false)

        let twoAttempts = RetryPolicy(maxAttempts: 2)
        #expect(twoAttempts.shouldRetry(code: .unavailable, attempt: 0) == true)
        #expect(twoAttempts.shouldRetry(code: .unavailable, attempt: 1) == false)
    }

    @Test("Default retryable codes treat UNKNOWN as transient and CANCELLED as permanent")
    func retryPolicyDefaultCodes() {
        let policy = RetryPolicy()
        #expect(policy.shouldRetry(code: .unknown, attempt: 0) == true)
        #expect(policy.shouldRetry(code: .cancelled, attempt: 0) == false)
    }

    @Test("Backoff delays are jittered within the configured cap")
    func backoffDelaysJitteredWithinCap() {
        let policy = RetryPolicy(
            initialBackoff: .milliseconds(100),
            maxBackoff: .seconds(1),
            multiplier: 2.0
        )
        for attempt in 0...20 {
            let delay = policy.delay(forAttempt: attempt)
            #expect(delay >= .zero)
            #expect(delay <= .seconds(1) + .milliseconds(1))
        }
    }

    @Test(
        "Backoff normalizes invalid runtime configuration",
        arguments: [
            PubSub.RetryPolicy(initialBackoff: .seconds(-1)),
            PubSub.RetryPolicy(maxBackoff: .seconds(-1)),
            PubSub.RetryPolicy(multiplier: -1),
            PubSub.RetryPolicy(multiplier: .nan),
            PubSub.RetryPolicy(multiplier: .infinity),
            PubSub.RetryPolicy(
                initialBackoff: .seconds(Int64.max),
                maxBackoff: .seconds(Int64.max),
                multiplier: 2
            ),
        ]
    )
    func backoffNormalizesInvalidConfiguration(_ policy: PubSub.RetryPolicy) {
        let delay = policy.delay(forAttempt: Int.max)
        #expect(delay >= .zero)
        #expect(delay <= .milliseconds(Int64.max / 2))
    }

    @Test("Non-idempotent operations retry only on UNAVAILABLE")
    func nonIdempotentRetriesOnlyOnUnavailable() async throws {
        let abortedCalls = Counter()
        await #expect(throws: PubSubServiceError.self) {
            let _: Int = try await withRetry(policy: RetryPolicy(), idempotent: false) {
                await abortedCalls.increment()
                throw PubSubServiceError(code: .aborted, message: "conflict")
            }
        }
        #expect(await abortedCalls.value == 1)

        let unavailableCalls = Counter()
        let result: String = try await withRetry(policy: RetryPolicy(), idempotent: false) {
            if await unavailableCalls.increment() < 3 {
                throw PubSubServiceError(code: .unavailable, message: "down")
            }
            return "ok"
        }
        #expect(result == "ok")
        #expect(await unavailableCalls.value == 3)
    }

    @Test("Retry stops once the elapsed-time budget is exhausted")
    func retryStopsAtElapsedTimeBudget() async throws {
        let calls = Counter()
        let policy = RetryPolicy(
            initialBackoff: .milliseconds(30),
            maxBackoff: .milliseconds(30),
            multiplier: 1.0,
            maxElapsedTime: .milliseconds(90)
        )
        await #expect(throws: PubSubServiceError.self) {
            let _: Int = try await withRetry(policy: policy) {
                await calls.increment()
                throw PubSubServiceError(code: .unavailable, message: "down")
            }
        }
        // The loop terminated (the expectation above resolved) and retried at
        // least once before the budget cut it off.
        #expect(await calls.value >= 2)
    }

    @Test("Retry passes the remaining elapsed-time budget to each attempt")
    func retryPassesRemainingBudget() async throws {
        let budget = Duration.seconds(1)
        let capturedRemainingTime: Duration? = try await withRetry(
            policy: RetryPolicy(maxElapsedTime: budget)
        ) { remainingTime in
            remainingTime
        }

        let remainingTime = try #require(capturedRemainingTime)
        #expect(remainingTime > .zero)
        #expect(remainingTime <= budget)
    }

    @Test("Errors without an RPC status map to a transient UNKNOWN code")
    func unclassifiedErrorsMapToUnknown() {
        struct TransportTeardown: Error {}
        #expect(asServiceError(TransportTeardown()).code == .unknown)
        #expect(asServiceError(CancellationError()).code == .cancelled)
    }

    @Test("Wrapped cancellation and credential errors preserve useful status")
    func wrappedErrorsPreserveCauseSemantics() {
        let cancellation = asServiceError(
            RPCError(code: .unknown, message: "", cause: CancellationError()))
        #expect(cancellation.code == .cancelled)
        #expect(cancellation.message == "cancelled")

        let credentials = asServiceError(
            RPCError(
                code: .unknown,
                message: "",
                cause: CredentialsError.parsing("missing private key")
            )
        )
        #expect(credentials.code == .unauthenticated)
        #expect(credentials.message.contains("missing private key"))
    }

    @Test("RPC errors preserve structured Google ErrorInfo")
    func rpcErrorsPreserveStructuredErrorInfo() {
        let rpcError = RPCError(
            GoogleRPCStatus(
                code: .failedPrecondition,
                message: "ack failed",
                details: .errorInfo(
                    reason: "EXACTLY_ONCE_ACK_FAILURE",
                    domain: "pubsub.googleapis.com",
                    metadata: ["ack-1": "PERMANENT_FAILURE_INVALID_ACK_ID"]
                )
            )
        )

        let serviceError = asServiceError(rpcError)
        let expectedInfo = [
            PubSubErrorInfo(
                reason: "EXACTLY_ONCE_ACK_FAILURE",
                domain: "pubsub.googleapis.com",
                metadata: ["ack-1": "PERMANENT_FAILURE_INVALID_ACK_ID"]
            )
        ]
        #expect(serviceError.errorInfo == expectedInfo)
        // Decoding is deferred to the first read, so it has to be stable across
        // reads and has to leave equality with an explicitly built error intact.
        #expect(serviceError.errorInfo == expectedInfo)
        #expect(
            serviceError
                == PubSubServiceError(
                    code: .failedPrecondition,
                    message: "ack failed",
                    errorInfo: expectedInfo
                )
        )
    }

    @Test("Errors without status details decode to empty ErrorInfo")
    func rpcErrorsWithoutDetailsHaveNoErrorInfo() {
        let serviceError = asServiceError(
            RPCError(code: .unavailable, message: "connection reset"))

        #expect(serviceError.code == .unavailable)
        #expect(serviceError.errorInfo.isEmpty)
        #expect(serviceError == PubSubServiceError(code: .unavailable, message: "connection reset"))
    }

    @Test("Cancelling a task awaiting AsyncThrowingQueue.next() resumes it")
    func queueWaiterCancellation() async throws {
        let queue = AsyncThrowingQueue<Int>()
        let waiter = Task {
            try await queue.next()
        }

        try await Task.sleep(for: .milliseconds(50))
        waiter.cancel()

        let result = await waiter.result
        #expect(throws: CancellationError.self) {
            _ = try result.get()
        }

        // The cancelled waiter must not steal the next yielded element.
        await queue.yield(7)
        #expect(try await queue.next() == 7)
    }

    @Test("Cancelling a task awaiting AsyncValue.value() resumes it")
    func valueWaiterCancellation() async throws {
        let value = AsyncValue<Int, PubSubServiceError>()
        let waiter = Task {
            try await value.value()
        }

        try await Task.sleep(for: .milliseconds(50))
        waiter.cancel()

        let result = await waiter.result
        #expect(throws: CancellationError.self) {
            _ = try result.get()
        }

        await value.succeed(9)
        #expect(try await value.value() == 9)
    }

    @Test("Finishing a queue returns the undelivered buffered elements")
    func queueFinishReturnsUndelivered() async throws {
        let queue = AsyncThrowingQueue<Int>()
        await queue.yield(1)
        await queue.yield(2)
        #expect(try await queue.next() == 1)

        let undelivered = await queue.finish()
        #expect(undelivered == [2])
        #expect(try await queue.next() == nil)
    }

    @Test("Dequeuing releases the consumed queue slot immediately")
    func queueReleasesDequeuedElement() async throws {
        let queue = AsyncThrowingQueue<QueueElement>()
        var element: QueueElement? = QueueElement()
        weak let weakElement = element
        await queue.yield(try #require(element))
        element = nil

        var dequeued: QueueElement? = try await queue.next()
        #expect(weakElement != nil)
        withExtendedLifetime(dequeued) {}
        dequeued = nil
        #expect(weakElement == nil)
    }

    @Test("Publisher message byte count matches SwiftProtobuf without serializing")
    func publisherMessageByteCountIsExact() throws {
        var message = Message(
            data: Data(repeating: 0xAB, count: 300),
            attributes: ["": "", "key": "value"],
            orderingKey: "ordered"
        )
        message.messageID = "server-id"
        message.publishTime.seconds = -1
        message.publishTime.nanos = 123_456_789

        let serializedCount = try message.serializedData().count
        #expect(message.serializedByteCount == serializedCount)
    }

    @Test("Client payload validation enforces the 10 MB outbound limit")
    func clientPayloadLimitIsEnforced() throws {
        var request = Google_Pubsub_V1_PublishRequest()
        request.topic = "projects/p/topics/t"
        request.messages = [Message(data: Data(repeating: 0, count: 300))]
        let byteCount = request.serializedByteCount

        try PayloadLimitInterceptor.validate(request, maximumBytes: byteCount)
        #expect(throws: RPCError.self) {
            try PayloadLimitInterceptor.validate(request, maximumBytes: byteCount - 1)
        }
    }
}
