import Foundation
import Testing

@testable import PubSub

private actor FakePublisherService: PublisherRPC {
    enum Outcome: Sendable {
        case success(@Sendable ([Message]) -> [String])
        case gated(
            started: AsyncValue<Void, PubSubServiceError>,
            release: AsyncValue<Void, PubSubServiceError>,
            transform: @Sendable ([Message]) -> [String]
        )
        case failure(PubSubServiceError)
    }

    private(set) var publishedBatches: [[Message]] = []
    private(set) var shutdownCalls = 0
    private var outcomes: [Outcome]

    init(outcomes: [Outcome]) {
        self.outcomes = outcomes
    }

    func shutdown() async {
        shutdownCalls += 1
    }

    func publish(topic: String, messages: [Message], timeout: Duration?) async throws -> [String] {
        publishedBatches.append(messages)
        guard !outcomes.isEmpty else {
            return messages.map { String(decoding: $0.data, as: UTF8.self) }
        }

        let outcome = outcomes.removeFirst()
        switch outcome {
        case .success(let closure):
            return closure(messages)
        case .gated(let started, let release, let closure):
            await started.succeed(())
            try await release.value()
            return closure(messages)
        case .failure(let error):
            throw error
        }
    }
}

private actor CompletionFlag {
    private var completed = false

    func markCompleted() {
        completed = true
    }

    var isCompleted: Bool {
        completed
    }
}

@Suite("Publisher")
struct PublisherTests {
    @Test("Batching options defaults and setters match the upstream semantics")
    func batchingOptions() {
        let options = BatchingOptions()
            .setByteThreshold(1_234)
            .setMessageCountThreshold(123)
            .setDelayThreshold(.milliseconds(12))

        #expect(options.byteThreshold == 1_234)
        #expect(options.messageCountThreshold == 123)
        #expect(options.delayThreshold == .milliseconds(12))
    }

    @Test("Publisher batches messages when the threshold is reached")
    func publisherPublishSuccessfully() async throws {
        let service = FakePublisherService(outcomes: [
            .success { messages in
                messages.map { String(decoding: $0.data, as: UTF8.self) }
            }
        ])

        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        )
        .setMessageCountThreshold(2)
        .build()

        let first = publisher.publish(Message(data: Data("hello".utf8)))
        let second = publisher.publish(Message(data: Data("world".utf8)))

        #expect(try await first.value() == "hello")
        #expect(try await second.value() == "world")
        #expect(await service.publishedBatches.count == 1)
        #expect(await service.publishedBatches.first?.count == 2)
    }

    @Test("Flush includes a message published immediately before the flush call")
    func flushSeesImmediatelyPriorPublish() async throws {
        let service = FakePublisherService(outcomes: [
            .success { messages in
                messages.map { String(decoding: $0.data, as: UTF8.self) }
            }
        ])

        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        )
        .setMessageCountThreshold(100)
        .setDelayThreshold(.seconds(60))
        .build()

        let published = publisher.publish(Message(data: Data("flush".utf8)))
        await publisher.flush()

        #expect(await service.publishedBatches.count == 1)
        #expect(try await published.value() == "flush")
    }

    @Test("Flush waits for an oversized message to be rejected locally")
    func flushWaitsForLocalRejection() async {
        let service = FakePublisherService(outcomes: [])
        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        ).build()

        let rejected = publisher.publish(
            Message(data: Data(repeating: 0, count: BatchingOptions.maxBytes)))
        await publisher.flush()

        await #expect(throws: PublishError.self) {
            try await rejected.value()
        }
        #expect(await service.publishedBatches.isEmpty)
    }

    @Test("Flush waits for an immediately started in-flight batch")
    func flushWaitsForInFlightBatch() async throws {
        let started = AsyncValue<Void, PubSubServiceError>()
        let release = AsyncValue<Void, PubSubServiceError>()
        let service = FakePublisherService(outcomes: [
            .gated(
                started: started, release: release,
                transform: { messages in
                    messages.map { String(decoding: $0.data, as: UTF8.self) }
                })
        ])
        let completionFlag = CompletionFlag()

        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        )
        .setMessageCountThreshold(1)
        .build()

        let published = publisher.publish(Message(data: Data("in-flight".utf8)))
        let flushTask = Task {
            await publisher.flush()
            await completionFlag.markCompleted()
        }

        try await started.value()
        try await Task.sleep(for: .milliseconds(20))
        #expect(await completionFlag.isCompleted == false)

        await release.succeed(())
        await flushTask.value

        #expect(await completionFlag.isCompleted)
        #expect(try await published.value() == "in-flight")
    }

    @Test("Publisher shutdown flushes buffered messages")
    func publisherShutdownFlushesBufferedMessages() async throws {
        let service = FakePublisherService(outcomes: [
            .success { messages in
                messages.map { String(decoding: $0.data, as: UTF8.self) }
            }
        ])

        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        )
        .setMessageCountThreshold(100)
        .setDelayThreshold(.seconds(60))
        .build()

        let published = publisher.publish(Message(data: Data("shutdown".utf8)))
        await publisher.shutdown()

        #expect(await service.publishedBatches.count == 1)
        #expect(try await published.value() == "shutdown")
    }

    @Test("BasePublisher drains derived publishers before closing the shared service")
    func basePublisherShutdownDrainsDerivedPublishers() async throws {
        let service = FakePublisherService(outcomes: [])
        let basePublisher = BasePublisher(service: service, configuration: ClientConfiguration())
        let publisher = basePublisher.publisher("projects/p/topics/t")
            .setMessageCountThreshold(100)
            .setDelayThreshold(.seconds(60))
            .build()
        let published = publisher.publish(Message(data: Data("buffered".utf8)))

        await basePublisher.shutdown()

        #expect(try await published.value() == "buffered")
        #expect(await service.publishedBatches.count == 1)
        #expect(await service.shutdownCalls == 1)
    }

    @Test("Cancelled shutdown still drains before closing its service")
    func cancelledShutdownStillDrains() async throws {
        let started = AsyncValue<Void, PubSubServiceError>()
        let release = AsyncValue<Void, PubSubServiceError>()
        let service = FakePublisherService(outcomes: [
            .gated(
                started: started,
                release: release,
                transform: { messages in
                    messages.map { String(decoding: $0.data, as: UTF8.self) }
                }
            )
        ])
        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration(),
            ownsService: true
        )
        .setMessageCountThreshold(100)
        .setDelayThreshold(.seconds(60))
        .build()
        let published = publisher.publish(Message(data: Data("shutdown".utf8)))

        let shutdownTask = Task {
            withUnsafeCurrentTask { task in
                task?.cancel()
            }
            await publisher.shutdown()
        }
        try await started.value()
        #expect(await service.shutdownCalls == 0)

        await release.succeed(())
        await shutdownTask.value

        #expect(try await published.value() == "shutdown")
        #expect(await service.shutdownCalls == 1)
    }

    @Test("Publish futures retain buffered dispatcher work", .timeLimit(.minutes(1)))
    func publishFutureSurvivesPublisherRelease() async throws {
        let service = FakePublisherService(outcomes: [])
        var publisher: Publisher? = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        )
        .setMessageCountThreshold(100)
        .setDelayThreshold(.milliseconds(10))
        .build()

        let published = publisher?.publish(Message(data: Data("retained".utf8)))
        publisher = nil

        let future = try #require(published)
        #expect(try await future.value() == "retained")
    }

    @Test("Ordering keys pause after a failed publish and can be resumed")
    func orderingKeyPausedAfterFailure() async throws {
        let service = FakePublisherService(outcomes: [
            .failure(PubSubServiceError(code: .unavailable, message: "temporary failure")),
            .success { messages in
                messages.map { String(decoding: $0.data, as: UTF8.self) }
            },
        ])

        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration(
                retryPolicy: RetryPolicy(retryableCodes: [], maxAttempts: 1))
        )
        .setMessageCountThreshold(1)
        .build()

        var failedMessage = Message(data: Data("first".utf8))
        failedMessage.orderingKey = "ordered"
        let failed = publisher.publish(failedMessage)

        await #expect(throws: PublishError.self) {
            _ = try await failed.value()
        }

        var pausedMessage = Message(data: Data("second".utf8))
        pausedMessage.orderingKey = "ordered"
        let paused = publisher.publish(pausedMessage)

        // Resume immediately: the publish is committed to ingress synchronously,
        // so it must still be rejected while the key is paused.
        await publisher.resumePublish("ordered")
        await #expect(throws: PublishError.self) {
            _ = try await paused.value()
        }

        var resumedMessage = Message(data: Data("third".utf8))
        resumedMessage.orderingKey = "ordered"
        let resumed = publisher.publish(resumedMessage)
        #expect(try await resumed.value() == "third")
        #expect(await service.publishedBatches.count == 2)
    }

    @Test(
        "Ordered backlog accumulated while a batch is in flight is re-chunked to the batch limits")
    func pendingAccumulatedInFlightIsChunked() async throws {
        let started = AsyncValue<Void, PubSubServiceError>()
        let release = AsyncValue<Void, PubSubServiceError>()
        let service = FakePublisherService(outcomes: [
            .gated(
                started: started, release: release,
                transform: { messages in
                    messages.map { String(decoding: $0.data, as: UTF8.self) }
                })
        ])

        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        )
        .setMessageCountThreshold(100)
        .build()

        var firstMessage = Message(data: Data("0".utf8))
        firstMessage.orderingKey = "k"
        let first = publisher.publish(firstMessage)
        // The 10ms default delay threshold flushes the first message alone.
        try await started.value()

        var futures: [PublishFuture] = []
        for index in 1...250 {
            var message = Message(data: Data("\(index)".utf8))
            message.orderingKey = "k"
            futures.append(publisher.publish(message))
        }
        await release.succeed(())

        #expect(try await first.value() == "0")
        for future in futures {
            _ = try await future.value()
        }

        let batches = await service.publishedBatches
        #expect(batches.reduce(0) { $0 + $1.count } == 251)
        // Backlog drains in configured-threshold chunks, not API-limit chunks.
        #expect(batches.allSatisfy { $0.count <= 100 })
    }

    @Test("Flush bypasses the delay for every remaining ordered chunk")
    func orderedFlushBypassesDelayForRemainder() async throws {
        let started = AsyncValue<Void, PubSubServiceError>()
        let release = AsyncValue<Void, PubSubServiceError>()
        let service = FakePublisherService(outcomes: [
            .gated(
                started: started,
                release: release,
                transform: { messages in
                    messages.map { String(decoding: $0.data, as: UTF8.self) }
                }
            )
        ])
        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        )
        .setMessageCountThreshold(2)
        .setDelayThreshold(.seconds(2))
        .build()

        func orderedMessage(_ value: String) -> Message {
            var message = Message(data: Data(value.utf8))
            message.orderingKey = "key"
            return message
        }

        var futures = [
            publisher.publish(orderedMessage("1")),
            publisher.publish(orderedMessage("2")),
        ]
        try await started.value()
        futures.append(contentsOf: [
            publisher.publish(orderedMessage("3")),
            publisher.publish(orderedMessage("4")),
            publisher.publish(orderedMessage("5")),
        ])

        let flushTask = Task { await publisher.flush() }
        await release.succeed(())

        for _ in 0..<100 where await service.publishedBatches.count < 3 {
            try await Task.sleep(for: .milliseconds(10))
        }
        let batchCountBeforeDelay = await service.publishedBatches.count
        #expect(batchCountBeforeDelay == 3)

        // Reissuing shutdown makes cleanup bounded even if this regression returns.
        if batchCountBeforeDelay < 3 {
            await publisher.shutdown()
        }
        await flushTask.value
        for future in futures {
            _ = try await future.value()
        }
    }

    @Test("Unordered batches publish concurrently")
    func unorderedBatchesPublishConcurrently() async throws {
        let started1 = AsyncValue<Void, PubSubServiceError>()
        let release1 = AsyncValue<Void, PubSubServiceError>()
        let started2 = AsyncValue<Void, PubSubServiceError>()
        let release2 = AsyncValue<Void, PubSubServiceError>()
        let service = FakePublisherService(outcomes: [
            .gated(
                started: started1, release: release1,
                transform: { messages in
                    messages.map { String(decoding: $0.data, as: UTF8.self) }
                }),
            .gated(
                started: started2, release: release2,
                transform: { messages in
                    messages.map { String(decoding: $0.data, as: UTF8.self) }
                }),
        ])

        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        )
        .setMessageCountThreshold(1)
        .build()

        let first = publisher.publish(Message(data: Data("a".utf8)))
        try await started1.value()

        // The second batch must go in flight while the first is still gated.
        let second = publisher.publish(Message(data: Data("b".utf8)))
        try await started2.value()

        await release2.succeed(())
        #expect(try await second.value() == "b")

        await release1.succeed(())
        #expect(try await first.value() == "a")
    }

    @Test("Ordered batches stay serialized per key")
    func orderedBatchesRemainSequential() async throws {
        let started = AsyncValue<Void, PubSubServiceError>()
        let release = AsyncValue<Void, PubSubServiceError>()
        let service = FakePublisherService(outcomes: [
            .gated(
                started: started, release: release,
                transform: { messages in
                    messages.map { String(decoding: $0.data, as: UTF8.self) }
                })
        ])

        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        )
        .setMessageCountThreshold(1)
        .build()

        var firstMessage = Message(data: Data("a".utf8))
        firstMessage.orderingKey = "k"
        var secondMessage = Message(data: Data("b".utf8))
        secondMessage.orderingKey = "k"

        let first = publisher.publish(firstMessage)
        try await started.value()

        let second = publisher.publish(secondMessage)
        try await Task.sleep(for: .milliseconds(50))
        #expect(await service.publishedBatches.count == 1)

        await release.succeed(())
        #expect(try await first.value() == "a")
        #expect(try await second.value() == "b")
    }

    @Test("Publish after shutdown fails fast with .shutdown")
    func publishAfterShutdownFailsFast() async throws {
        let service = FakePublisherService(outcomes: [])
        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        ).build()

        await publisher.shutdown()

        let late = publisher.publish(Message(data: Data("late".utf8)))
        await #expect(throws: PublishError.self) {
            _ = try await late.value()
        }
        #expect(await service.publishedBatches.isEmpty)
    }

    @Test("A response with missing message IDs fails the batch instead of hanging")
    func shortIDResponseFailsFutures() async throws {
        let service = FakePublisherService(outcomes: [
            .success { _ in ["only-one-id"] }
        ])

        let publisher = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration(
                retryPolicy: RetryPolicy(retryableCodes: [], maxAttempts: 1))
        )
        .setMessageCountThreshold(2)
        .build()

        let first = publisher.publish(Message(data: Data("a".utf8)))
        let second = publisher.publish(Message(data: Data("b".utf8)))

        await #expect(throws: PublishError.self) {
            _ = try await first.value()
        }
        await #expect(throws: PublishError.self) {
            _ = try await second.value()
        }
    }

    @Test("Per-topic publisher shutdown leaves a shared service running")
    func sharedServiceSurvivesPublisherShutdown() async throws {
        let service = FakePublisherService(outcomes: [])

        let shared = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration()
        ).build()
        await shared.shutdown()
        #expect(await service.shutdownCalls == 0)

        let owned = PublisherPartialBuilder(
            service: service,
            topic: "projects/p/topics/t",
            configuration: ClientConfiguration(),
            ownsService: true
        ).build()
        await owned.shutdown()
        #expect(await service.shutdownCalls == 1)
    }
}
