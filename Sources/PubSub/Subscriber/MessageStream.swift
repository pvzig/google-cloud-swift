import Foundation
import Logging
import Synchronization

private enum StreamControl: Error {
    case stop
}

private actor StreamRetryState {
    enum Decision: Sendable {
        case retry(after: Duration, attempt: Int)
        case stop
    }

    private let clock = ContinuousClock()
    private var attempt = 0
    private var failureWindowStart: ContinuousClock.Instant?

    func connected() {
        attempt = 0
        failureWindowStart = nil
    }

    func decision(after error: PubSubServiceError, policy: RetryPolicy) -> Decision {
        let failureTime = clock.now
        let windowStart = failureWindowStart ?? failureTime
        failureWindowStart = windowStart
        guard policy.shouldRetry(code: error.code, attempt: attempt),
            policy.maxElapsedTime.map({ failureTime - windowStart < $0 }) ?? true
        else {
            return .stop
        }

        attempt += 1
        let delay = policy.delay(forAttempt: attempt)
        if let maxElapsedTime = policy.maxElapsedTime {
            let remainingTime = maxElapsedTime - (clock.now - windowStart)
            guard remainingTime > .zero, delay < remainingTime else {
                return .stop
            }
        }
        return .retry(after: delay, attempt: attempt)
    }
}

actor ShutdownState {
    private var requested = false
    private let finished = AsyncValue<Void, Never>()

    func requestShutdown() {
        requested = true
    }

    func isShutdownRequested() -> Bool {
        requested
    }

    func markFinished() async {
        await finished.succeed(())
    }

    func waitForFinish() async {
        _ = await finished.terminalResult()
    }
}

public final class ShutdownToken: @unchecked Sendable {
    private struct WorkerState: Sendable {
        var task: Task<Void, Never>?
        var shutdownRequested = false
    }

    private let shutdownState: ShutdownState
    private let workerState = Mutex(WorkerState())

    init(shutdownState: ShutdownState) {
        self.shutdownState = shutdownState
    }

    func attach(workerTask: Task<Void, Never>) {
        let shouldCancel = workerState.withLock { state in
            state.task = workerTask
            return state.shutdownRequested
        }
        if shouldCancel {
            workerTask.cancel()
        }
    }

    public func shutdown() async {
        await shutdownState.requestShutdown()
        let workerTask = workerState.withLock { state in
            state.shutdownRequested = true
            return state.task
        }
        workerTask?.cancel()
        await shutdownState.waitForFinish()
    }
}

public final class MessageStream: AsyncSequence, @unchecked Sendable {
    public typealias Element = (Message, Handler)

    public struct AsyncIterator: AsyncIteratorProtocol {
        // Retains the stream itself, not just its queue: otherwise iterating a
        // temporary (`for try await ... in subscriber.subscribe(s).build()`) lets
        // ARC release the MessageStream mid-loop and its deinit shuts the
        // subscription down silently.
        private let stream: MessageStream

        init(stream: MessageStream) {
            self.stream = stream
        }

        public mutating func next() async throws -> Element? {
            try await stream.next()
        }
    }

    private let queue: AsyncThrowingQueue<Element>
    private let shutdownState: ShutdownState

    private static let logger = Logger(label: "google-cloud-swift.pubsub.message-stream")

    // Owns the worker task; MessageStream does not keep a second reference.
    private let shutdownTokenValue: ShutdownToken

    init(subscribe: Subscribe) {
        let queue = AsyncThrowingQueue<Element>()
        let shutdownState = ShutdownState()
        let shutdownToken = ShutdownToken(shutdownState: shutdownState)
        let streamID = UUID()
        let shouldStart = subscribe.streamRegistry.register(shutdownToken, id: streamID)
        self.queue = queue
        self.shutdownState = shutdownState
        let workerTask = Task {
            if shouldStart {
                await Self.run(subscribe: subscribe, queue: queue, shutdownState: shutdownState)
            } else {
                // Finishing without an error here is indistinguishable from "no
                // messages", so a subscribe() that raced (or followed) shutdown would
                // silently iterate zero messages forever.
                _ = await queue.finish(
                    throwing: PubSubServiceError(message: "subscriber is shut down"))
                await shutdownState.markFinished()
            }
            subscribe.streamRegistry.unregister(id: streamID)
        }
        self.shutdownTokenValue = shutdownToken
        shutdownToken.attach(workerTask: workerTask)
    }

    deinit {
        let shutdownTokenValue = self.shutdownTokenValue
        Task {
            await shutdownTokenValue.shutdown()
        }
    }

    public func next() async throws -> Element? {
        try await queue.next()
    }

    public func shutdownToken() -> ShutdownToken {
        shutdownTokenValue
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(stream: self)
    }

    private static func run(
        subscribe: Subscribe,
        queue: AsyncThrowingQueue<Element>,
        shutdownState: ShutdownState
    ) async {
        let service = subscribe.service
        let subscription = subscribe.subscription
        let leaseManager = LeaseManager(
            subscription: subscription,
            service: service,
            shutdownBehavior: subscribe.shutdownBehavior,
            maxLease: subscribe.maxLease,
            maxLeaseExtension: subscribe.maxLeaseExtension,
            maxOutstandingMessages: subscribe.maxOutstandingMessages,
            maxOutstandingBytes: subscribe.maxOutstandingBytes,
            shutdownGracePeriod: subscribe.shutdownGracePeriod,
            retryPolicy: subscribe.retryPolicy
        )
        await leaseManager.start()

        var streamError: PubSubServiceError?
        let retryState = StreamRetryState()
        pull: while !Task.isCancelled {
            if await shutdownState.isShutdownRequested() {
                break
            }

            do {
                try await service.streamingPull(
                    subscription: subscription,
                    streamAckDeadlineSeconds: Int32(
                        subscribe.maxLeaseExtension.components.seconds),
                    maxOutstandingMessages: subscribe.maxOutstandingMessages,
                    maxOutstandingBytes: subscribe.maxOutstandingBytes,
                    clientID: subscribe.clientID,
                    onConnected: {
                        await retryState.connected()
                        Self.logger.debug(
                            "StreamingPull connected", metadata: ["subscription": "\(subscription)"]
                        )
                    },
                    onResponse: { response in
                        if await shouldStop(shutdownState: shutdownState) {
                            await leaseManager.nackUnregistered(
                                response.receivedMessages.map(\.ackID))
                            throw StreamControl.stop
                        }

                        let exactlyOnce =
                            response.hasSubscriptionProperties
                            && response.subscriptionProperties.exactlyOnceDeliveryEnabled
                        var deliveries: [Element] = []
                        deliveries.reserveCapacity(response.receivedMessages.count)

                        // Register the complete wire response before yielding any element.
                        // A response has already consumed the server-side flow-control
                        // allowance; leaving its tail outside activeLeases lets its 60s ack
                        // deadline expire while an earlier handler is being processed.
                        for receivedMessage in response.receivedMessages {
                            let handler = await leaseManager.register(
                                receivedMessage, exactlyOnce: exactlyOnce)
                            deliveries.append((receivedMessage.message, handler))
                        }
                        for delivery in deliveries {
                            await queue.yield(delivery)
                        }
                    }
                )

                if await shouldStop(shutdownState: shutdownState) {
                    break
                }

                Self.logger.debug(
                    "StreamingPull ended; reconnecting",
                    metadata: ["subscription": "\(subscription)"])
                try await Task.sleep(for: .milliseconds(50))
            } catch StreamControl.stop {
                break
            } catch is CancellationError {
                break
            } catch {
                if isCancellation(error) {
                    break
                }
                if await shouldStop(shutdownState: shutdownState) {
                    break
                }

                let serviceError = asServiceError(error)
                switch await retryState.decision(after: serviceError, policy: subscribe.retryPolicy)
                {
                case .stop:
                    Self.logger.error(
                        "StreamingPull stopped after a permanent or exhausted failure",
                        metadata: ["subscription": "\(subscription)", "error": "\(serviceError)"]
                    )
                    streamError = serviceError
                    break pull
                case .retry(let delay, let attempt):
                    Self.logger.debug(
                        "StreamingPull failed; reconnecting",
                        metadata: [
                            "subscription": "\(subscription)", "attempt": "\(attempt)",
                            "error": "\(serviceError)",
                        ]
                    )
                    do {
                        try await Task.sleep(for: delay)
                    } catch {
                        break pull
                    }
                }
            }
        }

        // Drain in a fresh task: this worker is cancelled by ShutdownToken, and
        // the final ack/nack RPCs (and their backoff sleeps) must not run in a
        // cancelled context or they fail immediately and the wait loop busy-spins.
        let finalError = streamError
        await Task {
            let undelivered = await queue.finish(throwing: finalError)
            for (_, handler) in undelivered {
                await handler.nackImmediately()
            }
            await leaseManager.shutdown()
            await shutdownState.markFinished()
        }.value
    }

    private static func shouldStop(shutdownState: ShutdownState) async -> Bool {
        if Task.isCancelled {
            return true
        }

        return await shutdownState.isShutdownRequested()
    }
}
