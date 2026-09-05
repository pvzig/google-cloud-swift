import Foundation

actor AsyncValue<Value: Sendable, Failure: Error & Sendable> {
    private var result: Result<Value, Failure>?
    private var continuations: [UInt64: CheckedContinuation<Result<Value, Failure>?, Never>] = [:]
    private var nextWaiterID: UInt64 = 0
    private var cancelledWaiterIDs: Set<UInt64> = []

    func succeed(_ value: Value) {
        guard result == nil else {
            return
        }

        result = .success(value)
        resumeAll(returning: .success(value))
    }

    func fail(_ error: Failure) {
        guard result == nil else {
            return
        }

        result = .failure(error)
        resumeAll(returning: .failure(error))
    }

    func value() async throws -> Value {
        if let result {
            return try result.get()
        }

        let id = nextWaiterID
        nextWaiterID += 1
        // A nil outcome means the waiter was cancelled before the value resolved.
        let outcome = await withTaskCancellationHandler {
            await withCheckedContinuation {
                (continuation: CheckedContinuation<Result<Value, Failure>?, Never>) in
                addWaiter(id: id, continuation: continuation)
            }
        } onCancel: {
            Task { await self.cancelWaiter(id) }
        }

        guard let outcome else {
            throw CancellationError()
        }

        return try outcome.get()
    }

    /// Waits for the terminal result without treating caller cancellation as a
    /// request to stop waiting. Internal flush and shutdown paths use this to
    /// finish ownership cleanup even when their caller is already cancelled.
    func terminalResult() async -> Result<Value, Failure> {
        if let result {
            return result
        }

        let id = nextWaiterID
        nextWaiterID += 1
        let outcome = await withCheckedContinuation {
            (continuation: CheckedContinuation<Result<Value, Failure>?, Never>) in
            addWaiter(id: id, continuation: continuation)
        }

        // This waiter has no cancellation handler, so a nil result is impossible.
        guard let outcome else {
            preconditionFailure("an uncancellable AsyncValue waiter was cancelled")
        }
        return outcome
    }

    private func addWaiter(
        id: UInt64,
        continuation: CheckedContinuation<Result<Value, Failure>?, Never>
    ) {
        if let result {
            continuation.resume(returning: result)
            return
        }

        // The cancellation handler can run before the waiter registers.
        if cancelledWaiterIDs.remove(id) != nil {
            continuation.resume(returning: nil)
            return
        }

        continuations[id] = continuation
    }

    private func cancelWaiter(_ id: UInt64) {
        if let continuation = continuations.removeValue(forKey: id) {
            continuation.resume(returning: nil)
            return
        }

        if result == nil {
            cancelledWaiterIDs.insert(id)
        }
    }

    private func resumeAll(returning outcome: Result<Value, Failure>) {
        let continuations = self.continuations
        self.continuations.removeAll(keepingCapacity: false)
        cancelledWaiterIDs.removeAll(keepingCapacity: false)
        continuations.values.forEach { $0.resume(returning: outcome) }
    }

}

actor AsyncThrowingQueue<Element: Sendable> {
    private enum State {
        case open
        case finished(PubSubServiceError?)
    }

    private typealias Waiter = (
        id: UInt64, continuation: CheckedContinuation<Result<Element?, PubSubServiceError>?, Never>
    )

    private var state: State = .open
    // Consumed via a head index instead of removeFirst(): every delivered
    // message passes through this buffer, and shifting the array per element
    // makes draining a backlog O(n²) right when the consumer is already behind.
    private var buffered: [Element?] = []
    private var bufferedHead = 0
    private var waiters: [Waiter] = []
    private var nextWaiterID: UInt64 = 0
    private var cancelledWaiterIDs: Set<UInt64> = []

    private var hasBuffered: Bool {
        bufferedHead < buffered.count
    }

    private func dequeueBuffered() -> Element {
        guard let element = buffered[bufferedHead] else {
            preconditionFailure("buffered queue slot was consumed twice")
        }
        // Release handlers and payloads as soon as they are consumed. The head
        // index avoids O(n) shifting, but it must not keep the skipped elements
        // alive until the next compaction threshold.
        buffered[bufferedHead] = nil
        bufferedHead += 1
        if bufferedHead > 64, bufferedHead * 2 >= buffered.count {
            buffered.removeFirst(bufferedHead)
            bufferedHead = 0
        }
        return element
    }

    func yield(_ element: Element) {
        guard case .open = state else {
            return
        }

        if !waiters.isEmpty {
            let waiter = waiters.removeFirst()
            waiter.continuation.resume(returning: .success(element))
            return
        }

        buffered.append(element)
    }

    /// Finishes the queue and returns the buffered elements that were never
    /// delivered to a consumer, so the caller can dispose of them (e.g. nack).
    @discardableResult
    func finish(throwing error: PubSubServiceError? = nil) -> [Element] {
        guard case .open = state else {
            return []
        }

        state = .finished(error)
        let undelivered = buffered[bufferedHead...].compactMap { $0 }
        buffered.removeAll(keepingCapacity: false)
        bufferedHead = 0
        let waiters = self.waiters
        self.waiters.removeAll(keepingCapacity: false)
        cancelledWaiterIDs.removeAll(keepingCapacity: false)
        for waiter in waiters {
            if let error {
                waiter.continuation.resume(returning: .failure(error))
            } else {
                waiter.continuation.resume(returning: .success(nil))
            }
        }
        return undelivered
    }

    func next() async throws -> Element? {
        if hasBuffered {
            return dequeueBuffered()
        }

        switch state {
        case .open:
            let id = nextWaiterID
            nextWaiterID += 1
            // A nil outcome means the waiter was cancelled before an element arrived.
            let outcome = await withTaskCancellationHandler {
                await withCheckedContinuation {
                    (
                        continuation: CheckedContinuation<
                            Result<Element?, PubSubServiceError>?, Never
                        >
                    ) in
                    addWaiter(id: id, continuation: continuation)
                }
            } onCancel: {
                Task { await self.cancelWaiter(id) }
            }

            guard let outcome else {
                throw CancellationError()
            }

            return try outcome.get()
        case .finished(let error):
            if let error {
                throw error
            }

            return nil
        }
    }

    private func addWaiter(
        id: UInt64,
        continuation: CheckedContinuation<Result<Element?, PubSubServiceError>?, Never>
    ) {
        // The cancellation handler can run before the waiter registers.
        if cancelledWaiterIDs.remove(id) != nil {
            continuation.resume(returning: nil)
            return
        }

        if hasBuffered {
            continuation.resume(returning: .success(dequeueBuffered()))
            return
        }

        if case .finished(let error) = state {
            if let error {
                continuation.resume(returning: .failure(error))
            } else {
                continuation.resume(returning: .success(nil))
            }
            return
        }

        waiters.append((id: id, continuation: continuation))
    }

    private func cancelWaiter(_ id: UInt64) {
        if let index = waiters.firstIndex(where: { $0.id == id }) {
            let waiter = waiters.remove(at: index)
            waiter.continuation.resume(returning: nil)
            return
        }

        if case .open = state {
            cancelledWaiterIDs.insert(id)
        }
    }
}
