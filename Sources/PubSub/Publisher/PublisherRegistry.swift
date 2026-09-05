import Foundation
import Synchronization

final class PublisherRegistry: @unchecked Sendable {
    private struct State: Sendable {
        var isShutdown = false
        var dispatchers: [UUID: PublisherDispatcher] = [:]
    }

    private let state = Mutex(State())

    func register(_ dispatcher: PublisherDispatcher) -> UUID? {
        state.withLock { state in
            guard state.isShutdown == false else {
                return nil
            }
            let id = UUID()
            state.dispatchers[id] = dispatcher
            return id
        }
    }

    func unregister(id: UUID) {
        state.withLock { state in
            state.dispatchers[id] = nil
        }
    }

    func shutdownAll() async {
        let dispatchers = state.withLock { state in
            state.isShutdown = true
            return Array(state.dispatchers.values)
        }

        await withTaskGroup(of: Void.self) { group in
            for dispatcher in dispatchers {
                group.addTask {
                    await dispatcher.shutdown()
                }
            }
        }

        state.withLock { state in
            state.dispatchers.removeAll(keepingCapacity: false)
        }
    }
}
