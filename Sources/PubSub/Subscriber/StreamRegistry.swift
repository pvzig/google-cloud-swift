import Foundation
import Synchronization

final class SubscriberStreamRegistry: @unchecked Sendable {
  private struct State: Sendable {
    var isShutdown = false
    var streams: [UUID: ShutdownToken] = [:]
  }

  private let state = Mutex(State())

  func register(_ token: ShutdownToken, id: UUID) -> Bool {
    state.withLock { state in
      guard !state.isShutdown else {
        return false
      }

      state.streams[id] = token
      return true
    }
  }

  func unregister(id: UUID) {
    state.withLock { state in
      state.streams[id] = nil
    }
  }

  func shutdownAll() async {
    let tokens = state.withLock { state in
      state.isShutdown = true
      return Array(state.streams.values)
    }

    await withTaskGroup(of: Void.self) { group in
      for token in tokens {
        group.addTask {
          await token.shutdown()
        }
      }
    }

    state.withLock { state in
      state.streams.removeAll(keepingCapacity: false)
    }
  }
}
