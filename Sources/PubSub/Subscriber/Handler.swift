import Foundation
import Synchronization

public enum Handler: Sendable {
  case atLeastOnce(AtLeastOnce)
  case exactlyOnce(ExactlyOnce)

  public func ack() {
    switch self {
    case .atLeastOnce(let handler):
      handler.ack()
    case .exactlyOnce(let handler):
      handler.ack()
    }
  }

  public func nack() {
    switch self {
    case .atLeastOnce(let handler):
      handler.nack()
    case .exactlyOnce(let handler):
      handler.nack()
    }
  }

  /// The number of times the service has attempted to deliver this message,
  /// or `nil` when the subscription has no dead-letter policy (the service
  /// only populates it for subscriptions that do).
  public var deliveryAttempt: Int32? {
    switch self {
    case .atLeastOnce(let handler):
      return handler.deliveryAttempt
    case .exactlyOnce(let handler):
      return handler.deliveryAttempt
    }
  }

  /// Nacks synchronously with respect to the lease manager, for shutdown
  /// paths that must enqueue the nack before the final flush runs.
  func nackImmediately() async {
    switch self {
    case .atLeastOnce(let handler):
      await handler.nackImmediately()
    case .exactlyOnce(let handler):
      await handler.nackImmediately()
    }
  }
}

public final class AtLeastOnce: @unchecked Sendable {
  private let ackID: String
  private let leaseManager: LeaseManager
  private let consumed = Mutex(false)

  /// See `Handler.deliveryAttempt`.
  public let deliveryAttempt: Int32?

  init(ackID: String, leaseManager: LeaseManager, deliveryAttempt: Int32? = nil) {
    self.ackID = ackID
    self.leaseManager = leaseManager
    self.deliveryAttempt = deliveryAttempt
  }

  public func ack() {
    guard consume() else {
      return
    }

    leaseManager.enqueueAcknowledgeAtLeastOnce(ackID)
  }

  public func nack() {
    guard consume() else {
      return
    }

    leaseManager.enqueueNegativelyAcknowledgeAtLeastOnce(ackID)
  }

  func nackImmediately() async {
    guard consume() else {
      return
    }

    await leaseManager.negativelyAcknowledgeAtLeastOnce(ackID)
  }

  deinit {
    guard consume() else {
      return
    }

    leaseManager.enqueueNegativelyAcknowledgeAtLeastOnce(ackID)
  }

  private func consume() -> Bool {
    consumed.withLock { state in
      if state {
        return false
      }

      state = true
      return true
    }
  }
}

public final class ExactlyOnce: @unchecked Sendable {
  private let ackID: String
  private let leaseManager: LeaseManager
  private let consumed = Mutex(false)
  private let resultBox: AsyncValue<Void, AckError>

  /// See `Handler.deliveryAttempt`.
  public let deliveryAttempt: Int32?

  init(
    ackID: String,
    leaseManager: LeaseManager,
    resultBox: AsyncValue<Void, AckError>,
    deliveryAttempt: Int32? = nil
  ) {
    self.ackID = ackID
    self.leaseManager = leaseManager
    self.resultBox = resultBox
    self.deliveryAttempt = deliveryAttempt
  }

  public func ack() {
    guard consume() else {
      return
    }

    leaseManager.enqueueAcknowledgeExactlyOnce(ackID)
  }

  public func nack() {
    guard consume() else {
      return
    }

    leaseManager.enqueueNegativelyAcknowledgeExactlyOnce(ackID)
  }

  func nackImmediately() async {
    guard consume() else {
      return
    }

    await leaseManager.negativelyAcknowledgeExactlyOnce(ackID)
  }

  public func confirmedAck() async throws {
    guard consume() else {
      throw AckError.shutdown("acknowledgement handler already consumed")
    }

    leaseManager.enqueueAcknowledgeExactlyOnce(ackID)
    try await resultBox.value()
  }

  public func confirmedNack() async throws {
    guard consume() else {
      throw AckError.shutdown("acknowledgement handler already consumed")
    }

    leaseManager.enqueueNegativelyAcknowledgeExactlyOnce(ackID)
    try await resultBox.value()
  }

  func waitForConfirmation() async throws {
    try await resultBox.value()
  }

  deinit {
    guard consume() else {
      return
    }

    leaseManager.enqueueNegativelyAcknowledgeExactlyOnce(ackID)
  }

  private func consume() -> Bool {
    consumed.withLock { state in
      if state {
        return false
      }

      state = true
      return true
    }
  }
}
