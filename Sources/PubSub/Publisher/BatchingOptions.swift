import Foundation

public struct BatchingOptions: Sendable, Equatable {
  public static let maxMessages = 1_000
  public static let maxBytes = 10_000_000
  public static let maxDelay = Duration.seconds(24 * 60 * 60)

  public var messageCountThreshold: Int
  public var byteThreshold: Int
  public var delayThreshold: Duration

  public init(
    messageCountThreshold: Int = 100,
    byteThreshold: Int = 1_000_000,
    delayThreshold: Duration = .milliseconds(10)
  ) {
    self.messageCountThreshold = messageCountThreshold
    self.byteThreshold = byteThreshold
    self.delayThreshold = delayThreshold
  }

  public func setMessageCountThreshold(_ threshold: Int) -> Self {
    var copy = self
    copy.messageCountThreshold = threshold
    return copy
  }

  public func setByteThreshold(_ threshold: Int) -> Self {
    var copy = self
    copy.byteThreshold = threshold
    return copy
  }

  public func setDelayThreshold(_ threshold: Duration) -> Self {
    var copy = self
    copy.delayThreshold = threshold
    return copy
  }

  var normalized: Self {
    Self(
      messageCountThreshold: min(max(1, messageCountThreshold), Self.maxMessages),
      byteThreshold: min(max(1, byteThreshold), Self.maxBytes),
      delayThreshold: min(delayThreshold, Self.maxDelay)
    )
  }
}
