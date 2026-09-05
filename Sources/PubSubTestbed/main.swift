import Foundation
import PubSub

@main
struct PubSubTestbed {
  static func main() async {
    do {
      let configuration = try TestbedConfiguration.environment()
      let runner = TestbedRunner(configuration: configuration)
      let results = try await runner.run()

      print("Pub/Sub testbed passed")
      for result in results {
        print("  [ok] \(result)")
      }
    } catch {
      // Glibc exposes `stderr` as a mutable global, which strict concurrency
      // rejects; FileHandle reaches the same descriptor on every platform.
      let message = Data("Pub/Sub testbed failed: \(error)\n".utf8)
      try? FileHandle.standardError.write(contentsOf: message)
      Foundation.exit(1)
    }
  }
}

private struct TestbedConfiguration: Sendable {
  let projectID: String
  let resourcePrefix: String
  let emulatorHost: String?
  let runExactlyOnceScenario: Bool

  static func environment(
    _ environment: [String: String] = ProcessInfo.processInfo.environment
  ) throws -> Self {
    let emulatorHost = environment["PUBSUB_EMULATOR_HOST"]
    let allowRealCloud = environment["PUBSUB_TESTBED_ALLOW_REAL"] == "1"

    guard emulatorHost != nil || allowRealCloud else {
      throw TestbedError.configuration(
        "set PUBSUB_EMULATOR_HOST or PUBSUB_TESTBED_ALLOW_REAL=1")
    }

    let projectID =
      environment["PUBSUB_TEST_PROJECT_ID"]
      ?? environment["PUBSUB_PROJECT_ID"]
      ?? "google-cloud-swift-testbed"
    let suffix =
      environment["PUBSUB_TESTBED_SUFFIX"]
      ?? String(UUID().uuidString.lowercased().prefix(8))

    return Self(
      projectID: projectID,
      resourcePrefix: "swift-pubsub-\(suffix)",
      emulatorHost: emulatorHost,
      runExactlyOnceScenario: environment["PUBSUB_TESTBED_EXACTLY_ONCE"] == "1"
    )
  }
}

private struct TestbedRunner: Sendable {
  let configuration: TestbedConfiguration

  func run() async throws -> [String] {
    let topicAdmin = try await makeTopicAdmin()
    let subscriptionAdmin = try await makeSubscriptionAdmin()
    let topicName =
      "projects/\(configuration.projectID)/topics/\(configuration.resourcePrefix)-topic"
    let subscriptionName =
      "projects/\(configuration.projectID)/subscriptions/\(configuration.resourcePrefix)-sub"
    let redeliverySubscriptionName =
      "projects/\(configuration.projectID)/subscriptions/\(configuration.resourcePrefix)-redelivery"
    let exactlyOnceSubscriptionName =
      "projects/\(configuration.projectID)/subscriptions/\(configuration.resourcePrefix)-exactly-once"

    var createdSubscriptions: [String] = []
    var createdTopic: String?

    do {
      try await createTopic(topicName, using: topicAdmin)
      createdTopic = topicName

      try await createSubscription(
        subscriptionName,
        topic: topicName,
        using: subscriptionAdmin
      )
      createdSubscriptions.append(subscriptionName)

      var results: [String] = []
      results.append(
        try await runPublishAndStreamingReceiveScenario(
          topicName: topicName,
          subscriptionName: subscriptionName
        )
      )

      try await createSubscription(
        redeliverySubscriptionName,
        topic: topicName,
        using: subscriptionAdmin
      )
      createdSubscriptions.append(redeliverySubscriptionName)
      results.append(
        try await runNackRedeliveryScenario(
          topicName: topicName,
          subscriptionName: redeliverySubscriptionName
        )
      )

      if configuration.runExactlyOnceScenario {
        try await createSubscription(
          exactlyOnceSubscriptionName,
          topic: topicName,
          using: subscriptionAdmin,
          exactlyOnce: true
        )
        createdSubscriptions.append(exactlyOnceSubscriptionName)
        results.append(
          try await runExactlyOnceScenario(
            topicName: topicName,
            subscriptionName: exactlyOnceSubscriptionName
          )
        )
      } else {
        results.append(
          "exactly-once scenario skipped; set PUBSUB_TESTBED_EXACTLY_ONCE=1 to enable it"
        )
      }

      try await cleanup(
        topicAdmin: topicAdmin,
        subscriptionAdmin: subscriptionAdmin,
        topicName: createdTopic,
        subscriptionNames: createdSubscriptions
      )
      await topicAdmin.shutdown()
      await subscriptionAdmin.shutdown()
      return results
    } catch {
      try? await cleanup(
        topicAdmin: topicAdmin,
        subscriptionAdmin: subscriptionAdmin,
        topicName: createdTopic,
        subscriptionNames: createdSubscriptions
      )
      await topicAdmin.shutdown()
      await subscriptionAdmin.shutdown()
      throw error
    }
  }

  private func makeTopicAdmin() async throws -> TopicAdmin {
    var builder = TopicAdmin.builder()
    if let emulatorHost = configuration.emulatorHost {
      builder = builder.useEmulator(host: emulatorHost).useAnonymousCredentials()
    }

    return try await builder.build()
  }

  private func makeSubscriptionAdmin() async throws -> SubscriptionAdmin {
    var builder = SubscriptionAdmin.builder()
    if let emulatorHost = configuration.emulatorHost {
      builder = builder.useEmulator(host: emulatorHost).useAnonymousCredentials()
    }

    return try await builder.build()
  }

  private func makeBasePublisher() async throws -> BasePublisher {
    var builder = BasePublisher.builder()
    if let emulatorHost = configuration.emulatorHost {
      builder = builder.useEmulator(host: emulatorHost).useAnonymousCredentials()
    }

    return try await builder.build()
  }

  private func makeSubscriber() async throws -> Subscriber {
    var builder = Subscriber.builder()
    if let emulatorHost = configuration.emulatorHost {
      builder = builder.useEmulator(host: emulatorHost).useAnonymousCredentials()
    }

    return try await builder.build()
  }

  private func createTopic(_ name: String, using admin: TopicAdmin) async throws {
    var topic = Topic()
    topic.name = name
    _ = try await admin.createTopic(topic)
    _ = try await admin.getTopic(name: name)
  }

  private func createSubscription(
    _ name: String,
    topic: String,
    using admin: SubscriptionAdmin,
    exactlyOnce: Bool = false
  ) async throws {
    var subscription = Subscription()
    subscription.name = name
    subscription.topic = topic
    subscription.ackDeadlineSeconds = 10
    subscription.enableMessageOrdering = true
    subscription.enableExactlyOnceDelivery = exactlyOnce
    _ = try await admin.createSubscription(subscription)
    _ = try await admin.getSubscription(name: name)
  }

  private func runPublishAndStreamingReceiveScenario(
    topicName: String,
    subscriptionName: String
  ) async throws -> String {
    let basePublisher = try await makeBasePublisher()
    let subscriber = try await makeSubscriber()
    let stream = subscriber.subscribe(subscriptionName)
      .setMaxOutstandingMessages(2)
      .setMaxOutstandingBytes(1_024 * 1_024)
      .setShutdownBehavior(.nackImmediately)
      .build()

    do {
      let publisher = basePublisher.publisher(topicName)
        .setMessageCountThreshold(3)
        .setDelayThreshold(.seconds(30))
        .build()
      let expectedBodies = ["alpha", "bravo", "charlie"]
      let futures = expectedBodies.map { body in
        var message = Message(data: Data(body.utf8), attributes: ["scenario": "batch"])
        message.orderingKey = "batch-key"
        return publisher.publish(message)
      }

      await publisher.flush()
      for future in futures {
        _ = try await future.value()
      }

      let received = try await receiveBodies(
        expectedCount: expectedBodies.count,
        from: stream,
        timeout: .seconds(20),
        acknowledge: { $0.ack() }
      )
      guard Set(received) == Set(expectedBodies) else {
        throw TestbedError.unexpected("received \(received), expected \(expectedBodies)")
      }

      await stream.shutdownToken().shutdown()
      await publisher.shutdown()
      await basePublisher.shutdown()
      await subscriber.shutdown()
      return "publish batching plus StreamingPull receive and ack"
    } catch {
      await stream.shutdownToken().shutdown()
      await basePublisher.shutdown()
      await subscriber.shutdown()
      throw error
    }
  }

  private func runNackRedeliveryScenario(topicName: String, subscriptionName: String) async throws
    -> String
  {
    let basePublisher = try await makeBasePublisher()
    let subscriber = try await makeSubscriber()
    let stream = subscriber.subscribe(subscriptionName)
      .setShutdownBehavior(.nackImmediately)
      .build()

    do {
      let publisher = basePublisher.publisher(topicName)
        .setMessageCountThreshold(1)
        .build()
      let future = publisher.publish(Message(data: Data("redeliver".utf8)))
      await publisher.flush()
      _ = try await future.value()

      let first = try await receiveNext(from: stream, timeout: .seconds(20))
      guard first.body == "redeliver" else {
        throw TestbedError.unexpected("redelivery scenario first body was \(first.body)")
      }
      first.handler.nack()

      let second = try await receiveNext(from: stream, timeout: .seconds(20))
      guard second.body == "redeliver" else {
        throw TestbedError.unexpected("redelivery scenario second body was \(second.body)")
      }
      second.handler.ack()

      await stream.shutdownToken().shutdown()
      await publisher.shutdown()
      await basePublisher.shutdown()
      await subscriber.shutdown()
      return "nack redelivery over StreamingPull"
    } catch {
      await stream.shutdownToken().shutdown()
      await basePublisher.shutdown()
      await subscriber.shutdown()
      throw error
    }
  }

  private func runExactlyOnceScenario(topicName: String, subscriptionName: String) async throws
    -> String
  {
    let basePublisher = try await makeBasePublisher()
    let subscriber = try await makeSubscriber()
    let stream = subscriber.subscribe(subscriptionName)
      .setShutdownBehavior(.nackImmediately)
      .build()

    do {
      let publisher = basePublisher.publisher(topicName)
        .setMessageCountThreshold(1)
        .build()
      let future = publisher.publish(Message(data: Data("exactly-once".utf8)))
      await publisher.flush()
      _ = try await future.value()

      let received = try await receiveNext(from: stream, timeout: .seconds(30))
      guard received.body == "exactly-once" else {
        throw TestbedError.unexpected("exactly-once body was \(received.body)")
      }

      guard case .exactlyOnce(let handler) = received.handler else {
        throw TestbedError.unexpected("exactly-once subscription yielded at-least-once handler")
      }

      try await handler.confirmedAck()

      await stream.shutdownToken().shutdown()
      await publisher.shutdown()
      await basePublisher.shutdown()
      await subscriber.shutdown()
      return "exactly-once confirmed ack"
    } catch {
      await stream.shutdownToken().shutdown()
      await basePublisher.shutdown()
      await subscriber.shutdown()
      throw error
    }
  }

  private func receiveBodies(
    expectedCount: Int,
    from stream: MessageStream,
    timeout: Duration,
    acknowledge: @escaping @Sendable (Handler) -> Void
  ) async throws -> [String] {
    var bodies: [String] = []
    bodies.reserveCapacity(expectedCount)

    while bodies.count < expectedCount {
      let item = try await receiveNext(from: stream, timeout: timeout)
      bodies.append(item.body)
      acknowledge(item.handler)
    }

    return bodies
  }

  private func receiveNext(from stream: MessageStream, timeout: Duration) async throws -> (
    body: String, handler: Handler
  ) {
    let element = try await withTimeout(timeout, label: "waiting for next Pub/Sub message") {
      try await stream.next()
    }
    guard let (message, handler) = element else {
      throw TestbedError.unexpected("stream ended before the expected message arrived")
    }

    guard let body = String(data: message.data, encoding: .utf8) else {
      throw TestbedError.unexpected("received message data is not UTF-8")
    }

    return (body, handler)
  }

  private func cleanup(
    topicAdmin: TopicAdmin,
    subscriptionAdmin: SubscriptionAdmin,
    topicName: String?,
    subscriptionNames: [String]
  ) async throws {
    for subscriptionName in subscriptionNames.reversed() {
      try? await subscriptionAdmin.deleteSubscription(name: subscriptionName)
    }

    if let topicName {
      try? await topicAdmin.deleteTopic(name: topicName)
    }
  }
}

private enum TestbedError: Error, CustomStringConvertible {
  case configuration(String)
  case timeout(String)
  case unexpected(String)

  var description: String {
    switch self {
    case .configuration(let message):
      return message
    case .timeout(let label):
      return "timed out: \(label)"
    case .unexpected(let message):
      return message
    }
  }
}

private func withTimeout<Value: Sendable>(
  _ duration: Duration,
  label: String,
  operation: @escaping @Sendable () async throws -> Value
) async throws -> Value {
  try await withThrowingTaskGroup(of: Value.self) { group in
    group.addTask {
      try await operation()
    }
    group.addTask {
      try await Task.sleep(for: duration)
      throw TestbedError.timeout(label)
    }

    guard let value = try await group.next() else {
      throw TestbedError.timeout(label)
    }

    group.cancelAll()
    return value
  }
}
