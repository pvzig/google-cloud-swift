# Google Cloud Swift

Swift clients for Google Cloud Pub/Sub and authentication, using `async`/`await`, `AsyncSequence`, and gRPC.

The package provides:

- **PubSub** — batching publishers, ordered publishing, streaming subscribers, automatic lease management, confirmed exactly-once acknowledgements, and topic, subscription, and schema administration.
- **Auth** — Application Default Credentials (ADC), token caching, service-account JWTs, user credentials, metadata-server authentication, service-account impersonation, and a gRPC authorization interceptor.
- **PubSubTestbed** — an executable integration testbed for the local emulator or an isolated Google Cloud project.

## Requirements

- Swift 6.3 or later.
- macOS 15 or later, or Linux. See [SPEC.md](SPEC.md) for recorded platform validation.
- For service calls, configured Google Cloud credentials or a running Pub/Sub emulator.

## Installation

Add the repository to your application's `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/pvzig/google-cloud-swift.git", branch: "main")
]
```

For development against a local checkout, use `.package(path: "../google-cloud-swift")` instead.

Then add the products your target uses:

```swift
.executableTarget(
    name: "Server",
    dependencies: [
        .product(name: "PubSub", package: "google-cloud-swift"),
        .product(name: "Auth", package: "google-cloud-swift"),
    ]
)
```

Use `import PubSub` for messaging. Add `Auth` to your target when you need to configure credentials directly.

## Authentication

Pub/Sub clients use ADC by default. The current lookup order is:

1. The credential JSON file named by `GOOGLE_APPLICATION_CREDENTIALS`.
2. `$HOME/.config/gcloud/application_default_credentials.json`.
3. The metadata server, for workloads running on Google Cloud.

For a credential file:

```sh
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
```

Supported ADC types are `authorized_user`, `service_account`, and `impersonated_service_account`. The `Auth` product also provides explicit credential builders and anonymous/API-key credentials. External-account credentials and STS exchange for Workload Identity Federation are not implemented.

To inject credentials, construct an `Auth.Authorization` and pass it to a client builder with `.withCredentials(authorization)`. Injected authorization remains caller-owned; shut it down after all clients using it have stopped.

## Publish messages

The following examples run inside an `async throws` function. Use full resource names, and create the topic and subscription before publishing.

```swift
import Foundation
import PubSub

let publisher = try await Publisher.builder("projects/my-project/topics/events")
    .setMessageCountThreshold(100)
    .setDelayThreshold(.milliseconds(10))
    .build()

do {
    let future = publisher.publish(
        Message(
            data: Data("Hello, Pub/Sub!".utf8),
            attributes: ["source": "swift"]
        )
    )

    await publisher.flush()
    let messageID = try await future.value()
    print("Published \(messageID)")
} catch {
    await publisher.shutdown()
    throw error
}

await publisher.shutdown()
```

`publish(_:)` synchronously queues a message and returns a `PublishFuture`. Await `value()` to obtain the server-assigned message ID or a `PublishError`. `flush()` bypasses batching delays and waits for messages already queued or in flight to reach a terminal result; inspect each future to detect failures.

The default batch thresholds are 100 messages, 1,000,000 bytes, or 10 milliseconds. Requests are capped at 1,000 messages and 10,000,000 serialized bytes.

Reuse publishers for ongoing traffic. For multiple topics sharing a connection, build a `BasePublisher` and derive publishers with `basePublisher.publisher(topic).build()`. Shutting down a derived publisher drains its messages; `BasePublisher.shutdown()` drains all derived publishers and closes the shared connection.

### Ordered publishing

Set `Message.orderingKey` to serialize batches for that key. Unordered batches can run concurrently. If an ordered batch fails permanently or exhausts retries, that key pauses and later messages fail with `PublishError.orderingKeyPaused`. After handling the failure, call `await publisher.resumePublish(orderingKey)` to accept new messages for the key. Failed messages are not automatically resubmitted by resume.

## Receive messages

```swift
import Foundation
import PubSub

let subscriber = try await Subscriber.builder().build()
let stream = subscriber.subscribe("projects/my-project/subscriptions/events-worker")
    .setMaxOutstandingMessages(100)
    .setMaxOutstandingBytes(10 * 1_024 * 1_024)
    .build()

do {
    for try await (message, handler) in stream {
        print(String(decoding: message.data, as: UTF8.self))
        handler.ack()
    }
} catch {
    await subscriber.shutdown()
    throw error
}

await subscriber.shutdown()
```

Each element contains a `Message` and a `Handler`. Call `ack()` after processing succeeds, or `nack()` to request redelivery. Dropping an unconsumed handler also queues a nack. These synchronous methods queue the operation; they do not wait for server confirmation.

The subscriber extends message leases during processing. The defaults are a one-hour maximum lease, 60-second extensions, 1,000 outstanding messages, and 100 MiB of outstanding message bytes. Configure these with the `Subscribe` builder's `setMaxLease`, `setMaxLeaseExtension`, and flow-control setters.

### Exactly-once acknowledgements

Handlers reflect the subscription properties reported by the service. For a subscription with exactly-once delivery enabled, use `confirmedAck()` or `confirmedNack()` to await the server result. For example, replace the `handler.ack()` above with:

```swift
switch handler {
case .atLeastOnce(let acknowledgement):
    acknowledgement.ack()
case .exactlyOnce(let acknowledgement):
    try await acknowledgement.confirmedAck()
}
```

A handler can be consumed once: do not call `ack()` before `confirmedAck()`. Confirmed operations throw `AckError` for service failures, lease expiry, or shutdown. Structured service details are available through `PubSubServiceError.errorInfo`.

### Shutdown

Keep a `stream.shutdownToken()` to stop one stream from another task with `await token.shutdown()`. Call `await subscriber.shutdown()` to stop all its streams and close the shared connection. Explicitly shut down clients when their owner finishes, including error paths.

The default `.waitForProcessing` shutdown behavior gives delivered handlers time to finish. `.setShutdownBehavior(.nackImmediately)` skips that processing wait. `.setShutdownGracePeriod(...)` defaults to 30 seconds **per phase**: processing, pending acknowledgements, and final nacks. It is not a single total shutdown deadline. Undelivered buffered messages are nacked during shutdown.

## Administration and configuration

| Surface | Operations |
| --- | --- |
| `TopicAdmin` | Create, get, update, list, and delete topics; list subscriptions and snapshots for a topic; detach subscriptions. |
| `SubscriptionAdmin` | Manage subscriptions, push configuration, snapshots, and seek operations. |
| `SchemaService` | Create, get, list, delete, validate, commit, and roll back schemas; validate messages. |

Each surface has a `.builder().build()` entry point and an async `shutdown()`. `Topic`, `Subscription`, `Message`, and `Schema` alias the generated protobuf models; other requests use the public `Google_Pubsub_V1_*` types. List operations return one response page; pass its `nextPageToken` in the next request to continue.

Client builders share `.withEndpoint(...)`, `.withCredentials(...)`, `.withCallTimeout(...)`, `.withGRPCSubchannelCount(...)`, and `.withRetryPolicy(...)`. Unary calls default to a 60-second timeout; StreamingPull has no unary deadline. `RetryPolicy` supports status-code selection, jittered exponential backoff, maximum attempts, and elapsed-time budgets.

## Local development

Build and run the unit tests from the repository root:

```sh
swift build
swift test --parallel
```

Run the emulator integration testbed:

```sh
./Scripts/run-pubsub-testbed.sh
```

The wrapper starts the emulator using Google Cloud CLI plus Java, or Docker, runs the testbed, and stops the emulator on exit. See the [testbed guide](Testbeds/PubSub/README.md) for prerequisites, environment variables, and real-service validation.

To point your own application at an existing emulator:

```sh
export PUBSUB_EMULATOR_HOST=127.0.0.1:8085
```

Alternatively, set `.useEmulator(host: "127.0.0.1:8085")` on each client builder. Emulator connections use plaintext transport and anonymous credentials. The emulator does not fully reproduce production behavior; exactly-once validation uses an explicitly enabled real-project testbed run.

See [SPEC.md](SPEC.md) for architecture, implementation scope, known gaps, and validation history.
