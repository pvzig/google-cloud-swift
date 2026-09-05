# PubSub Swift Port

## Goal

Reimplement the upstream `google-cloud-rust` Pub/Sub and auth surfaces in Swift with modern Swift concurrency.

## Upstream References

- `googleapis/google-cloud-rust/src/pubsub`
- `googleapis/google-cloud-rust/src/pubsub/ARCHITECTURE.md`
- `googleapis/google-cloud-rust/src/auth`
- `yoshidan/google-cloud-rust/pubsub`
- `rosecoder/google-cloud-pubsub-swift`

## Package Layout

- `README.md`: Package requirements, local SwiftPM integration, authentication, publish/subscribe examples, lifecycle contracts, administration, and development entry points.
- `Sources/Auth`: In-repo auth module modeled on `google-cloud-rust/src/auth`, including credentials, token caching, metadata-server auth, and gRPC authorization interception.
- `Sources/PubSub/Generated`: Vendored protobuf and gRPC definitions for Pub/Sub and Schema services.
- `Sources/PubSub/Core`: Shared errors, retry/backoff, configuration, and connection management.
- `Sources/PubSub/Admin`: Thin administrative wrappers for topics, subscriptions, and schemas.
- `Sources/PubSub/Publisher`: Hand-written batching publisher aligned with the Rust actor architecture.
- `Sources/PubSub/Subscriber`: Hand-written message stream, handler, and lease management aligned with the Rust subscriber architecture.
- `Sources/PubSubTestbed`: Executable integration testbed for exercising the client against a live Pub/Sub-compatible endpoint.
- `Scripts/run-pubsub-testbed.sh`: One-command wrapper that starts the official Pub/Sub emulator through local `gcloud` or the Google Cloud CLI Docker emulator image, falls back to Docker when the local component cannot start, then runs the Swift testbed.
- `Testbeds/PubSub`: Testbed runbook, environment variables, and real-service guidance.
- `Tests/PubSubTests`: Adapted verification from upstream Rust Pub/Sub unit tests plus Swift-specific transport tests.
- `Tests/AuthTests`: Swift tests for auth session caching, API-key headers, and ADC field decoding.

## Dependency Decisions

Checked against GitHub latest releases on 2026-07-13:

- `swift-server/async-http-client`: `1.36.0`, used for auth token and metadata-server HTTP calls.
- `apple/swift-nio`: `2.101.2`
- `apple/swift-protobuf`: `1.38.1`
- `grpc/grpc-swift-2`: `2.4.2` for the local Auth gRPC interceptor dependency.
- `grpc/grpc-swift-protobuf`: `2.4.1`
- `grpc/grpc-swift-nio-transport`: `2.9.0`
- `vapor/jwt-kit`: `5.6.0`, used only for service-account RS256 JWT/JWS signing.

Checked when logging was made a direct dependency on 2026-09-02:

- `apple/swift-log`: `1.15.0`, the latest GitHub release, used for auth refresh/fail-open diagnostics and Pub/Sub retry, reconnect, and lease-extension diagnostics.

Removed:

- `rosecoder/google-cloud-auth-swift`: replaced by the local `Auth` target to mirror `googleapis/google-cloud-rust/src/auth`.

## Architecture Notes

- Use real Pub/Sub gRPC service definitions instead of hand-written request models.
- Keep the public Swift surface ergonomic, while mirroring the Rust crate concepts:
  - `Publisher`, `BasePublisher`
  - `Subscriber`, `Subscribe`, `MessageStream`
  - `Handler`, `AtLeastOnce`, `ExactlyOnce`
- `TopicAdmin`, `SubscriptionAdmin`, `SchemaService`
- Publisher uses background batching actors keyed by ordering key.
- Subscriber uses a dedicated async receive loop plus a lease manager actor that batches ack, nack, and lease extension operations.
- Subscriber receive awaits the bidirectional `StreamingPull` response callback directly, preserving grpc-swift/NIO inbound watermark backpressure; ack, nack, and lease extension remain batched through the lease manager’s unary RPCs like the Rust leaser.
- Modern Swift concurrency only: actors, `AsyncThrowingStream`, `Task`, and `TaskGroup`.
- Auth follows the Rust module split where practical:
  - `Authorization` and `AuthorizationClientInterceptor` are the Swift gRPC integration surface.
  - `DefaultProvider` follows ADC order: `GOOGLE_APPLICATION_CREDENTIALS`, well-known gcloud ADC file, then metadata server.
  - Implemented credential providers: anonymous, API key, authorized user refresh token, service account self-signed JWT, metadata server, and service-account impersonation.
  - External account / STS exchange is represented as an explicit unsupported provider until a consumer requires Workload Identity Federation.
  - `TokenCache` coalesces refreshes and refreshes stale tokens on demand. Refresh slack is proportional for short-lived tokens, and only transient refresh failures may fail open to a still-valid cached token; permanent credential failures surface immediately.

## Correctness Notes

- `Publisher.publish(_:)` records messages synchronously in dispatcher ingress before returning a `PublishFuture`. `Publisher.flush()` drains that ingress, forces pending batches across ordering keys, and waits for all messages buffered or in flight at the time of the call to reach a terminal state. This mirrors the upstream Rust publisher channel ordering, where `Publish` and `Flush` messages are serialized through the same dispatcher and flush waits for batch actors.
- `Publisher.shutdown()` flushes outstanding messages before shutting down an owned service connection. `BasePublisher.shutdown()` first drains every derived publisher registered against its shared connection, then closes that connection.
- `LeaseManager.availablePullCapacity(defaultBatchSize:)` returns zero when the configured outstanding message limit or byte limit is already saturated.
- Exactly-once leases track delivery state separately from at-least-once leases. Leased exactly-once messages that exceed the max lease fail confirmation with `AckError.leaseExpired`; messages already pending ack remain lease-extended until the ack result completes; messages pending nack are not extended.
- At-least-once `ack()` remains optimistic and queues an ack even if the local lease is already gone, matching the upstream Rust distinction between best-effort at-least-once ack and confirmed exactly-once ack.
- `MessageStream` opens `StreamingPull` with subscription, stream ack deadline, max outstanding message/byte flow-control values, and a stable per-subscription client ID. It sends empty keepalive requests every 30 seconds while consuming responses as an `AsyncSequence`. Each complete wire response is registered with the lease manager before any message is yielded, so response tails remain lease-extended while the consumer is at capacity. Shutdown nacks every undelivered registered handler before closing the stream. A successful request-stream write resets reconnect attempts and the failure window even when an idle stream has delivered no response.
- Streaming response `subscriptionProperties.exactlyOnceDeliveryEnabled` controls whether yielded handlers are at-least-once or exactly-once, matching the Rust subscriber stream behavior.
- `Handler.deliveryAttempt` exposes the service's redelivery count for dead-letter routing, reported as `nil` rather than `0` when the subscription has no dead-letter policy and the service omits the field.
- `ClientConfiguration.grpcSubchannelCount` now controls the number of live gRPC clients created by `ServiceConnection`, and client RPC stubs are assigned round-robin across that pool.
- Every RPC sends `x-goog-request-params` with RFC 3986 percent-encoded resource values for the fields Pub/Sub routes on, matching each method's `google.api.http` path parameters (`subscription=`, `topic=`, `project=`, `parent=`, `snapshot=`, `name=`, and the `*.name` update variants). Regional endpoints and server-side StreamingPull affinity depend on it; without the header the service can only fall back to the global default, and unescaped query separators can route a different resource.
- Public client surfaces that own live connections expose `shutdown()` so callers can flush or close background gRPC/authentication resources explicitly.
- Pub/Sub no longer depends on an external auth package. Production credentials are provided by the local `Auth` target; emulator paths continue to force anonymous plaintext credentials.
- SwiftNIO is owned transitively by the gRPC/JWT dependency graph; the package does not expose unused direct `NIO` product dependencies. The resolved graph pins SwiftNIO 2.101.3.
- Auth's HTTP calls run on `AsyncHTTPClient` rather than `URLSession`. corelibs-foundation's `URLSession` is the weakest link on Linux, where most consumers of this client run, and NIO was already in the resolved graph, so the marginal dependency cost is small relative to having one HTTP path that behaves identically everywhere. The process-wide `HTTPClient.shared` is used deliberately: it cannot be shut down, so no credential provider has to own or tear down a connection pool. Responses are capped at 1 MB and the complete exchange, including body collection, is bounded to 30 seconds. Transport failures surface as transient `CredentialsError`s so the token cache's fail-open path can recognize them.
- The testbed writes diagnostics through `FileHandle.standardError` rather than glibc's mutable `stderr` global, which strict concurrency rejects on Linux.
- ByteBuffer payloads are read via `readableBytesView` rather than the `Data` bridging in `NIOFoundationCompat`, which Darwin imports transitively and Linux does not.
- Publisher admission and shutdown share one synchronized ingress state. Shutdown rejects later publishes atomically, drains through cancellation-insensitive terminal waits, and keeps delayed batch work alive even when the caller releases its `Publisher` value.
- Flush tracks both transported entries and local validation/pause rejections while their publish futures are being resolved, so actor reentrancy cannot make a concurrent flush return during the completion handoff.
- Ordering-key resume drains the synchronous publish ingress before clearing the paused state, so messages submitted while paused cannot overtake the resume command.
- grpc-swift-nio-transport 2.9 reads `maxRequestMessageBytes` as its inbound deframer limit and ignores `maxResponseMessageBytes`, so call options feed the 20 MB inbound limit through both fields. A client interceptor independently enforces Pub/Sub's 10 MB outbound limit for every request. Publisher batching computes exact protobuf sizes without materializing payload copies and retains only completion boxes while `flush()` waits, so admission and flush do not pin redundant full-message buffers.
- Subscriber handler commands enter a synchronized lease-command queue before synchronous `ack()` or `nack()` returns. Under `.waitForProcessing` that queue stays open for the whole wait, so acknowledgements released while shutdown is in progress are honored rather than dropped; it is closed and drained only once the lease state is final.
- `Subscribe.setShutdownGracePeriod()` (30 seconds by default) bounds each shutdown phase independently: consumer processing, the pending-ack drain while leases remain extended, and the final nack drain. A consumer using the full processing window therefore cannot reduce later RPCs to the 1 ms floor; the floor remains only for races at an individual phase deadline. Shutdown then cancels and joins the extender before converting available leases to nacks so no later positive extension can delay redelivery. Shutdown-generated exactly-once nacks fail their confirmation boxes with `AckError.shutdown`; only application-requested confirmed nacks resolve successfully.
- Subscriber shutdown owns active `MessageStream` lifetimes and waits for their workers to finish before gracefully closing the shared gRPC connection.
- Exactly-once ack/nack failures expose rich gRPC `ErrorInfo` reason, domain, and metadata through `PubSubServiceError.errorInfo`, decoded from the RPC status on first read rather than at construction so failures that never consult it (stream reconnects, lease-extension retries, publish backoff) skip the protobuf decode entirely; equality compares the decoded values, so an error carrying a status matches one built from the same explicit values. resolve successful and permanent IDs independently, and retry only transient IDs within upstream's 600-second default budget with no attempt cap. Upstream's 1s..64s exactly-once backoff is deliberately not adopted: one policy drives both ack paths here, and upstream retries at-least-once acks with no backoff at all, so the configured 100ms..60s shape is kept for both. `Subscribe.setShutdownGracePeriod()` still bounds how much of that budget a shutdown will wait out. Each per-ID elapsed-time budget starts immediately before its first RPC and honors configured jittered backoff instead of the periodic flush cadence. Budgets are enforced per ID, never per chunk: an ID whose budget is exhausted is failed on its own, and a chunk's RPC deadline is the longest remaining budget in it, so one nearly-expired ID can neither fail nor starve the other IDs sharing its RPC.
- Lease shutdown cancels and joins tracked periodic/immediate flush workers before its final drain, making any genuinely cancelled idempotent ack/nack RPC eligible for one of the bounded final attempts without charging it a failed attempt. A real service error arriving on a cancelled worker is still classified per ID and reported as itself.
- A `PubSubServiceError` without an RPC code is permanent in the acknowledgement retry path, matching `RetryPolicy.shouldRetry`; only transport errors converted by `asServiceError` receive the retryable `.unknown` code.
- Per-ID ack/nack dispositions are interpreted only for `ErrorInfo` with reason `EXACTLY_ONCE_ACK_FAILURE` and domain `pubsub.googleapis.com`. Other details, including quota metadata, follow the whole-RPC retry/failure path and cannot produce successful exactly-once confirmations for rejected RPCs.
- An immediate flush requested while one is already in flight is coalesced into another round rather than dropped until the next periodic tick, so a backlog that re-crosses the per-RPC cap during an in-flight flush does not wait for the periodic cadence.
- Every ack/nack chunk in a flush is issued concurrently, like upstream spawning each into its task tracker; outcomes are then applied on the actor so state updates stay serialized. Serializing the RPCs would let one slow chunk consume every later chunk's deadline budget.
- Streaming flow control is enforced at the transport boundary by awaiting response handling and by registering a complete received response before yielding it; this avoids a second capacity wait that would leave already-received messages outside lease tracking.
- Lease extension sweeps run on upstream's cadence (first sweep after 500ms, then every 3 seconds) and launch tracked per-sweep workers instead of awaiting a retry loop inline. A slow chunk therefore cannot suspend renewal scheduling for later leases. Re-extension uses the request-send instant as the conservative granted-deadline origin, and only a chunk that actually succeeded records its extension.
- Lease extension chunks retry concurrently within the current lease deadline, with each RPC timeout capped to the remaining renewal budget.
- Authorized-user refresh requests use `application/x-www-form-urlencoded`; caller-injected `Authorization` instances remain caller-owned when a Pub/Sub client shuts down.
- Authorized-user refresh-token grants never attach caller-requested API scopes, because an RFC 6749 refresh may reproduce but not widen the original grant. Impersonated credentials do not shut down the process-wide `DefaultProvider` source.
- The authorization interceptor maps permanent `CredentialsError`s to UNAUTHENTICATED and transient ones to UNAVAILABLE before grpc-swift can erase them into UNKNOWN. Public Pub/Sub errors preserve wrapped credential messages and recursive cancellation causes.
- The ADC JSON factory normalizes malformed JSON, missing fields, and invalid service-account private keys to permanent `CredentialsError.parsing` failures, including nested impersonated source credentials. Existing domain errors retain their original cases, so malformed local credentials reach the interceptor as UNAUTHENTICATED instead of entering transport retries.
- `ClientConfiguration.callTimeout` and `grpcSubchannelCount` normalize invalid values on every public mutation, not only through fluent builders.
- Invalid `GCE_METADATA_HOST` values, including out-of-range ports, are retained as a fallible metadata configuration and surface `CredentialsError.loading` during token acquisition instead of crashing provider initialization. A set-but-empty value is treated as unset and falls back to the default metadata server rather than becoming a permanent ADC failure.
- Custom endpoints and `PUBSUB_EMULATOR_HOST` share one validated authority parser and accept only valid HTTP(S) authorities with valid TCP ports; malformed schemes, hosts, userinfo, paths, and ports fail client construction with `PubSubServiceError`. A set-but-empty emulator variable is treated as unset. The emulator additionally requires an explicit port and is always plaintext.
- The emulator wrapper refuses to reuse an occupied HTTP port, checks that its own launcher remains alive before declaring readiness, and terminates the complete local emulator process tree on exit.
- Auth and Pub/Sub emit `swift-log` diagnostics for token/session refresh and fail-open, credential interception, RPC retry decisions, stream reconnects, and lease-extension failures. The three admin surfaces share one typed executor for retry, routing metadata, call options, and error mapping.
- Retry backoff calculation normalizes negative durations and invalid multipliers and caps its integer conversion, so runtime-supplied `RetryPolicy` values cannot trap the process.

## Upstream Comparison

Compared against `googleapis/google-cloud-rust` main at commit `686c1e2` on 2026-04-23:

- Publisher flush semantics now match the Rust contract in `publisher/client.rs` and `publisher/actor.rs`: a flush after publish observes earlier publishes, bypasses batching delay, and waits for in-flight batch completion.
- Exactly-once lease expiry now matches `subscriber/lease_state/exactly_once.rs`: only leased messages expire, lease-expired confirmations are completed with a lease-expired error, acking messages remain retained, and nacking messages are skipped for lease extension.
- `grpcSubchannelCount` now has an observable transport effect in Swift by creating a gRPC client pool. Rust also uses the value for subscriber lease-management retry attempts; Swift still uses its general `RetryPolicy` for those calls.
- Subscriber receive now aligns with Rust’s `StreamingPull` path. The remaining difference is that Rust wires keepalive through an explicit request channel, while Swift writes the initial request and periodic empty keepalive requests directly in the gRPC request producer.

Re-compared against `googleapis/google-cloud-rust` main on 2026-06-11:

- Retry/backoff now follows upstream gax semantics: full-jitter exponential backoff, default retryable codes `[aborted, deadlineExceeded, internal, resourceExhausted, unavailable, unknown]` (CANCELLED is permanent), an optional `maxElapsedTime` budget (admin defaults to 10 attempts/60s, publish to a 600s time limit), and AIP-194 idempotency: PATCH/POST-backed admin RPCs retry only on UNAVAILABLE. Errors without an RPC status map to a transient UNKNOWN, so transport blips reconnect streams and retry RPCs like upstream’s io/transport handling.
- Unordered (empty ordering key) publishing now uses concurrent in-flight batches like Rust’s `ConcurrentBatchActor`; ordered keys stay one-batch-at-a-time. All batches are chunked at the configured thresholds (which normalize to the 1,000-message/10MB API caps).
- Publisher shutdown matches upstream’s drain contract: new publishes fail fast with `.shutdown`, the final flush keeps the full (budget-bounded) retry stack.
- Lease management chunks every acknowledge/modifyAckDeadline RPC at 1,000 ack IDs (upstream `MAX_IDS_PER_RPC`) and flushes early when a full batch accumulates.
- Service-account JWTs mirror upstream `jws.rs` (iat backdated 10s, exp = now + 3610s); impersonation passes no scopes to the source credential, like upstream.
- Known deliberate divergences: the token cache refreshes on demand with transient-only fail-open instead of upstream’s background watch task; at-least-once acks are requeued on transient failures rather than dropped best-effort.
- Known gaps vs upstream (not yet ported): `protocol_version`/server heartbeats (genuinely requires proto regeneration; the vendored descriptor has no such field), adaptive retry throttling, quota-project threading for non-user credentials, and universe-domain validation. One further divergence is deliberate: upstream batches at-least-once and exactly-once acknowledgements into separate RPCs with separate retry configurations (no-backoff/few-attempts versus 1s..64s/600s), while this port batches them together under a single policy.

## Implementation Status

- `Package.swift` bootstrapped for Swift 6.3.
- Pub/Sub and Schema protobuf/gRPC Swift sources vendored under `Sources/PubSub/Generated`.
- Implemented thin admin wrappers:
  - `TopicAdmin`
  - `SubscriptionAdmin`
  - `SchemaService`
- Implemented hand-written publisher surface:
  - `BasePublisher`
  - `Publisher`
  - `PublisherBuilder`
  - `PublisherPartialBuilder`
  - `BatchingOptions`
  - `PublishFuture`
- Implemented hand-written subscriber surface:
  - `Subscriber`
  - `Subscribe`
  - `MessageStream`
  - `ShutdownToken`
  - `Handler`
  - `AtLeastOnce`
  - `ExactlyOnce`
  - `LeaseManager`
- Implemented explicit shutdown paths for publisher, subscriber, admin, schema, and shared service connection clients.
- Implemented configured gRPC client pooling from `grpcSubchannelCount`.
- Implemented local Auth target modeled on `google-cloud-rust/src/auth` for ADC, service-account JWT signing, user ADC refresh-token flow, metadata server, impersonation, anonymous, API key, token caching, and gRPC metadata injection.
- Implemented a Pub/Sub integration testbed executable and wrapper. The default path targets the official Google Cloud Pub/Sub emulator; production-only behavior such as exactly-once confirmation can be enabled explicitly against an isolated real Google Cloud project.

## Validation Plan

- Compile the package with Swift 6.3.
- Run unit tests copied and adapted from upstream Rust publisher/subscriber semantics.
- Run auth unit tests for local ADC-compatible primitives.
- Run `./Scripts/run-pubsub-testbed.sh` for emulator-backed integration coverage when Docker or Google Cloud CLI plus Java is available.
- Run `PUBSUB_TESTBED_ALLOW_REAL=1 PUBSUB_TESTBED_EXACTLY_ONCE=1 PUBSUB_TEST_PROJECT_ID=<project> swift run PubSubTestbed` only against an isolated real project when production-only behavior needs validation.
- Keep `SPEC.md` updated with any architecture or scope adjustments discovered during implementation.

## Verification

Executed across the implementation passes:

- `/Users/pvzig/.codex/skills/swift-format/scripts/run-swift-format.sh Sources`
- `/Users/pvzig/.codex/skills/swift-format/scripts/run-swift-format.sh Tests`
- `/Users/pvzig/.codex/skills/swift-format/scripts/run-swift-format.sh Package.swift`
- `swift build`
- `swift test --parallel`
- `./Scripts/run-pubsub-testbed.sh`

Current automated coverage:

- Publisher batching option defaults and setters.
- Publisher batch flushing on threshold.
- Ordering-key pause and resume behavior after a failed publish.
- Immediate ordering-key resume still rejects messages synchronously admitted while the key was paused.
- `flush()` sees messages published immediately before the call and waits for in-flight batches.
- `flush()` waits for oversized messages rejected locally to reach a terminal future state.
- Publisher shutdown flushes buffered messages.
- Publisher shutdown drains when its caller is already cancelled, and buffered futures remain live after the last `Publisher` value is released.
- Publisher and subscriber call options cover valid Pub/Sub payloads above gRPC's 4 MiB transport default.
- Service connection gRPC subchannel pool sizing.
- Subscriber lease-extension clamping behavior.
- Flow control returns zero when outstanding message capacity is exhausted.
- Auto-nack when an at-least-once handler is dropped.
- Confirmed exactly-once ack completion path.
- Exactly-once lease expiry completes pending confirmed-ack futures with `AckError.leaseExpired`.
- End-to-end `MessageStream.next()` receive from a fake `StreamingPull` service and ack forwarding through the lease manager.
- `StreamingPull` initial request fields for subscription, stream ack deadline, flow-control limits, and client ID.
- Exactly-once handler selection from streaming response subscription properties.
- Exactly-once partial ack failures resolve successful, permanent, and transient IDs independently, and retry exhaustion terminates shutdown.
- Synchronous handler ack admission precedes immediate shutdown classification.
- Subscriber shutdown finishes active message streams before closing the underlying service.
- Lease shutdown drains acknowledgements interrupted during a tracked periodic flush.
- Initial and retried acknowledgement/nack RPC deadlines are drawn from remaining elapsed-time budgets, and exhausted budgets fail without issuing another RPC.
- An ack ID whose retry budget is exhausted fails alone; other IDs sharing its RPC chunk still succeed and receive a deadline drawn from the longest budget in the chunk.
- Acknowledgements released while `.waitForProcessing` shutdown is waiting are still sent, and a handler that is never released bounds shutdown at the grace period instead of at `maxLease`.
- A service rejection arriving on a cancelled shutdown flush keeps its per-ID outcome instead of being replayed as a cancellation.
- A backlog that re-crosses the per-RPC cap while an immediate flush is in flight is flushed again rather than deferred to the periodic tick.
- Ack chunks overlap in flight rather than being issued one after another.
- A lease covered by its last extension is extended once across many sweeps instead of once per sweep.
- Streaming response tails are registered and lease-extended even when a configured message-capacity boundary is already saturated; byte-capacity saturation is covered independently.
- `Handler.deliveryAttempt` reports the service's count and distinguishes an unreported field from zero attempts.
- Exactly-once acknowledgements receive upstream's 600-second default budget.
- Routing metadata is formatted as the resource fields Pub/Sub routes on.
- Structured exactly-once `ErrorInfo` reason, domain, and metadata survive conversion into the public service error, stay stable across repeated reads of the deferred decode, and compare equal to an explicitly constructed error; errors without status details decode to empty info.
- Malformed custom endpoints and malformed emulator hosts, including out-of-range and negative ports, fail construction instead of silently defaulting or reaching the transport.
- Subscribing after `Subscriber.shutdown()` surfaces a `PubSubServiceError` instead of finishing an empty stream silently.
- Negative, non-finite, and extreme retry backoff configuration is normalized without trapping.
- Streaming reconnect and shared RPC retry loops honor elapsed-time budgets.
- Authorization session caching and expired-session refresh.
- Token cache separation across distinct requested scope sets.
- API-key credential metadata generation.
- Service-account key decoding for Google ADC JSON field names.
- OAuth refresh request form encoding and caller-owned authorization shutdown behavior.
- Malformed metadata server hosts, including out-of-range ports, surface credential errors without process termination, and an empty `GCE_METADATA_HOST` falls back to the default metadata server.

Prior validation (2026-07-25):

- Formatted every changed Swift source and test file with the repository's required `swift-format` skill workflow.
- Ran `swift test --parallel`: all 78 tests passed across the Auth and PubSub test targets.
- Verified malformed endpoints, invalid retry configuration, bounded initial ack/nack attempts, and structured exactly-once errors with focused regressions.
- Mutation-checked the shutdown, per-ID retry budget, cancellation-classification, flush-coalescing, lease-extension-tracking, and flush-concurrency regressions: each one fails against the previous behavior it was written to pin down.
- Validated Linux support in a `swift:6.3-noble` container against a read-only source mount: `swift build` and `swift test` both succeed and all 78 tests pass. The container surfaced two Darwin-invisible breaks that a macOS build cannot: glibc's `stderr` under strict concurrency, and `ByteBuffer`'s `Data` bridging living in `NIOFoundationCompat`.
- Verified all 32 `x-goog-request-params` routing keys against the `google.api.http` path parameters in `googleapis/google/pubsub/v1/{pubsub,schema}.proto`.
- Ran `git diff --check` after the review fixes.

Prior validation (2026-09-02):

- Verified `apple/swift-log` 1.15.0 as the latest GitHub release before adding the direct dependency.
- Formatted every modified Swift source and test with the required `swift-format` skill workflow; vendored generated protobuf sources were excluded.
- Ran the required `swift-test` skill workflow: all 85 PubSub tests and all 21 Auth tests passed (106 total).
- Covered permanent/transient auth mapping and fail-open, whole-response HTTP deadlines, dynamic refresh slack and quota projects, response-wide leasing and shutdown nacks, idle reconnect resets, independent lease sweeps and shutdown phases, exact per-ID outcomes, ordered publisher flush and shared ownership, payload limits, configuration mutation, queue retention, and public admin/credential surfaces.
- Validated `Scripts/run-pubsub-testbed.sh` with `bash -n` and verified the final patch with `git diff --check`.

Review-fix validation (2026-09-04):

- Added regression cases for unrelated `ErrorInfo` reasons and domains across confirmed ack and nack, checking exact service errors and two-attempt retry exhaustion.
- Added malformed ADC JSON, missing-field, invalid-private-key, and nested impersonation cases, checking permanent credential classification and UNAUTHENTICATED mapping; a separate assertion preserves unsupported credential errors.
- Before applying the fixes, all six acknowledgement cases and all six malformed-credential cases failed against the staged implementation; the unsupported-credential assertion passed.
- Formatted all four modified Swift files with the required `swift-format` skill workflow.
- Ran the required `swift-test` skill workflow (`swift test --parallel`) on macOS with Swift 6.4: all 86 PubSub tests and all 23 Auth tests passed (109 total), including the new regressions and existing exactly-once partial-outcome coverage.
- Verified the final patch with `git diff --check`; validation used local fake services, with no live-provider or emulator run for these fixes.

README documentation (2026-09-04):

- Added a root README covering the implemented public API and current limitations, with local SwiftPM installation because this checkout has no configured Git remote.
- Examples document explicit client cleanup, publisher future errors, handler consumption, exactly-once confirmation, and per-phase subscriber shutdown grace periods; emulator details link to the existing testbed runbook.
- Validated the installation snippets with `swift package dump-package` and typechecked the publishing, subscribing, and acknowledgement examples against the current built modules. Checked relative links, code fences, and `git diff --check` (including staged changes). Examples were not executed against a service; Swift formatting and the test suite were not rerun for this Markdown-only update.

Current emulator-backed testbed coverage:

- Topic creation and lookup.
- Subscription creation and lookup.
- Publisher batching, `flush()`, and publish futures.
- End-to-end `StreamingPull` receive through `MessageStream`.
- Handler `ack()` forwarding through the lease manager.
- Handler `nack()` and redelivery.
- Explicit client, stream, and emulator shutdown.
- Optional real-service exactly-once confirmed ack.
