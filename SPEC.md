# Google Cloud Swift

## Scope and structure

Implement Google Cloud Pub/Sub and authentication in Swift, modeled on `googleapis/google-cloud-rust/src/{pubsub,auth}`. The package targets Swift 6.3+, macOS 15+, and Linux. Public API examples and installation live in [README.md](README.md); dependency requirements and resolved versions live in [Package.swift](Package.swift) and [Package.resolved](Package.resolved).

The public repository is `https://github.com/pvzig/google-cloud-swift`. Installation tracks `main` until a tagged release exists.

| Area | Responsibility |
| --- | --- |
| `Sources/Auth` | Credential providers, ADC, token caching, authorization, and gRPC interception. |
| `Sources/PubSub/Generated` | Vendored Pub/Sub and Schema protobuf models and gRPC clients. |
| `Sources/PubSub/Core` | Connection pooling, configuration, retry, errors, payload limits, and async primitives. |
| `Sources/PubSub/Admin` | `TopicAdmin`, `SubscriptionAdmin`, and `SchemaService`, sharing a typed RPC executor. |
| `Sources/PubSub/Publisher` | `Publisher`, `BasePublisher`, batching by ordering key, and publish futures. |
| `Sources/PubSub/Subscriber` | `Subscriber`, `Subscribe`, `MessageStream`, handlers, shutdown tokens, and lease management. |
| `Sources/PubSubTestbed` | Integration scenarios; [runbook](Testbeds/PubSub/README.md) and [emulator wrapper](Scripts/run-pubsub-testbed.sh). |
| `Tests/AuthTests`, `Tests/PubSubTests` | Swift Testing coverage for credentials, transport contracts, publishing, and subscriber lifecycle. |

Use actors, synchronized ingress, tasks, task groups, and async sequences for concurrency. Auth is an in-repo product; SwiftNIO is transitive through the gRPC/JWT dependency graph. `swift-log` provides refresh, retry, reconnect, and lease diagnostics.

## Publisher contracts

- `publish(_:)` admits messages synchronously before returning a `PublishFuture`. Admission and shutdown share one synchronized state; later publishes fail with `.shutdown`.
- `flush()` observes earlier admissions, bypasses delays across ordering keys, and waits for buffered, in-flight, and locally rejected futures to become terminal. Completion handoffs remain tracked across actor suspension. Flush retains completion boxes rather than duplicate payloads.
- Unordered batches run concurrently; ordered keys allow one in-flight batch each. Every batch respects configured thresholds and the 1,000-message/10 MB request caps. A single message may exceed a configured threshold but never the wire-size cap. Exact protobuf sizing includes framing without allocating another payload buffer.
- An ordered publish failure pauses the key and fails its backlog. Resume drains earlier ingress while the key is still paused, preventing rejected messages from overtaking resume. Resume does not resubmit failed messages.
- Shutdown drains with cancellation-insensitive waits and the normal bounded retry policy before closing an owned connection. Buffered work survives release of the last `Publisher` value.
- Derived publishers share their `BasePublisher` connection. Their shutdown drains only their work; base shutdown drains all registered publishers before closing the connection.

## Subscriber contracts

### Streaming and handlers

- `MessageStream` uses bidirectional `StreamingPull`, supplying subscription, ack deadline, outstanding message/byte limits, and a stable client ID across reconnects. Empty keepalive requests are sent every 30 seconds. A successful initial request write resets reconnect attempts and the failure window, including idle streams.
- Await response handling to preserve transport backpressure. Register the complete wire response with the lease manager before yielding any message, so response tails remain leased under message or byte saturation. The capacity helper returns zero at either limit; avoid a second capacity wait that leaves received messages unregistered.
- Iterators retain their stream. Consumed queue entries release handlers and payloads promptly. Subscribing after subscriber shutdown produces a service error instead of an empty stream.
- Streaming subscription properties select at-least-once or exactly-once handlers. `deliveryAttempt` is `nil` when the service omits its dead-letter delivery count.
- Each handler is consumed once. Synchronous ack/nack calls enter the command queue before returning; dropping an unconsumed handler queues a nack. At-least-once ack remains optimistic even after its local lease disappears. Exactly-once operations expose server confirmation through `confirmedAck()`/`confirmedNack()`.

### Leases and acknowledgements

- Track available, pending-ack, and pending-nack states separately. Available exactly-once leases exceeding `maxLease` fail with `AckError.leaseExpired`. Pending acks stay extended until resolved; pending nacks are not extended.
- Lease sweeps start after 500 ms, then run every 3 seconds. Renew only deadlines approaching the next sweep plus a 2-second buffer. Tracked sweep tasks and concurrent chunks prevent a slow renewal from blocking later sweeps. Record successful extension from the request-send instant; bound retries and RPC timeouts to the renewal budget.
- Ack/nack RPCs contain at most 1,000 IDs and run concurrently across chunks. Flush at a full batch or the periodic tick; a backlog crossing the threshold during an immediate flush schedules another round.
- Retry state is per ID. Elapsed budgets begin before the first RPC, include RPC time, and honor backoff. Expired IDs fail independently; a chunk uses the longest remaining ID budget, capped by its shutdown phase deadline.
- Interpret per-ID dispositions only for `ErrorInfo` with reason `EXACTLY_ONCE_ACK_FAILURE` and domain `pubsub.googleapis.com`. Resolve successful and permanent IDs independently; retry transient IDs. Unknown dispositions fail closed. Other metadata, including quota details, follows whole-RPC retry/failure handling and cannot falsely confirm an ack.
- `PubSubServiceError.errorInfo` decodes structured details on demand; repeated reads and equality preserve reason, domain, and metadata. A manually constructed service error without a status code is permanent in the acknowledgement path.

### Shutdown

`Subscriber.shutdown()` waits for every stream worker before closing its connection. Stream cleanup nacks all undelivered registered handlers and any received-but-unregistered messages. Run final RPC work in a fresh task so worker cancellation cannot abort cleanup.

Lease shutdown proceeds in this order:

1. Cancel and join periodic/immediate flush workers. Requeue RPCs actually cancelled by shutdown without charging an attempt; retain real service failures and per-ID outcomes.
2. Under `.waitForProcessing`, keep command ingress open, process consumer acknowledgements, and extend leases until processing finishes or its grace period expires. `.nackImmediately` skips this wait.
3. Close and drain ingress, then drain pending acknowledgements while leases remain extended.
4. Cancel and join all extension work before converting remaining available leases to nacks. Fail their exactly-once confirmation boxes with `AckError.shutdown` before the lifecycle nack RPC can succeed.
5. Drain final nacks, fail unresolved confirmations, and release lease state.

Processing, pending-ack drain, and final-nack drain each receive a fresh grace period, defaulting to 30 seconds. Final RPCs use a positive 1 ms floor at an expired phase deadline; this is not one total shutdown deadline.

## Transport, retry, and authentication

### Transport and retry

- `grpcSubchannelCount` creates a live client pool with round-robin stub assignment. Normalize it to at least one and unary call timeouts to at least 1 ms on every public mutation. StreamingPull has no unary deadline.
- Enforce 10 MB outbound and 20 MB inbound payload limits. The pinned grpc-swift-nio-transport 2.9 uses `maxRequestMessageBytes` for inbound deframing, so both transport fields carry 20 MB and a separate interceptor enforces the outbound cap.
- Every RPC sends `x-goog-request-params` for its routed resource fields, including update-name variants. Percent-encode all resource bytes except RFC 3986 unreserved characters. Routing keys follow the proto HTTP path parameters.
- Validate custom and emulator authorities: HTTP(S), nonempty host, valid TCP port, and no userinfo, non-root path, query, or fragment. Emulator endpoints require an explicit port and force anonymous plaintext connections. Empty emulator environment values are unset.
- Use full-jitter exponential backoff. Default retryable codes are aborted, deadline-exceeded, internal, resource-exhausted, unavailable, and unknown; cancellation is permanent. Normalize negative durations, invalid multipliers, and extreme integer conversions without trapping.
- Default budgets are 10 attempts/60 seconds for admin calls and 600 seconds for publishing and acknowledgements. Explicit caller budgets take precedence. Non-idempotent admin operations retry only UNAVAILABLE. Reconnects honor configured attempt and elapsed budgets.
- Normalize transport errors without RPC status to UNKNOWN while preserving credential messages and recursive cancellation causes. Map permanent credential failures to UNAUTHENTICATED and transient ones to UNAVAILABLE before gRPC erases their type.

### Authentication

- ADC checks `GOOGLE_APPLICATION_CREDENTIALS`, the well-known gcloud file, then the metadata server. Support anonymous, API-key, authorized-user, service-account JWT, metadata-server, and impersonated credentials.
- Token caches separate requested scopes, coalesce refreshes, and refresh on demand. Refresh slack is proportional for short-lived tokens. Only transient refresh errors may reuse a still-valid token; permanent failures surface immediately. Session-mode quota-project headers follow the current provider.
- ADC parsing wraps malformed JSON, missing fields, and invalid private keys in permanent `CredentialsError.parsing`, including nested impersonated source credentials. Preserve existing credential error cases.
- Authorized-user refresh uses form encoding without requested API scopes, preserving the original refresh-token grant. Service-account JWTs backdate `iat` by 10 seconds and set `exp` to now + 3,610 seconds. Impersonation requests no scopes from its source provider.
- Injected `Authorization` remains caller-owned. Automatic clients and impersonated credentials must not shut down the process-wide `DefaultProvider`.
- Auth HTTP uses `AsyncHTTPClient.shared`: no provider owns or closes its connection pool. Bound responses to 1 MiB and the entire exchange, including body collection, to 30 seconds. Transport failures are transient credential errors.
- Empty `GCE_METADATA_HOST` falls back to the default host; malformed hosts/ports fail token acquisition with `CredentialsError.loading` instead of trapping initialization.
- For Linux portability, read `ByteBuffer.readableBytesView` without implicit `NIOFoundationCompat` imports and write diagnostics through `FileHandle.standardError` instead of glibc's mutable `stderr`.

## Upstream alignment and remaining gaps

Prior comparisons used `google-cloud-rust` commit `686c1e2` (2026-04-23) and main as observed on 2026-06-11. Publisher flush, ordered/unordered batching, StreamingPull, and exactly-once lease states follow those contracts. Additional implementation references were `yoshidan/google-cloud-rust/pubsub` and `rosecoder/google-cloud-pubsub-swift`.

Deliberate differences:

- Tokens refresh on demand with transient-only fallback rather than a background watcher.
- Transient at-least-once acknowledgements are requeued rather than dropped best-effort.
- Both acknowledgement modes share batching and the configured 100 ms–60 s backoff; Rust separates their RPC/retry policies and uses 1 s–64 s for exactly-once operations. Swift uses `RetryPolicy` rather than subchannel count to bound lease-management retries.
- Streaming keepalives are written directly by the request producer rather than through a separate request channel.

Remaining gaps are external-account/STS exchange, `protocol_version` and server heartbeats requiring regenerated protos, adaptive retry throttling, quota-project propagation for non-user credentials, and universe-domain validation.

## Validation

For Swift changes, format touched files, run `swift build` and `swift test --parallel`, and finish with `git diff --check`. Add focused regressions for changed contracts above. For documentation changes, validate examples, links, and whitespace; update this specification without rerunning the suite solely for Markdown edits.

Run [Scripts/run-pubsub-testbed.sh](Scripts/run-pubsub-testbed.sh) for emulator integration. The wrapper prefers gcloud plus Java, falls back to Docker, refuses an occupied HTTP port, verifies launcher liveness, and stops the complete process tree on exit. Scenarios cover resource creation/lookup, batching/futures/flush, StreamingPull flow control, ack, nack/redelivery, and shutdown. Real-service exactly-once validation is explicit and uses an isolated project; commands are in the [testbed runbook](Testbeds/PubSub/README.md).

Recorded evidence:

| Scope | Result |
| --- | --- |
| macOS / Swift 6.4, 2026-09-04 | 109 tests passed: 86 PubSub and 23 Auth. All six unrelated-ErrorInfo cases and six malformed-ADC cases failed before their fixes, then passed. Formatting and diff checks passed; no live-provider run. |
| Linux / `swift:6.3-noble`, 2026-07-25 | Build and 78 tests passed using a read-only source mount. This predates later fixes. |
| Earlier protocol and lifecycle checks | All 32 routing keys checked against Pub/Sub/Schema proto paths; shutdown, retry-budget, cancellation, flush, and lease regressions mutation-checked. Emulator scenarios above were exercised; real-service exactly-once remains optional. |
| README examples, 2026-09-04 | Publish, subscribe, and acknowledgement snippets typechecked; links and fences checked. Examples were not executed against a service. |
| Public SwiftPM installation, 2026-09-04 | The complete README manifest resolved `main` at `61086c8` and built a fresh consumer importing Auth and PubSub with Swift 6.4. Documentation links, fences, and whitespace checked; source tests were not rerun for these Markdown edits. |
