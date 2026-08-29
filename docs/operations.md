# Operations, recovery, and errors

## Resource ownership

`Environment` owns its pooled network connections and tracks every producer, consumer, and super-stream resource it creates. Shutdown may be explicit at each level:

```crystal
begin
  environment = Crabbit::Environment.connect(uri)
  producer = environment.producer("events")
  consumer = environment.consumer("events")
  # ...
ensure
  producer.try(&.close)
  consumer.try(&.close)
  environment.try(&.close)
end
```

All close operations are idempotent. Closing an environment closes registered entities before network connections. Closing a producer resolves its pending handles with `ResourceClosedError`; closing a consumer drains and closes its delivery channel.

## Recovery state

Producers and consumers expose `Open`, `Recovering`, and `Closed` states. Connection loss and relevant stream metadata updates move a resource to `Recovering`. Recovery refreshes metadata and redeclares the protocol entity with exponential backoff and jitter until it succeeds or the application closes the resource.

```crystal
events = Channel(Crabbit::ResourceEvent).new

options = Crabbit::ProducerOptions.new(
  on_state_change: ->(event : Crabbit::ResourceEvent) {
    events.send(event)
  },
)
```

State callbacks run asynchronously. They are operational signals, not a transaction boundary: the connection can change again immediately after a callback is scheduled.

Customize retry timing with `RecoveryPolicy`:

```crystal
policy = Crabbit::RecoveryPolicy.new(
  initial_delay: 500.milliseconds,
  max_delay: 20.seconds,
  multiplier: 1.8,
  jitter: 0.25,
)
```

## Delivery guarantees

Crabbit does not claim exactly-once application processing.

- Publisher confirms establish whether RabbitMQ accepted a publishing ID.
- Named producers allow RabbitMQ to deduplicate a retried publishing ID.
- A connection failure can leave an unnamed publish outcome unknown and cause a duplicate after recovery.
- Consumer offsets represent progress checkpoints; a crash between side effects and offset storage can replay a delivery.
- Storing an offset before durable side effects can lose application work.

For effectively-once processing, combine stable message identifiers, a named producer, idempotent application writes, and offset storage after the durable application transaction.

## Error hierarchy

All library-specific exceptions inherit `Crabbit::Error`:

| Error | Meaning |
| --- | --- |
| `ConfigurationError` | Invalid local option combination or argument |
| `ProtocolError` | Malformed, inconsistent, or unexpected peer data |
| `FrameTooLargeError` | One frame or message cannot fit the negotiated limit |
| `ConnectionError` | TCP, TLS, heartbeat, or connection lifecycle failure |
| `ConnectionClosedError` | Required connection is no longer open |
| `TimeoutError` | Request, enqueue, confirm, or caller wait deadline expired |
| `AuthenticationError` | SASL authentication or re-authentication failure |
| `OAuth2Error` | Token retrieval or token response failure |
| `BrokerError` | RabbitMQ returned a non-success Stream response code |
| `ResourceClosedError` | Application used a permanently closed resource |
| `CompressionError` | Codec failure or invalid decompressed size |
| `CodecError` | Invalid AMQP or Stream payload encoding |

Management calls raise `BrokerError` directly. Asynchronous publishes report broker and timeout failures through `Confirmation#error`. Recovery handles transient connection failures internally while emitting state changes.

## Logging

Crabbit logs under the `crabbit` source. Configure it with Crystal's standard `Log` facilities:

```crystal
Log.setup("crabbit", :info)
```

- `debug` includes cleanup failures that do not change application outcome.
- `warn` reports recovery attempts, token refresh failures, and retryable processing failures.
- `error` reports exceptions raised by user callbacks or unexpectedly stopped background workers.

Never enable logging that exposes URI credentials, OAuth client secrets, access tokens, or raw sensitive message payloads in surrounding application code.

## Health and readiness

`Environment#closed?` only reports permanent shutdown. For messaging readiness, observe individual producer or consumer state. `Producer#unconfirmed_count` is useful for drain monitoring, but a zero value alone does not prove the producer is currently connected.

A graceful publisher shutdown normally follows this sequence:

```crystal
producer.wait_for_confirms(30.seconds)
producer.close
environment.close
```

Keep each `Confirmation` if individual negative outcomes must be audited; `wait_for_confirms` only waits for completion.

## Compatibility verification

Run unit and integration checks before upgrading Crystal, RabbitMQ, or compression dependencies:

```bash
crystal spec
scripts/integration.sh
```

CI tests the pinned RabbitMQ 4.3.5 image and an allowed-to-fail latest RabbitMQ compatibility lane. The integration suite covers connection negotiation, publishing and confirms, consuming and credit, filtering, compression, super streams, recovery-sensitive behavior, and OAuth 2 refresh/re-authentication.

See [protocol support](protocol-support.md) for the wire-level compatibility boundary and [benchmarks](../README.md#benchmarks) for performance commands.
