# Crabbit

[![CI](https://github.com/mikeoz32/crabbit/actions/workflows/ci.yml/badge.svg)](https://github.com/mikeoz32/crabbit/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/mikeoz32/crabbit)](https://github.com/mikeoz32/crabbit/releases)
[![API documentation](https://img.shields.io/badge/docs-API%20reference-blue)](https://mikeoz32.github.io/crabbit/)
[![shards.info](https://img.shields.io/badge/shards.info-crabbit-blue)](https://shards.info/github/mikeoz32/crabbit/readme)

`crabbit` is a native, asynchronous RabbitMQ Streams client for Crystal. It implements the Stream binary protocol directly, including AMQP 1.0 message encoding, publisher confirms, consumer credit, recovery, filtering, compression, offset tracking, and super streams.

Crystal 1.20 or newer is required. Crystal 1.20 applications can opt into execution contexts with `-Dpreview_mt -Dexecution_context`; Crystal 1.21 enables them by default. Crabbit uses ordinary fibers, channels, and non-blocking socket I/O, so it works in the default context and in caller-created concurrent or parallel contexts.

## Installation

Add the shard:

```yaml
dependencies:
  crabbit:
    github: mikeoz32/crabbit
    version: ~> 0.1.0
```

Then run `shards install`. Linux builds need the LZ4 runtime/development library available to the linker (`liblz4-dev` on Debian/Ubuntu). Gzip, Snappy, LZ4, and Zstandard codecs are registered by default; codecs can be replaced through `CompressionCodecs`.

## Documentation

- [Generated Crystal API reference](https://mikeoz32.github.io/crabbit/)
- [Guide index](https://github.com/mikeoz32/crabbit/blob/main/docs/README.md)
- [Getting started](https://github.com/mikeoz32/crabbit/blob/main/docs/getting-started.md)
- [Publishing](https://github.com/mikeoz32/crabbit/blob/main/docs/publishing.md) and [consuming](https://github.com/mikeoz32/crabbit/blob/main/docs/consuming.md)
- [Configuration and TLS](https://github.com/mikeoz32/crabbit/blob/main/docs/configuration.md)
- [Super streams](https://github.com/mikeoz32/crabbit/blob/main/docs/super-streams.md)
- [OAuth 2](https://github.com/mikeoz32/crabbit/blob/main/docs/oauth2.md)
- [AMQP 1.0 messages and codec](https://github.com/mikeoz32/crabbit/blob/main/docs/amqp-codec.md)
- [Operations, recovery, and errors](https://github.com/mikeoz32/crabbit/blob/main/docs/operations.md)
- [Protocol support matrix](https://github.com/mikeoz32/crabbit/blob/main/docs/protocol-support.md)

The API site is generated directly from Crystal doc comments with `crystal docs` on every push to `main`.

## Publish and consume

```crystal
require "crabbit"

environment = Crabbit::Environment.connect(
  "rabbitmq-stream://guest:guest@localhost:5552/%2f"
)

environment.create_stream("events")

consumer = environment.consumer(
  "events",
  Crabbit::ConsumerOptions.new(
    name: "billing",
    offset: Crabbit::OffsetSpecification.first,
    initial_credit: 10_u16,
  ),
) do |delivery|
  puts String.new(delivery.body)
end

producer = environment.producer(
  "events",
  Crabbit::ProducerOptions.new(
    name: "billing-api",
    batch_size: 100,
    max_unconfirmed: 10_000,
  ),
)

confirmation = producer.publish("invoice-created").await
raise confirmation.error.not_nil! unless confirmation.confirmed

producer.close
consumer.close
environment.close
```

`publish` accepts `Message`, `RawMessage`, `Bytes`, or `String`. `Bytes` and `String` are wrapped in an AMQP 1.0 Data section. `RawMessage` sends already encoded AMQP bytes unchanged.

The returned `PublishHandle` supports both `await` and `on_confirm`. `max_unconfirmed` is a hard backpressure limit: by default the publishing fiber waits for capacity; set `enqueue_timeout` to fail with `TimeoutError` instead. Broker confirms, broker errors, and confirmation timeouts resolve each handle exactly once.

Confirmation callbacks run asynchronously outside the connection reader. With
execution contexts enabled they use a dedicated concurrent context; plain
Crystal 1.20 uses one long-lived dispatcher fiber. In both modes a producer
avoids allocating a fiber for every confirmed message.

## Message API

`Message` supports the complete AMQP 1.0 section model used by Streams:

- header, delivery annotations, message annotations, properties, application properties, footer;
- Data, AMQP Sequence, and AMQP Value bodies;
- all AMQP primitive and container values;
- unknown described sections, preserved in `extra_sections` when decoding and re-encoding.

Use `Message#to_amqp` and `Message.from_amqp` when an application needs direct codec access. Like Crystal's JSON encoder, AMQP can write directly to any `IO` without creating an intermediate `Bytes` value:

```crystal
message.to_amqp(socket)
```

Decoding copies binary values by default. Use `Message.from_amqp(bytes, zero_copy: true)` when the input buffer remains alive and will not be mutated; consumer deliveries use this mode against their owned raw message buffer.

## Batching, compression, and filtering

```crystal
producer = environment.producer(
  "events",
  Crabbit::ProducerOptions.new(
    batch_size: 200,
    sub_entry_size: 20,
    compression: Crabbit::Compression::Zstd,
  ),
)
```

Sub-entry batching supports `None`, `Gzip`, `Snappy`, `Lz4`, and `Zstd`. A broker confirmation for a sub-entry batch resolves every logical message handle in that batch. Crabbit splits outbound batches at the negotiated maximum frame size and rejects a single message that cannot fit.

Filtering uses Publish version 2 and cannot be combined with sub-entry batching:

```crystal
producer.publish(message, filter: "eu")

consumer = environment.consumer(
  "events",
  Crabbit::ConsumerOptions.new(filters: ["eu"], match_unfiltered: false),
) { |delivery| process(delivery.message) }
```

## Consumer flow and offsets

Callback consumers acknowledge processing after the callback returns. `Consumer#each` does the same after each yielded block. For explicit control, call `receive` and later `delivery.processed!`.

Credit is replenished only after every logical delivery in a broker chunk is processed. With `concurrency > 1`, callbacks run in multiple fibers while the recovery offset advances only across a contiguous prefix of broker-delivered messages. Their numeric stream offsets can have gaps when server-side filtering skips chunks.

Named consumers can call `store_offset`. Automatic storage is disabled by default; enable it explicitly with `auto_store_every` and/or `auto_store_interval`. On recovery, the consumer re-resolves topology and resumes at the first stream offset after the last processed delivery in that prefix. A Single Active Consumer can supply `on_consumer_update`; otherwise a named consumer looks up its stored offset when it becomes active.

## Recovery

Producer and consumer recovery is infinite until the resource is closed. It uses exponential backoff with jitter, refreshes stream metadata, reconnects to the current leader/replica, and redeclares the protocol entity. `on_state_change` receives `Open`, `Recovering`, and `Closed` events.

Unconfirmed producer messages are republished by default. Use a named producer for broker-side deduplication; an unnamed producer has at-least-once recovery semantics and can produce duplicates when the connection outcome is unknown. Set `retry_on_recovery: false` to fail unconfirmed handles instead.

For a TCP load balancer, set `load_balancer: true` in `Configuration`. Crabbit connects through the configured entrypoints until the broker's `advertised_host` and `advertised_port` match the metadata-selected node.

## OAuth 2

Crabbit can retrieve OAuth 2 access tokens, share one token across an
environment, refresh it before expiration, and re-authenticate every open
Stream connection with SASL PLAIN:

```crystal
oauth2 = Crabbit::OAuth2Config.new(
  "https://identity.example/oauth/token",
  client_id: "billing-service",
  client_secret: ENV["OAUTH2_CLIENT_SECRET"],
  parameters: {"audience" => "rabbitmq"},
)

environment = Crabbit::Environment.connect(
  "rabbitmq-stream+tls://rabbitmq.example:5551/%2f",
  oauth2: oauth2,
)
```

The default grant type is `client_credentials`; use `grant_type` and
`parameters` for provider-specific forms. `tls_context` configures certificate
verification for the token endpoint independently of Stream TLS. Tokens are
refreshed after 80% of their advertised `expires_in` by default. Token retrieval
and live connection re-authentication failures use exponential backoff from
`refresh_retry_delay` up to `refresh_retry_max_delay` while connections remain
registered.

OAuth 2 requires HTTPS for the token endpoint and TLS for every Stream endpoint
by default. Isolated local tests can explicitly set
`allow_insecure_transport: true`; do not use that opt-in in production. For a
provider-specific token flow, implement `OAuth2TokenProvider` and pass it to
`OAuth2SaslAuthenticator`.

## Super streams

```crystal
producer = environment.super_stream_producer(
  "orders",
  Crabbit::SuperStreamProducerOptions.new(
    ->(message : Crabbit::Message) { message.properties.not_nil!.group_id.not_nil! },
    routing: Crabbit::SuperStreamRouting::Hash,
  ),
)

producer.publish(message).await
```

Hash routing is byte-compatible with the RabbitMQ Java client's seeded Murmur3 strategy. `RoutingKey` mode uses the protocol Route command. Partition producer/consumer sets refresh dynamically; Single Active Consumer subscriptions carry the super-stream subscription property.

## Management

`Environment` exposes stream create/delete/stats, offset query/store/resolve, super-stream create/delete, partition lookup, and routing lookup. Wire classes live under `Crabbit::Internal` and are intentionally not part of the compatibility contract.

## Testing

```bash
crystal spec
scripts/integration.sh
```

The integration suite starts the pinned RabbitMQ image from `docker-compose.yml`, enables the Stream and OAuth 2 plugins, and exercises every compression codec, filtering, named publisher sequences, consumer credit, super streams, verified-HTTPS OAuth 2 token retrieval, shared refresh, and live re-authentication of multiple open connections. Override the image with `RABBITMQ_IMAGE=rabbitmq:latest`; CI runs this lane as allowed-to-fail in addition to the pinned stable image.

## Benchmarks

```bash
shards build --release crabbit-benchmark
bin/crabbit-benchmark codec

docker compose up --detach --wait
bin/crabbit-benchmark broker
benchmarks/compare.sh
```

Results are newline-delimited JSON with message/s, MiB/s, elapsed time, latency p50/p95/p99, and Crystal allocation totals. The codec lane reports the allocating `to_amqp : Bytes` API, direct writes to a reused `IO`, safe-copy and zero-copy decoding, and messages containing header, properties, and application-properties sections. `compare.sh` runs a 500,000-message publish-confirm workload against RabbitMQ's Java Stream client 1.9.0 with matching 1 KiB payloads, fixed batches of 100, and 10,000 outstanding confirms. It uses local Maven when available and otherwise runs Maven/JDK 21 through Docker. Benchmarks are informational and intentionally have no pass/fail performance threshold.

Set `CRABBIT_BENCH_VARIANT` to `bytes-callback` (the default), `bytes-throughput`, or `raw-throughput` to isolate callback dispatch, AMQP data-section encoding, and the pre-encoded wire path.

## License

MIT
