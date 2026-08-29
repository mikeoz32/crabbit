# Publishing

`Producer` publishes asynchronously to one stream. Each call reserves backpressure capacity, assigns a publishing ID, queues encoded AMQP bytes, and returns a `PublishHandle`.

## Payload forms

```crystal
producer.publish("UTF-8 string")
producer.publish(Bytes[0x01, 0x02])
producer.publish(Crabbit::Message.new("complete AMQP message"))
producer.publish(Crabbit::RawMessage.new(encoded_amqp))
```

- `String` and `Bytes` become a complete message with one AMQP Data section.
- `Message` encodes all configured AMQP 1.0 sections.
- `RawMessage` is sent unchanged and must already contain a complete valid AMQP message.

Use `Message#to_amqp(io)` when preparing encoded messages for another destination without an intermediate byte allocation. See [AMQP codec](amqp-codec.md).

## Confirmations

Wait in the current fiber:

```crystal
confirmation = producer.publish(message).await
unless confirmation.confirmed
  error = confirmation.error || Crabbit::Error.new("publish was rejected")
  raise error
end
```

Or register a callback:

```crystal
producer.publish(message).on_confirm do |confirmation|
  if confirmation.confirmed
    record_success(confirmation.publishing_id)
  else
    record_failure(confirmation.publishing_id, confirmation.error)
  end
end
```

Callbacks run asynchronously outside the connection reader fiber, so they may publish, close resources, or perform other non-blocking I/O. Exceptions raised by a callback are logged and do not stop confirmation dispatch.

`PublishHandle#await(timeout)` only limits that particular wait; it does not cancel the publish. `ProducerOptions#confirm_timeout` is the producer-level deadline that resolves an unconfirmed publish as failed and releases capacity.

`publish_confirmed` is a convenience wrapper around `publish(...).await`. It returns negative confirmations instead of raising them. `wait_for_confirms` waits until the producer has no unresolved messages but does not return their individual outcomes.

## Backpressure

`max_unconfirmed` is a hard per-producer limit. Once reached, subsequent publishing fibers wait for a permit:

```crystal
options = Crabbit::ProducerOptions.new(
  max_unconfirmed: 20_000,
  enqueue_timeout: 5.seconds,
)
```

Without `enqueue_timeout`, publishing waits indefinitely unless the producer closes. With a timeout, Crabbit raises `TimeoutError` if capacity is not available. This bounds the number of pending payload buffers retained by a producer.

## Wire batching

Ordinary batching combines logical messages into Publish frames:

```crystal
options = Crabbit::ProducerOptions.new(
  batch_size: 200,
  batch_delay: 2.milliseconds,
)
```

The batch is sent when `batch_size` is reached or `batch_delay` expires. Crabbit splits a batch at the negotiated frame limit. A single encoded message that cannot fit raises `FrameTooLargeError` through its confirmation.

## Sub-entry batching and compression

Sub-entry batching packs several logical AMQP messages into one physical Stream entry:

```crystal
options = Crabbit::ProducerOptions.new(
  batch_size: 500,
  sub_entry_size: 50,
  compression: Crabbit::Compression::Zstd,
)
```

Supported algorithms are None, Gzip, Snappy, LZ4, and Zstandard. Each logical message still has its own handle. One broker confirmation for the physical sub-entry resolves all logical publishing IDs in that group.

`sub_entry_size` controls the maximum logical messages per physical entry; `batch_size` still controls messages collected per send cycle. Server-side filtering cannot be combined with sub-entry batching because RabbitMQ attaches one filter value to each physical entry.

## Server-side filtering

Supply a filter explicitly:

```crystal
producer.publish(message, filter: "eu")
```

Or derive it from every `Message`:

```crystal
options = Crabbit::ProducerOptions.new(
  filter_value_extractor: ->(message : Crabbit::Message) {
    message.application_properties["region"]?.try(&.payload.as?(String))
  },
)
```

An explicit `filter:` takes precedence over the extractor. A `nil` value publishes an unfiltered message. Filtering selects Publish protocol version 2 automatically.

## Publishing IDs and deduplication

A named producer asks RabbitMQ for its previous sequence and continues assigning publishing IDs:

```crystal
producer = environment.producer(
  "events",
  Crabbit::ProducerOptions.new(name: "billing-api"),
)
```

RabbitMQ deduplicates repeated publishing IDs for the same producer reference and stream. This is the recommended configuration when messages are retried during recovery.

Advanced applications can provide an explicit `publishing_id:`. IDs must not collide with another unresolved publish in the same producer. Crabbit advances its automatic sequence past an explicit ID when necessary.

## Recovery guarantees

When a connection fails, the producer refreshes metadata, reconnects to the current leader, redeclares itself, and republishes unresolved messages by default.

- Named producer plus stable name: at-least-once transport with broker deduplication by publishing ID.
- Unnamed producer: at-least-once; duplicates are possible when the old connection outcome is unknown.
- `retry_on_recovery: false`: unresolved handles fail instead of being sent again.

Recovery continues with exponential backoff until `Producer#close`. Observe transitions through `on_state_change` when an application needs readiness or alerting signals.
