# Consuming

A `Consumer` subscribes to one stream, validates and parses Deliver chunks, exposes logical messages, replenishes broker credit, and recovers after topology or connection changes.

## Callback mode

```crystal
consumer = environment.consumer(
  "events",
  Crabbit::ConsumerOptions.new(
    name: "billing-worker",
    offset: Crabbit::OffsetSpecification.first,
    concurrency: 8,
  ),
) do |delivery|
  persist(delivery.message)
end
```

The callback runs on handler fibers. Crabbit calls `Delivery#processed!` in an `ensure` after the callback returns. This includes callbacks that raise: exceptions are logged, but the delivery is considered processed. If failed work must not advance processing, catch application exceptions inside the callback and persist or retry the work before returning.

## Pull mode

```crystal
consumer = environment.consumer("events")

loop do
  delivery = consumer.receive
  begin
    persist(delivery.message)
    delivery.processed!
  rescue ex
    report(ex, delivery.offset)
  end
end
```

`receive` raises `ResourceClosedError` after close. `receive?` returns `nil` after the queue drains. Pull consumers own acknowledgement and must call `processed!`; forgetting it eventually prevents the associated chunk credit from being replenished.

`Consumer#each` is a middle ground:

```crystal
consumer.each do |delivery|
  persist(delivery.message)
end
```

Like callback mode, `each` marks the delivery processed after the block exits, including exceptional exits.

## Delivery data and ownership

`Delivery#raw` is the complete encoded AMQP message. `Delivery#message` decodes lazily in zero-copy mode and caches the result. Binary AMQP values and Data sections may therefore reference `raw`; keep the delivery or decoded message alive while using those slices and do not mutate `raw`.

`Delivery#body` is a shortcut for the logical Data body. It returns an empty slice for Sequence or Value bodies. Use `message.body_kind`, `message.sequences`, and `message.value` when body type matters.

## Offset specifications

```crystal
Crabbit::OffsetSpecification.first
Crabbit::OffsetSpecification.last
Crabbit::OffsetSpecification.next
Crabbit::OffsetSpecification.offset(42)
Crabbit::OffsetSpecification.timestamp(30.minutes.ago)
```

- `first` starts at the oldest retained message.
- `last` starts at the beginning of the last committed chunk, not necessarily one message.
- `next` starts after the current tail and waits for new messages.
- `offset` starts at an exact absolute offset.
- `timestamp` asks RabbitMQ for the first chunk at or after a time.

## Credit and buffering

`initial_credit` is measured in broker chunks. One chunk can contain many logical messages. Crabbit sends one replacement credit only after every delivered logical message in that chunk is processed.

`buffer_size` bounds the logical delivery queue between the parser and application. It should accommodate the expected chunk size and handler concurrency. If incoming chunk credit cannot be safely buffered, Crabbit treats it as a protocol-flow failure and recovers the connection instead of allocating without a bound.

## Concurrency and recovery position

Callback `concurrency` controls the number of handler fibers. Deliveries may finish out of order. Crabbit records broker delivery order and advances the recoverable prefix only when all earlier delivered messages are processed.

Numeric offsets are not assumed contiguous. RabbitMQ server-side filtering can deliver offsets such as `0, 3, 5`; Crabbit tracks the registered delivery order so those gaps do not cause already processed messages to replay.

## Stored offsets

Offset storage requires a stable consumer `name`:

```crystal
options = Crabbit::ConsumerOptions.new(
  name: "billing-worker",
  auto_store_every: 1_000,
  auto_store_interval: 10.seconds,
)
```

Both automatic triggers may be enabled together. Crabbit stores only the latest contiguous processed offset and suppresses duplicate stores. On recovery it resumes at the following absolute offset.

Manual storage is also available:

```crystal
delivery = consumer.receive
persist(delivery.message)
delivery.processed!
consumer.store_offset(delivery)
```

`stored_offset` queries the broker and returns `nil` when no offset exists.

## Filtering

```crystal
options = Crabbit::ConsumerOptions.new(
  filters: ["eu", "uk"],
  match_unfiltered: false,
)
```

Filter values are OR-matched by RabbitMQ. `match_unfiltered` controls whether messages published without a filter value are also delivered. Filtering happens at chunk level and can still yield false positives; applications should retain domain-level validation when correctness depends on the predicate.

## Single Active Consumer

Single Active Consumer requires a consumer name:

```crystal
options = Crabbit::ConsumerOptions.new(
  name: "billing-sac",
  single_active_consumer: true,
  on_consumer_update_context: ->(context : Crabbit::ConsumerUpdateContext) {
    if context.active
      stored = context.consumer.stored_offset
      stored ? Crabbit::OffsetSpecification.offset(stored + 1) :
        Crabbit::OffsetSpecification.first
    else
      Crabbit::OffsetSpecification.none
    end
  },
)
```

Without a callback, an active named consumer queries its stored offset and otherwise falls back to the configured offset. The legacy `on_consumer_update` callback receives only the active flag; prefer the context-aware form for new code.

## Recovery

On disconnect or relevant metadata update, Crabbit refreshes topology, tries available replicas and then the leader, resubscribes, and resumes after the latest contiguous processed offset. Recovery continues with `RecoveryPolicy` backoff until the consumer closes.
