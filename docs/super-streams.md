# Super streams

A RabbitMQ super stream is an ordered set of partition streams plus routing bindings. Crabbit manages the topology and creates ordinary producers or consumers per partition.

## Create topology

Partition names and binding keys are positional pairs:

```crystal
environment.create_super_stream(
  "orders",
  ["orders-0", "orders-1", "orders-2"],
  ["0", "1", "2"],
  {"max-age" => "7D"},
)
```

Applications commonly provision topology outside the process. `create_super_stream` and `delete_super_stream` are available for tests, development, or services that explicitly own their topology.

Query the broker directly when needed:

```crystal
partitions = environment.partitions("orders")
selected = environment.route("customer-42", "orders")
```

## Hash routing

Hash routing selects exactly one partition using the same seeded Murmur3 algorithm as the RabbitMQ Stream clients:

```crystal
options = Crabbit::SuperStreamProducerOptions.new(
  ->(message : Crabbit::Message) {
    message.properties.not_nil!.group_id.not_nil!
  },
  routing: Crabbit::SuperStreamRouting::Hash,
  producer: Crabbit::ProducerOptions.new(name: "orders-api"),
)

producer = environment.super_stream_producer("orders", options)
confirmation = producer.publish(message).await.first
```

The partition list order affects hash placement and is supplied by RabbitMQ. `Murmur3.hash32` is public for applications that need to reproduce the mapping. A custom `hash_function` can replace it while retaining modulo partition selection.

## Binding-key routing

Routing-key mode asks RabbitMQ to evaluate the super-stream bindings:

```crystal
options = Crabbit::SuperStreamProducerOptions.new(
  ->(message : Crabbit::Message) { routing_key_for(message) },
  routing: Crabbit::SuperStreamRouting::RoutingKey,
)
```

Route results are cached until topology refresh. A key can resolve to zero, one, or multiple partitions. Publishing to zero partitions raises `BrokerError`; multiple partitions produce one handle and confirmation per partition.

For `RawMessage`, `Bytes`, or `String`, pass an explicit key because no `Message` exists for the extractor:

```crystal
producer.publish(payload, "customer-42")
```

## Custom routing

A custom strategy receives the complete message and a snapshot of partition names:

```crystal
strategy = ->(message : Crabbit::Message, partitions : Array(String)) {
  important?(message) ? partitions : [partitions.first]
}

options = Crabbit::SuperStreamProducerOptions.new(
  routing_strategy: strategy,
)
```

The result may contain one or more current partitions. Duplicates are removed. Returning an unknown partition raises `ConfigurationError`; returning no partitions causes the publish to fail with `StreamNotAvailable`.

## Confirmation model

`SuperStreamPublishHandle#handles` exposes each partition's ordinary `PublishHandle`. `await(timeout)` applies one shared deadline across all handles. `on_confirm` invokes the callback once per partition, so callback code must not assume a single invocation when routing can fan out.

## Consumers

```crystal
consumer = environment.super_stream_consumer(
  "orders",
  Crabbit::ConsumerOptions.new(
    name: "orders-worker",
    offset: Crabbit::OffsetSpecification.first,
    concurrency: 4,
  ),
) do |delivery|
  process(delivery.stream, delivery.offset, delivery.message)
end
```

Crabbit creates one ordinary `Consumer` per partition and passes the super-stream subscription property required by RabbitMQ. `SuperStreamConsumer#consumers` returns a current snapshot for monitoring or explicit offset operations.

Topology refresh adds new partition consumers and closes removed ones. Ordering exists within each partition only; there is no single global order across partition handlers.

For partition-specific starting positions, use `subscription_offset`:

```crystal
options = Crabbit::ConsumerOptions.new(
  subscription_offset: ->(stream : String) {
    checkpoint = checkpoint_for(stream)
    checkpoint ? Crabbit::OffsetSpecification.offset(checkpoint + 1) :
      Crabbit::OffsetSpecification.first
  },
)
```

Single Active Consumer is supported across super-stream partitions when a stable consumer name is supplied.
