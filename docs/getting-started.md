# Getting started

Crabbit is a native asynchronous RabbitMQ Streams client. It speaks the Stream binary protocol directly and uses Crystal fibers and non-blocking `IO`; no Java client, AMQP 0-9-1 client, or FFI bridge is involved.

## Requirements

- Crystal 1.20 or newer.
- RabbitMQ with the `rabbitmq_stream` plugin enabled.
- A Stream listener, normally port 5552 for plain TCP or 5551 for TLS.
- The LZ4 library available to the linker (`liblz4-dev` on Debian/Ubuntu).

For local development, this repository provides a broker with the required plugins:

```bash
docker compose up --detach --wait
```

## Install the shard

Add Crabbit to `shard.yml`:

```yaml
dependencies:
  crabbit:
    github: mikeoz32/crabbit
    version: ~> 0.2.0
```

Then install dependencies:

```bash
shards install
```

## Publish and consume

```crystal
require "crabbit"

environment = Crabbit::Environment.connect(
  "rabbitmq-stream://guest:guest@localhost:5552/%2f"
)

stream = "getting-started"
environment.create_stream(stream) unless environment.stream_exists?(stream)

received = Channel(String).new
consumer = environment.consumer(
  stream,
  Crabbit::ConsumerOptions.new(
    name: "getting-started-consumer",
    offset: Crabbit::OffsetSpecification.first,
  ),
) do |delivery|
  received.send(String.new(delivery.body))
end

producer = environment.producer(
  stream,
  Crabbit::ProducerOptions.new(name: "getting-started-producer"),
)

confirmation = producer.publish("hello, stream").await
raise confirmation.error.not_nil! unless confirmation.confirmed

puts received.receive # => hello, stream

producer.close
consumer.close
environment.close
```

`Environment.connect` only constructs the environment. The first management or messaging operation opens the necessary connection. Producers and consumers register with the environment, so `Environment#close` is sufficient for final cleanup; closing each resource explicitly is still useful when its lifetime is shorter than the environment's.

## Choose a consumption style

Callback mode is convenient when every delivery follows the same processing path. Crabbit marks the delivery processed when the callback returns:

```crystal
consumer = environment.consumer("events") do |delivery|
  process(delivery.message)
end
```

Pull mode gives the caller explicit acknowledgement timing:

```crystal
consumer = environment.consumer("events")

while delivery = consumer.receive?
  persist(delivery.message)
  delivery.processed!
end
```

Use `Consumer#each` when Enumerable-style iteration is preferable; it marks each delivery processed after the block returns.

## Next steps

- Read [publishing](publishing.md) before choosing confirmation and recovery settings.
- Read [consuming](consuming.md) before enabling concurrency or automatic offset storage.
- Use [configuration](configuration.md) for production TLS, multiple endpoints, and load balancers.
- Consult the [API reference](https://mikeoz32.github.io/crabbit/) for every option and return type.
