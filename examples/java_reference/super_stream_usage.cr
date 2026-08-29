require "./support"

environment = JavaReferenceExamples.environment
super_stream = JavaReferenceExamples.unique_name("super-stream")
partitions = 3.times.map { |index| "#{super_stream}-#{index}" }.to_a
binding_keys = ["amer", "emea", "apac"]
count_super_stream = JavaReferenceExamples.unique_name("super-stream-count")
count_partitions = 5.times.map { |index| "#{count_super_stream}-#{index}" }.to_a
producer : Crabbit::SuperStreamProducer? = nil
consumer : Crabbit::SuperStreamConsumer? = nil

begin
  # Crabbit's protocol API names partitions explicitly. Sequential binding keys
  # reproduce Java's `partitions(5)` form; named keys reproduce `bindingKeys`.
  environment.create_super_stream(
    count_super_stream,
    count_partitions,
    5.times.map(&.to_s),
  )
  environment.create_super_stream(super_stream, partitions, binding_keys)

  received = Channel(String).new(8)
  consumer = environment.super_stream_consumer(
    super_stream,
    Crabbit::ConsumerOptions.new(offset: Crabbit::OffsetSpecification.first),
  ) { |delivery| received.send(String.new(delivery.body)) }

  # Default seeded Murmur3 hash routing.
  producer = environment.super_stream_producer(
    super_stream,
    Crabbit::SuperStreamProducerOptions.new(
      ->(message : Crabbit::Message) { message.properties.not_nil!.message_id.not_nil!.to_s },
    ),
  )
  producer.publish(
    Crabbit::Message.new("default-hash", properties: Crabbit::Properties.new(message_id: "invoice-1")),
  ).await.each { |confirmation| JavaReferenceExamples.confirmed!(confirmation) }
  producer.close

  # Caller-provided hash function.
  producer = environment.super_stream_producer(
    super_stream,
    Crabbit::SuperStreamProducerOptions.new(
      ->(message : Crabbit::Message) { message.properties.not_nil!.message_id.not_nil!.to_s },
      hash_function: ->(key : String) { key.to_slice.sum(&.to_u32) },
    ),
  )
  producer.publish(
    Crabbit::Message.new("custom-hash", properties: Crabbit::Properties.new(message_id: "invoice-2")),
  ).await.each { |confirmation| JavaReferenceExamples.confirmed!(confirmation) }
  producer.close

  # Binding-key routing asks the broker to resolve the route.
  producer = environment.super_stream_producer(
    super_stream,
    Crabbit::SuperStreamProducerOptions.new(
      ->(message : Crabbit::Message) do
        JavaReferenceExamples.string_property(message, "region").not_nil!
      end,
      routing: Crabbit::SuperStreamRouting::RoutingKey,
    ),
  )
  routed = Crabbit::Message.new(
    "binding-key",
    application_properties: {"region" => Crabbit::AMQP::Value.wrap("emea")},
  )
  producer.publish(routed).await.each { |confirmation| JavaReferenceExamples.confirmed!(confirmation) }
  producer.close

  # A completely custom routing strategy receives the current partition list.
  next_partition = 0
  strategy = ->(_message : Crabbit::Message, current : Array(String)) do
    selected = current[next_partition % current.size]
    next_partition += 1
    [selected]
  end
  producer = environment.super_stream_producer(
    super_stream,
    Crabbit::SuperStreamProducerOptions.new(routing_strategy: strategy),
  )
  producer.publish(Crabbit::Message.new("custom-routing")).await.each do |confirmation|
    JavaReferenceExamples.confirmed!(confirmation)
  end
  producer.close

  values = JavaReferenceExamples.await_count(received, 4, 30.seconds)
  expected = ["default-hash", "custom-hash", "binding-key", "custom-routing"].sort
  raise "unexpected super stream deliveries #{values}" unless values.sort == expected
  consumer.close

  # Single Active Consumer with manual broker offset tracking.
  sac_received = Channel(String).new(1)
  consumer = environment.super_stream_consumer(
    super_stream,
    Crabbit::ConsumerOptions.new(
      name: JavaReferenceExamples.unique_name("super-sac"),
      offset: Crabbit::OffsetSpecification.first,
      single_active_consumer: true,
      on_consumer_update_context: ->(context : Crabbit::ConsumerUpdateContext) do
        if context.active
          if stored = context.consumer.stored_offset
            Crabbit::OffsetSpecification.offset(stored &+ 1_u64)
          else
            Crabbit::OffsetSpecification.first
          end
        else
          Crabbit::OffsetSpecification.none
        end
      end,
    ),
  ) do |delivery|
    delivery.stream
    sac_received.send(String.new(delivery.body))
  end
  JavaReferenceExamples.await(sac_received, 30.seconds)
  consumer.close

  # External offset tracking uses the partition stream supplied by Delivery.
  external_offsets = {} of String => UInt64
  external_mutex = Mutex.new
  external_received = Channel(String).new(1)
  consumer = environment.super_stream_consumer(
    super_stream,
    Crabbit::ConsumerOptions.new(
      name: JavaReferenceExamples.unique_name("super-sac-external"),
      offset: Crabbit::OffsetSpecification.first,
      single_active_consumer: true,
      on_consumer_update_context: ->(context : Crabbit::ConsumerUpdateContext) do
        next_offset = external_mutex.synchronize { external_offsets[context.stream]? }
        if context.active && next_offset
          Crabbit::OffsetSpecification.offset(next_offset &+ 1_u64)
        elsif context.active
          Crabbit::OffsetSpecification.first
        else
          Crabbit::OffsetSpecification.none
        end
      end,
    ),
  ) do |delivery|
    external_mutex.synchronize { external_offsets[delivery.stream] = delivery.offset }
    external_received.send(String.new(delivery.body))
  end
  JavaReferenceExamples.await(external_received, 30.seconds)
  puts "Super stream examples completed"
ensure
  producer.try(&.close)
  consumer.try(&.close)
  JavaReferenceExamples.delete_super_stream(environment, super_stream)
  JavaReferenceExamples.delete_super_stream(environment, count_super_stream)
  environment.close
end
