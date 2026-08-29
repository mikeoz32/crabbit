require "./support"

environment = JavaReferenceExamples.environment
stream = JavaReferenceExamples.unique_name("consumer")
producer : Crabbit::Producer? = nil
consumer : Crabbit::Consumer? = nil

begin
  environment.create_stream(stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))
  producer = environment.producer(stream)
  handles = 20.times.map { |index| producer.not_nil!.publish(index.to_s) }.to_a
  handles.each { |handle| JavaReferenceExamples.confirmed!(handle.await) }
  producer.not_nil!.close
  producer = nil

  # Consumer creation and an explicit first offset.
  consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(offset: Crabbit::OffsetSpecification.first),
  )
  first = consumer.not_nil!.receive
  raise "unexpected first message" unless String.new(first.body) == "0"
  first.processed!
  consumer.not_nil!.close

  # Java enables automatic tracking for a named consumer by default. Crabbit
  # makes this opt-in, so both policies are stated explicitly here.
  auto_deliveries = Channel(UInt64).new(5)
  auto_name = JavaReferenceExamples.unique_name("auto-tracking")
  consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(
      name: auto_name,
      offset: Crabbit::OffsetSpecification.first,
      auto_store_every: 3,
      auto_store_interval: 5.seconds,
    ),
  ) { |delivery| auto_deliveries.send(delivery.offset) }
  JavaReferenceExamples.await_count(auto_deliveries, 5)
  consumer.not_nil!.close
  raise "automatic offset was not stored" unless environment.query_offset(auto_name, stream)

  # Custom automatic tracking settings.
  configured_auto_name = JavaReferenceExamples.unique_name("configured-auto")
  configured_auto = Channel(UInt64).new(5)
  consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(
      name: configured_auto_name,
      offset: Crabbit::OffsetSpecification.first,
      auto_store_every: 50_000,
      auto_store_interval: 10.seconds,
    ),
  ) { |delivery| configured_auto.send(delivery.offset) }
  JavaReferenceExamples.await_count(configured_auto, 5)
  consumer.not_nil!.close

  # A name alone means no automatic storage in Crabbit. This is the explicit
  # analogue of choosing Java's default tracking policy.
  named_consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(
      name: JavaReferenceExamples.unique_name("named"),
      offset: Crabbit::OffsetSpecification.first,
    ),
  )
  delivery = named_consumer.receive
  delivery.processed!
  named_consumer.close

  # Manual tracking stores the offset selected by application logic.
  manual_name = JavaReferenceExamples.unique_name("manual")
  consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(
      name: manual_name,
      offset: Crabbit::OffsetSpecification.first,
    ),
  )
  delivery = consumer.not_nil!.receive
  consumer.not_nil!.store_offset(delivery)
  delivery.processed!
  consumer.not_nil!.close
  raise "manual offset was not stored" unless environment.query_offset(manual_name, stream) == delivery.offset

  # Crabbit sends StoreOffset immediately, so Java's manual check interval does
  # not require a separate builder setting.
  interval_name = JavaReferenceExamples.unique_name("manual-interval")
  consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(
      name: interval_name,
      offset: Crabbit::OffsetSpecification.first,
    ),
  )
  delivery = consumer.not_nil!.receive
  consumer.not_nil!.store_offset(delivery.offset)
  delivery.processed!
  consumer.not_nil!.close

  # Subscription listener: consult an external store on every initial attach
  # and recovery, then start after the externally committed offset.
  external_offset = 4_u64
  external = Channel(UInt64).new(1)
  consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(
      subscription_offset: ->(_stream : String) do
        Crabbit::OffsetSpecification.offset(external_offset &+ 1_u64)
      end,
    ),
  ) { |item| external.send(item.offset) }
  external_delivery_offset = JavaReferenceExamples.await(external)
  unless external_delivery_offset == 5_u64
    raise "subscription offset callback returned first delivery #{external_delivery_offset}, expected 5"
  end
  consumer.not_nil!.close

  # Explicit processing controls credit replenishment for asynchronous work.
  consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(offset: Crabbit::OffsetSpecification.first),
  )
  delivery = consumer.not_nil!.receive
  delivery.processed!
  consumer.not_nil!.close

  # Single Active Consumer and its update callback. The context exposes the
  # concrete partition stream and consumer for internal or external tracking.
  sac = Channel(String).new(1)
  consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(
      name: JavaReferenceExamples.unique_name("sac"),
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
  ) { |item| sac.send(String.new(item.body)) }
  JavaReferenceExamples.await(sac)
  puts "Consumer examples completed"
ensure
  producer.try(&.close)
  consumer.try(&.close)
  JavaReferenceExamples.delete_stream(environment, stream)
  environment.close
end
