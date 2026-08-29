require "./support"

environment = JavaReferenceExamples.environment
stream = JavaReferenceExamples.unique_name("filtering")
producer : Crabbit::Producer? = nil
filtered_consumer : Crabbit::Consumer? = nil
unfiltered_consumer : Crabbit::Consumer? = nil

begin
  environment.create_stream(
    stream,
    Crabbit::StreamOptions.new(initial_cluster_size: 1, filter_size_bytes: 32),
  )

  filter_value = "california"
  filtered = Channel(String).new(3)
  filtered_consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(
      offset: Crabbit::OffsetSpecification.first,
      filters: [filter_value],
    ),
  ) do |delivery|
    message = delivery.message
    # Stream filters are probabilistic, so applications must post-filter.
    if JavaReferenceExamples.string_property(message, "state") == filter_value
      filtered.send(String.new(delivery.body))
    end
  end

  matching_or_unfiltered = Channel(String).new(3)
  unfiltered_consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(
      offset: Crabbit::OffsetSpecification.first,
      filters: [filter_value],
      match_unfiltered: true,
    ),
  ) do |delivery|
    message = delivery.message
    state = JavaReferenceExamples.string_property(message, "state")
    if state == filter_value || state.nil?
      matching_or_unfiltered.send(String.new(delivery.body))
    end
  end

  producer = environment.producer(
    stream,
    Crabbit::ProducerOptions.new(
      filter_value_extractor: ->(message : Crabbit::Message) do
        JavaReferenceExamples.string_property(message, "state")
      end,
    ),
  )

  california = Crabbit::Message.new(
    "invoice-ca",
    application_properties: {"state" => Crabbit::AMQP::Value.wrap(filter_value)},
  )
  new_york = Crabbit::Message.new(
    "invoice-ny",
    application_properties: {"state" => Crabbit::AMQP::Value.wrap("new-york")},
  )
  unfiltered = Crabbit::Message.new("invoice-without-state")
  [california, new_york, unfiltered].each do |message|
    JavaReferenceExamples.confirmed!(producer.publish(message).await)
  end

  raise "filtered consumer missed California" unless JavaReferenceExamples.await(filtered) == "invoice-ca"
  values = JavaReferenceExamples.await_count(matching_or_unfiltered, 2).sort
  expected = ["invoice-ca", "invoice-without-state"].sort
  raise "match_unfiltered produced #{values}" unless values == expected
  puts "Filtering examples completed"
ensure
  producer.try(&.close)
  filtered_consumer.try(&.close)
  unfiltered_consumer.try(&.close)
  JavaReferenceExamples.delete_stream(environment, stream)
  environment.close
end
