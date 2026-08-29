require "./support"

environment = JavaReferenceExamples.environment
stream = JavaReferenceExamples.unique_name("producer")
producer : Crabbit::Producer? = nil

begin
  environment.create_stream(stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))

  # Producer creation, byte publishing, and asynchronous confirmation callback.
  confirms = Channel(Crabbit::Confirmation).new(1)
  producer = environment.producer(stream)
  producer.not_nil!.publish("hello".to_slice) { |confirmation| confirms.send(confirmation) }
  JavaReferenceExamples.confirmed!(JavaReferenceExamples.await(confirms))
  producer.not_nil!.close

  # A message with AMQP 1.0 properties.
  producer = environment.producer(stream)
  complex = Crabbit::Message.new(
    "hello",
    properties: Crabbit::Properties.new(
      message_id: JavaReferenceExamples.unique_name("message-id"),
      correlation_id: JavaReferenceExamples.unique_name("correlation-id"),
      content_type: "text/plain",
    ),
  )
  JavaReferenceExamples.confirmed!(producer.not_nil!.publish(complex).await)
  producer.not_nil!.close

  # Named producer, explicit publishing ID, and querying the broker sequence.
  named_options = Crabbit::ProducerOptions.new(
    name: JavaReferenceExamples.unique_name("named-producer"),
    batch_size: 100,
  )
  producer = environment.producer(stream, named_options)
  JavaReferenceExamples.confirmed!(producer.not_nil!.publish("hello", publishing_id: 1_u64).await)
  next_publishing_id = producer.not_nil!.last_publishing_id &+ 1_u64
  3.times do |index|
    id = next_publishing_id &+ index.to_u64
    JavaReferenceExamples.confirmed!(producer.not_nil!.publish("content-#{id}", publishing_id: id).await)
  end
  producer.not_nil!.close

  # Sub-entry batching, first without compression and then with Zstandard.
  producer = environment.producer(
    stream,
    Crabbit::ProducerOptions.new(batch_size: 100, sub_entry_size: 10),
  )
  handles = 20.times.map { |index| producer.not_nil!.publish("sub-entry-#{index}") }.to_a
  handles.each { |handle| JavaReferenceExamples.confirmed!(handle.await) }
  producer.not_nil!.close

  producer = environment.producer(
    stream,
    Crabbit::ProducerOptions.new(
      batch_size: 100,
      sub_entry_size: 10,
      compression: Crabbit::Compression::Zstd,
    ),
  )
  handles = 20.times.map { |index| producer.not_nil!.publish("zstd-sub-entry-#{index}") }.to_a
  handles.each { |handle| JavaReferenceExamples.confirmed!(handle.await) }
  puts "Producer examples completed"
ensure
  producer.try(&.close)
  JavaReferenceExamples.delete_stream(environment, stream)
  environment.close
end
