require "./support"

include JavaReferenceExamples

message_count = (ENV["CRABBIT_EXAMPLE_MESSAGE_COUNT"]? || "10000").to_i
environment = JavaReferenceExamples.environment
stream = JavaReferenceExamples.unique_name("sample")
producer : Crabbit::Producer? = nil
consumer : Crabbit::Consumer? = nil

begin
  puts "Connecting..."
  environment.create_stream(stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))

  puts "Starting publishing..."
  publish_confirms = Channel(Crabbit::Confirmation).new(message_count)
  producer = environment.producer(stream)
  message_count.times do |index|
    producer.publish(index.to_s) { |confirmation| publish_confirms.send(confirmation) }
  end
  JavaReferenceExamples.await_count(publish_confirms, message_count, 30.seconds).each do |confirmation|
    JavaReferenceExamples.confirmed!(confirmation)
  end
  producer.close
  producer = nil
  puts "Published #{message_count} messages"

  puts "Starting consuming..."
  deliveries = Channel(Int64).new(message_count)
  consumer = environment.consumer(
    stream,
    Crabbit::ConsumerOptions.new(offset: Crabbit::OffsetSpecification.first),
  ) do |delivery|
    deliveries.send(String.new(delivery.body).to_i64)
  end

  sum = JavaReferenceExamples.await_count(deliveries, message_count, 30.seconds).sum
  expected = message_count.to_i64 * (message_count - 1) // 2
  raise "unexpected sum #{sum}, expected #{expected}" unless sum == expected
  puts "Sum: #{sum}"
ensure
  producer.try(&.close)
  consumer.try(&.close)
  JavaReferenceExamples.delete_stream(environment, stream)
  environment.close
end
