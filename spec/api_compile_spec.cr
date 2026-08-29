require "./spec_helper"

# Crystal specializes methods lazily. This branch is deliberately runtime-only:
# it makes the complete public API part of every spec build without opening a
# network connection during the test run.
if ENV["CRABBIT_COMPILE_API"]? == "1"
  oauth2 = Crabbit::OAuth2Config.new(
    "https://identity.example/token",
    client_id: "client",
    client_secret: "secret",
  )
  Crabbit::Configuration.parse(
    "rabbitmq-stream+tls://localhost:5551/%2f",
    oauth2: oauth2,
  ).authenticator

  environment = Crabbit::Environment.connect
  producer = environment.producer("events", Crabbit::ProducerOptions.new)
  producer.publish("hello").on_confirm { |confirmation| confirmation.confirmed }
  producer.publish("hello", publishing_id: 42_u64).await
  producer.publish(Crabbit::RawMessage.new(Bytes[0x00_u8])).await
  producer.publish_confirmed(Crabbit::Message.new("hello"))
  producer.wait_for_confirms

  consumer = environment.consumer("events", Crabbit::ConsumerOptions.new) do |delivery|
    delivery.message
    delivery.body
  end
  consumer.receive.processed!
  consumer.store_offset(0_u64)
  consumer.stored_offset

  super_options = Crabbit::SuperStreamProducerOptions.new(->(message : Crabbit::Message) { message.properties.try(&.subject) || "" })
  super_producer = environment.super_stream_producer("events-super", super_options)
  super_producer.publish(Crabbit::Message.new("hello")).await
  custom_super_options = Crabbit::SuperStreamProducerOptions.new(
    hash_function: ->(key : String) { key.bytesize.to_u32 },
    routing_key_extractor: ->(_message : Crabbit::Message) { "key" },
  )
  custom_super_producer = environment.super_stream_producer("events-super", custom_super_options)
  custom_super_producer.close
  super_consumer = environment.super_stream_consumer(
    "events-super",
    Crabbit::ConsumerOptions.new,
  ) { |delivery| delivery.processed! }

  super_consumer.close
  super_producer.close
  consumer.close
  producer.close
  environment.close
end

describe "Crabbit public API" do
  it "validates producer and consumer limits" do
    expect_raises(Crabbit::ConfigurationError) { Crabbit::ProducerOptions.new(batch_size: 0) }
    expect_raises(Crabbit::ConfigurationError) { Crabbit::ConsumerOptions.new(concurrency: 0) }
    expect_raises(Crabbit::ConfigurationError) { Crabbit::SuperStreamProducerOptions.new }
    expect_raises(Crabbit::ConfigurationError) do
      Crabbit::ConsumerOptions.new(
        on_consumer_update: ->(_active : Bool) { Crabbit::OffsetSpecification.next },
        on_consumer_update_context: ->(_context : Crabbit::ConsumerUpdateContext) do
          Crabbit::OffsetSpecification.next
        end,
      )
    end
  end
end
