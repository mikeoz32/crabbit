require "../spec_helper"

private def integration_environment : Crabbit::Environment
  uri = ENV["CRABBIT_URI"]? || "rabbitmq-stream://crabbit:crabbit@localhost:5552/%2f"
  Crabbit::Environment.connect(uri, request_timeout: 15.seconds)
end

private def unique_stream(prefix : String) : String
  "crabbit-#{prefix}-#{Random.rand(UInt64).to_s(16)}"
end

private def receive_count(channel : Channel(T), count : Int32, timeout_span : Time::Span) : Array(T) forall T
  values = [] of T
  deadline = Time.instant + timeout_span
  count.times do
    remaining = deadline - Time.instant
    raise Crabbit::TimeoutError.new("received #{values.size}/#{count} values") if remaining <= Time::Span.zero
    select
    when value = channel.receive
      values << value
    when timeout(remaining)
      raise Crabbit::TimeoutError.new("received #{values.size}/#{count} values")
    end
  end
  values
end

if ENV["CRABBIT_INTEGRATION"]? == "1"
  describe "RabbitMQ Stream integration" do
    it "lets a confirmation callback publish while max_unconfirmed is full" do
      environment = integration_environment
      stream = unique_stream("callback-backpressure")
      environment.create_stream(stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))
      producer = environment.producer(
        stream,
        Crabbit::ProducerOptions.new(batch_size: 1, max_unconfirmed: 1),
      )
      confirmations = Channel(Bool).new(2)

      begin
        producer.publish("first") do |first|
          confirmations.send(first.confirmed)
          producer.publish("second") do |second|
            producer.close
            confirmations.send(second.confirmed)
          end
        end

        receive_count(confirmations, 2, 20.seconds).should eq [true, true]
      ensure
        producer.close
        environment.delete_stream(stream)
        environment.close
      end
    end

    it "owns a Bytes payload until its delayed batch is transmitted" do
      environment = integration_environment
      stream = unique_stream("owned-bytes")
      environment.create_stream(stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))
      producer = environment.producer(
        stream,
        Crabbit::ProducerOptions.new(batch_size: 10, batch_delay: 250.milliseconds),
      )
      consumer = environment.consumer(
        stream,
        Crabbit::ConsumerOptions.new(offset: Crabbit::OffsetSpecification.first),
      )
      payload = "owned-by-producer".to_slice.dup

      begin
        handle = producer.publish(payload)
        payload.fill(0x78_u8)
        handle.await(20.seconds).confirmed.should be_true
        delivery = consumer.receive
        String.new(delivery.body).should eq "owned-by-producer"
        delivery.processed!
      ensure
        consumer.close
        producer.close
        environment.delete_stream(stream)
        environment.close
      end
    end

    it "publishes and consumes every built-in sub-entry compression format" do
      environment = integration_environment
      stream = unique_stream("compression")
      environment.create_stream(stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))
      received = Channel(String).new(100)
      consumer = environment.consumer(
        stream,
        Crabbit::ConsumerOptions.new(offset: Crabbit::OffsetSpecification.first, initial_credit: 2_u16),
      ) do |delivery|
        received.send(String.new(delivery.body))
      end

      begin
        handles = [] of Crabbit::PublishHandle
        Crabbit::Compression.each do |compression|
          producer = environment.producer(
            stream,
            Crabbit::ProducerOptions.new(
              name: "producer-#{compression}",
              batch_size: 20,
              sub_entry_size: 5,
              compression: compression,
            ),
          )
          20.times do |index|
            handles << producer.publish("#{compression}:#{index}")
          end
          handles.last(20).each { |handle| handle.await(20.seconds).confirmed.should be_true }
          producer.close
        end

        values = receive_count(received, 100, 30.seconds)
        values.uniq.size.should eq(100)
      ensure
        consumer.close
        environment.delete_stream(stream)
        environment.close
      end
    end

    it "uses publish v2 filtering and named producer deduplication sequences" do
      environment = integration_environment
      stream = unique_stream("filter")
      environment.create_stream(
        stream,
        Crabbit::StreamOptions.new(initial_cluster_size: 1, filter_size_bytes: 32),
      )
      received = Channel(String).new(30)
      consumer = environment.consumer(
        stream,
        Crabbit::ConsumerOptions.new(
          offset: Crabbit::OffsetSpecification.first,
          filters: ["blue"],
          match_unfiltered: false,
        ),
      ) { |delivery| received.send(String.new(delivery.body)) }

      begin
        producer_options = Crabbit::ProducerOptions.new(name: "deduplicated", batch_size: 10)
        producer = environment.producer(stream, producer_options)
        10.times do |index|
          filter = index.even? ? "blue" : "red"
          producer.publish("first-#{index}", filter).await(20.seconds).confirmed.should be_true
        end
        producer.close

        producer = environment.producer(stream, producer_options)
        10.times do |index|
          filter = index.even? ? "blue" : "red"
          producer.publish("second-#{index}", filter).await(20.seconds).confirmed.should be_true
        end
        producer.close

        values = receive_count(received, 10, 30.seconds)
        values.all? { |value| value.ends_with?('0') || value.ends_with?('2') || value.ends_with?('4') || value.ends_with?('6') || value.ends_with?('8') }.should be_true
      ensure
        consumer.close
        environment.delete_stream(stream)
        environment.close
      end
    end

    it "starts an exact-offset subscription at the requested logical message" do
      environment = integration_environment
      stream = unique_stream("exact-offset")
      environment.create_stream(
        stream,
        Crabbit::StreamOptions.new(initial_cluster_size: 1),
      )
      producer = environment.producer(stream, Crabbit::ProducerOptions.new(batch_size: 20))
      handles = 20.times.map { |index| producer.publish(index.to_s) }.to_a
      handles.each { |handle| handle.await(20.seconds).confirmed.should be_true }
      consumer = environment.consumer(
        stream,
        Crabbit::ConsumerOptions.new(offset: Crabbit::OffsetSpecification.offset(5_u64)),
      )

      begin
        delivery = consumer.receive
        delivery.offset.should eq(5_u64)
        String.new(delivery.body).should eq("5")
        delivery.processed!
      ensure
        consumer.close
        producer.close
        environment.delete_stream(stream)
        environment.close
      end
    end

    it "creates, routes, publishes, and consumes a super stream" do
      environment = integration_environment
      super_stream = unique_stream("super")
      partitions = 3.times.map { |index| "#{super_stream}-#{index}" }.to_a
      environment.create_super_stream(super_stream, partitions, ["0", "1", "2"])
      received = Channel(String).new(60)
      options = Crabbit::ConsumerOptions.new(
        name: "super-consumer",
        offset: Crabbit::OffsetSpecification.first,
        single_active_consumer: true,
        topology_refresh: 1.second,
      )
      consumer = environment.super_stream_consumer(super_stream, options) do |delivery|
        received.send(String.new(delivery.body))
      end
      producer = environment.super_stream_producer(
        super_stream,
        Crabbit::SuperStreamProducerOptions.new(
          ->(message : Crabbit::Message) { message.properties.try(&.subject) || String.new(message.body) },
          topology_refresh: 1.second,
        ),
      )

      begin
        60.times do |index|
          producer.publish(Crabbit::Message.new("super-#{index}"), index.to_s).await(20.seconds)
        end
        receive_count(received, 60, 45.seconds).uniq.size.should eq(60)
        producer.close
        consumer.close
        environment.delete_super_stream(super_stream)
      ensure
        producer.close
        consumer.close
        begin
          environment.delete_super_stream(super_stream)
        rescue Crabbit::BrokerError
        end
        environment.close
      end
    end

    it "recovers publishers and consumers after the broker restarts" do
      environment = integration_environment
      stream = unique_stream("recovery")
      environment.create_stream(stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))
      received = Channel(String).new(10)
      states = Channel(Crabbit::ResourceEvent).new(20)
      policy = Crabbit::RecoveryPolicy.new(
        initial_delay: 100.milliseconds,
        max_delay: 2.seconds,
      )
      consumer = environment.consumer(
        stream,
        Crabbit::ConsumerOptions.new(
          name: "recovery-consumer",
          offset: Crabbit::OffsetSpecification.first,
          recovery_policy: policy,
          on_state_change: ->(event : Crabbit::ResourceEvent) { states.send(event) },
        ),
      ) { |delivery| received.send(String.new(delivery.body)) }
      producer = environment.producer(
        stream,
        Crabbit::ProducerOptions.new(
          name: "recovery-producer",
          confirm_timeout: 90.seconds,
          recovery_policy: policy,
          on_state_change: ->(event : Crabbit::ResourceEvent) { states.send(event) },
        ),
      )

      begin
        producer.publish("before-restart").await(30.seconds).confirmed.should be_true
        receive_count(received, 1, 30.seconds).should eq(["before-restart"])

        status = Process.run("docker", ["compose", "restart", "rabbitmq"])
        status.success?.should be_true

        confirmation = producer.publish("after-restart").await(90.seconds)
        unless confirmation.confirmed
          raise "post-restart publish failed: #{confirmation.error.inspect}"
        end
        receive_count(received, 1, 90.seconds).should eq(["after-restart"])

        observed = [] of Crabbit::ResourceEvent
        deadline = Time.instant + 30.seconds
        until observed.count(&.state.recovering?) >= 2 && observed.count(&.state.open?) >= 4
          remaining = deadline - Time.instant
          raise Crabbit::TimeoutError.new("lifecycle recovery events did not arrive") if remaining <= Time::Span.zero
          select
          when event = states.receive
            observed << event
          when timeout(remaining)
            raise Crabbit::TimeoutError.new("lifecycle recovery events did not arrive")
          end
        end
        observed.any? { |event| event.resource == "producer" && event.state.recovering? }.should be_true
        observed.any? { |event| event.resource == "consumer" && event.state.recovering? }.should be_true
      ensure
        producer.close
        consumer.close
        begin
          environment.delete_stream(stream)
        rescue Crabbit::Error
        end
        environment.close
      end
    end
  end
end
