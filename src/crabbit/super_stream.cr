module Crabbit
  # RabbitMQ-compatible seeded MurmurHash3 x86 32-bit implementation.
  #
  # `SuperStreamProducer` uses this hash for
  # `SuperStreamRouting::Hash`. It is exposed so custom routing code can remain
  # byte-compatible with the official RabbitMQ clients.
  module Murmur3
    extend self

    # Seed used by RabbitMQ Stream client hash routing.
    DEFAULT_SEED = 104_729_u32
    # :nodoc:
    C1 = 0xcc9e2d51_u32
    # :nodoc:
    C2 = 0x1b873593_u32

    # Returns the 32-bit Murmur3 hash of the UTF-8 bytes in *value*.
    def hash32(value : String, seed : UInt32 = DEFAULT_SEED) : UInt32
      data = value.to_slice
      hash = seed
      blocks = data.size // 4
      blocks.times do |block|
        index = block * 4
        key = data[index].to_u32 |
              (data[index + 1].to_u32 << 8) |
              (data[index + 2].to_u32 << 16) |
              (data[index + 3].to_u32 << 24)
        hash = mix(key, hash)
      end

      index = blocks * 4
      tail = data.size - index
      key = 0_u32
      key ^= data[index + 2].to_u32 << 16 if tail >= 3
      key ^= data[index + 1].to_u32 << 8 if tail >= 2
      if tail >= 1
        key ^= data[index].to_u32
        key = key &* C1
        key = rotate_left(key, 15)
        key = key &* C2
        hash ^= key
      end

      hash ^= data.size.to_u32
      finalize(hash)
    end

    private def mix(key : UInt32, hash : UInt32) : UInt32
      key = key &* C1
      key = rotate_left(key, 15)
      key = key &* C2
      hash ^= key
      (rotate_left(hash, 13) &* 5_u32) &+ 0xe6546b64_u32
    end

    private def finalize(hash : UInt32) : UInt32
      hash ^= hash >> 16
      hash = hash &* 0x85ebca6b_u32
      hash ^= hash >> 13
      hash = hash &* 0xc2b2ae35_u32
      hash ^ (hash >> 16)
    end

    private def rotate_left(value : UInt32, bits : Int32) : UInt32
      (value << bits) | (value >> (32 - bits))
    end
  end

  # Aggregate confirmation handle for a super-stream publish.
  #
  # Most routing strategies select one partition, but a custom strategy can
  # publish to several. This handle exposes one `PublishHandle` per selected
  # partition.
  class SuperStreamPublishHandle
    # Returns the partition publish handles.
    getter handles : Array(PublishHandle)

    # :nodoc:
    def initialize(@handles : Array(PublishHandle))
    end

    # Waits for every partition confirmation within one shared *timeout*.
    def await(timeout : Time::Span = 30.seconds) : Array(Confirmation)
      deadline = Time.instant + timeout
      handles.map do |handle|
        remaining = deadline - Time.instant
        raise TimeoutError.new("super stream publish confirmation timed out") if remaining <= Time::Span.zero
        handle.await(remaining)
      end
    end

    # Registers *callback* on every partition publish and returns `self`.
    def on_confirm(&callback : Confirmation ->) : self
      handles.each { |handle| handle.on_confirm { |confirmation| callback.call(confirmation) } }
      self
    end

    # Returns whether every partition publish has completed.
    def completed? : Bool
      handles.all?(&.completed?)
    end
  end

  # Routes publishes across the current partitions of a RabbitMQ super stream.
  #
  # Partition producers are created lazily and share
  # `SuperStreamProducerOptions#producer`. Topology is refreshed on schedule and
  # after routing failures.
  class SuperStreamProducer
    # Returns the logical super-stream name.
    getter super_stream : String
    # Returns routing and partition-producer options.
    getter options : SuperStreamProducerOptions

    @mutex = Mutex.new
    @producers = {} of String => Producer
    @routing_cache = {} of String => Array(String)
    @partitions = [] of String
    @last_refresh = Time.instant - 365.days
    @closed = false
    @entity_id = 0_u64

    # Creates a super-stream producer and resolves its initial topology.
    #
    # Applications normally call `Environment#super_stream_producer`.
    def initialize(
      @environment : Environment,
      @super_stream : String,
      @options : SuperStreamProducerOptions,
    )
      raise ConfigurationError.new("super stream must not be empty") if super_stream.empty?
      refresh_topology!
      @entity_id = environment.register_entity { close }
    end

    # Routes and publishes an AMQP message.
    #
    # *routing_key* overrides `SuperStreamProducerOptions#routing_key_extractor`.
    # A custom routing strategy ignores both values.
    def publish(message : Message, routing_key : String? = nil) : SuperStreamPublishHandle
      streams = if strategy = options.routing_strategy
                  partitions = current_partitions
                  selected = strategy.call(message, partitions)
                  invalid = selected.reject { |stream| partitions.includes?(stream) }
                  unless invalid.empty?
                    raise ConfigurationError.new(
                      "custom routing strategy returned unknown partitions: #{invalid.join(", ")}",
                    )
                  end
                  selected.uniq
                else
                  extractor = options.routing_key_extractor.not_nil!
                  route(routing_key || extractor.call(message))
                end
      raise BrokerError.new(ResponseCode::StreamNotAvailable, "super stream #{super_stream} has no partitions") if streams.empty?
      SuperStreamPublishHandle.new(streams.map { |stream| producer_for(stream).publish(message) })
    rescue ex : BrokerError
      refresh_topology!
      raise ex
    end

    # Routes and publishes an AMQP message, invoking the block for each selected
    # partition's confirmation.
    def publish(
      message : Message,
      routing_key : String? = nil,
      &callback : Confirmation ->
    ) : SuperStreamPublishHandle
      publish(message, routing_key).on_confirm { |confirmation| callback.call(confirmation) }
    end

    # Routes and publishes raw, byte, or string payloads using an explicit key.
    #
    # `RawMessage` is passed through; `Bytes` and `String` are wrapped in an AMQP
    # Data section by the partition producer.
    def publish(message : RawMessage | Bytes | String, routing_key : String) : SuperStreamPublishHandle
      streams = route(routing_key)
      raise BrokerError.new(ResponseCode::StreamNotAvailable, "super stream #{super_stream} has no partitions") if streams.empty?
      SuperStreamPublishHandle.new(streams.map { |stream| producer_for(stream).publish(message) })
    end

    # Idempotently closes all partition producers.
    def close : Nil
      producers = @mutex.synchronize do
        return if @closed
        @closed = true
        values = @producers.values
        @producers.clear
        values
      end
      producers.each(&.close)
      @environment.unregister_entity(@entity_id) unless @entity_id == 0
    end

    private def route(key : String) : Array(String)
      refresh_topology_if_stale
      case options.routing
      when .hash?
        partitions = current_partitions
        return [] of String if partitions.empty?
        hash = options.hash_function.try(&.call(key)) || Murmur3.hash32(key)
        [partitions[(hash % partitions.size.to_u32).to_i]]
      when .routing_key?
        if cached = @mutex.synchronize { @routing_cache[key]? }
          cached
        else
          streams = @environment.route(key, super_stream)
          @mutex.synchronize { @routing_cache[key] = streams }
          streams
        end
      else
        raise ConfigurationError.new("unsupported super stream routing strategy")
      end
    end

    private def current_partitions : Array(String)
      refresh_topology_if_stale
      @mutex.synchronize { @partitions.dup }
    end

    private def producer_for(stream : String) : Producer
      @mutex.synchronize do
        raise ResourceClosedError.new("super stream producer is closed") if @closed
        @producers[stream] ||= @environment.producer(stream, options.producer)
      end
    end

    private def refresh_topology_if_stale : Nil
      stale = @mutex.synchronize { Time.instant - @last_refresh >= options.topology_refresh }
      refresh_topology! if stale
    end

    private def refresh_topology! : Nil
      partitions = @environment.partitions(super_stream)
      removed = [] of Producer
      @mutex.synchronize do
        removed_names = @partitions - partitions
        removed_names.each do |name|
          if producer = @producers.delete(name)
            removed << producer
          end
        end
        @partitions = partitions
        @routing_cache.clear
        @last_refresh = Time.instant
      end
      removed.each(&.close)
    end
  end

  # Maintains callback consumers for every current super-stream partition.
  #
  # Partition additions and removals are reconciled every
  # `ConsumerOptions#topology_refresh`. Delivery order is preserved within each
  # partition, not globally across the super stream.
  class SuperStreamConsumer
    # Returns the logical super-stream name.
    getter super_stream : String

    @mutex = Mutex.new
    @consumers = {} of String => Consumer
    @closed = false
    @entity_id = 0_u64

    # Creates partition consumers and starts topology refresh.
    #
    # Applications normally call `Environment#super_stream_consumer`.
    def initialize(
      @environment : Environment,
      @super_stream : String,
      @options : ConsumerOptions,
      @handler : Proc(Delivery, Nil),
    )
      raise ConfigurationError.new("super stream must not be empty") if super_stream.empty?
      refresh_topology!
      @entity_id = environment.register_entity { close }
      start_topology_refresh
    end

    # Returns a snapshot of the current partition consumers.
    def consumers : Array(Consumer)
      @mutex.synchronize { @consumers.values.dup }
    end

    # Idempotently stops topology refresh and closes all partition consumers.
    def close : Nil
      values = @mutex.synchronize do
        return if @closed
        @closed = true
        result = @consumers.values
        @consumers.clear
        result
      end
      values.each(&.close)
      @environment.unregister_entity(@entity_id) unless @entity_id == 0
    end

    private def start_topology_refresh : Nil
      spawn(name: "crabbit-super-stream-topology-#{super_stream}") do
        loop do
          sleep @options.topology_refresh
          break if @mutex.synchronize { @closed }
          begin
            refresh_topology!
          rescue ex
            Log.warn(exception: ex) { "could not refresh super stream topology" }
          end
        end
      end
    end

    private def refresh_topology! : Nil
      partitions = @environment.partitions(super_stream)
      removed = [] of Consumer
      added = [] of String
      @mutex.synchronize do
        return if @closed
        (@consumers.keys - partitions).each do |stream|
          removed << @consumers.delete(stream).not_nil!
        end
        added = partitions.reject { |stream| @consumers.has_key?(stream) }
      end
      removed.each(&.close)
      added.each do |stream|
        consumer = @environment.consumer(stream, partition_options) do |delivery|
          @handler.call(delivery)
        end
        @mutex.synchronize do
          if @closed
            consumer.close
          else
            @consumers[stream] = consumer
          end
        end
      end
    end

    private def partition_options : ConsumerOptions
      ConsumerOptions.new(
        name: @options.name,
        offset: @options.offset,
        initial_credit: @options.initial_credit,
        filters: @options.filters,
        match_unfiltered: @options.match_unfiltered,
        single_active_consumer: @options.single_active_consumer,
        super_stream: super_stream,
        concurrency: @options.concurrency,
        buffer_size: @options.buffer_size,
        validate_crc: @options.validate_crc,
        auto_store_every: @options.auto_store_every,
        auto_store_interval: @options.auto_store_interval,
        recovery_policy: @options.recovery_policy,
        on_consumer_update: @options.on_consumer_update,
        on_consumer_update_context: @options.on_consumer_update_context,
        subscription_offset: @options.subscription_offset,
        on_state_change: @options.on_state_change,
        topology_refresh: @options.topology_refresh,
      )
    end
  end
end
