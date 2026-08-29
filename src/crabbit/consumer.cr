module Crabbit
  class ConsumerUpdateContext
    getter active : Bool
    getter stream : String
    getter consumer : Consumer

    def initialize(@active : Bool, @stream : String, @consumer : Consumer)
    end
  end

  private record DeliveryFrame,
    frame : Internal::Wire::Frame,
    generation : UInt64,
    client : Internal::Client,
    subscription_id : UInt8

  private class ChunkAcknowledgement
    @mutex = Mutex.new
    @remaining : Int32

    def initialize(
      @consumer : Consumer,
      count : Int32,
      @generation : UInt64,
      @client : Internal::Client,
      @subscription_id : UInt8,
    )
      @remaining = count
    end

    def processed(offset : UInt64) : Nil
      complete = @mutex.synchronize do
        @consumer.record_processed_offset(offset)
        @remaining -= 1
        @remaining == 0
      end
      @consumer.chunk_processed(@generation, @client, @subscription_id) if complete
    end
  end

  class Delivery
    getter stream : String
    getter offset : UInt64
    getter timestamp : Time
    getter committed_chunk_id : UInt64
    getter raw : Bytes

    @processed_mutex = Mutex.new
    @processed = false
    @message_mutex = Mutex.new
    @decoded_message : Message?

    protected def initialize(
      @stream : String,
      raw_delivery : Internal::RawDelivery,
      @acknowledgement : ChunkAcknowledgement,
    )
      @offset = raw_delivery.offset
      @timestamp = raw_delivery.timestamp
      @committed_chunk_id = raw_delivery.committed_chunk_id
      @raw = raw_delivery.bytes
    end

    def message : Message
      @message_mutex.synchronize do
        @decoded_message ||= Message.from_amqp(raw, zero_copy: true)
      end
    end

    def body : Bytes
      message.body
    end

    def processed? : Bool
      @processed_mutex.synchronize { @processed }
    end

    def processed! : Nil
      first = @processed_mutex.synchronize do
        unless @processed
          @processed = true
          true
        else
          false
        end
      end
      @acknowledgement.processed(offset) if first
    end
  end

  class Consumer
    include Enumerable(Delivery)

    getter stream : String
    getter options : ConsumerOptions

    @state_mutex = Mutex.new
    @state = ResourceState::Recovering
    @recovering = false
    @generation = 0_u64
    @client : Internal::Client? = nil
    @subscription_id : UInt8? = nil
    @disconnect_registration : Tuple(Internal::Client, Internal::Connection::DisconnectHandler)? = nil
    @metadata_registration : Tuple(Internal::Client, Internal::Connection::FrameHandler)? = nil
    @frames : Channel(DeliveryFrame)
    @deliveries : Channel(Delivery)
    @handler : Proc(Delivery, Nil)?
    @offset_mutex = Mutex.new
    @offset_tracker = Internal::DeliveryOffsetTracker.new
    @processed_since_store = 0
    @last_stored_offset : UInt64? = nil
    @minimum_delivery_offset : UInt64? = nil
    @entity_id = 0_u64

    def initialize(
      @environment : Environment,
      @stream : String,
      @options : ConsumerOptions,
      @handler : Proc(Delivery, Nil)? = nil,
    )
      raise ConfigurationError.new("stream must not be empty") if stream.empty?
      frame_capacity = Math.max(options.initial_credit.to_i, 2)
      @frames = Channel(DeliveryFrame).new(frame_capacity)
      @deliveries = Channel(Delivery).new(options.buffer_size)
      attach(@environment.consumer_client(stream), options.offset)
      @entity_id = environment.register_entity { close }
      start_parser
      start_handlers if @handler
      start_auto_store_worker if options.auto_store_interval
      transition(ResourceState::Open)
    end

    def state : ResourceState
      @state_mutex.synchronize { @state }
    end

    def open? : Bool
      state.open?
    end

    def closed? : Bool
      state.closed?
    end

    # Receive one delivery. The caller owns acknowledgement and must invoke
    # `processed!` when processing is complete.
    def receive : Delivery
      @deliveries.receive
    rescue Channel::ClosedError
      raise ResourceClosedError.new("consumer is closed")
    end

    def receive? : Delivery?
      @deliveries.receive?
    end

    # Enumerable consumption acknowledges after the block returns, including
    # when it raises. Use `receive` for explicit acknowledgement control.
    def each(&block : Delivery ->) : Nil
      while delivery = @deliveries.receive?
        begin
          yield delivery
        ensure
          delivery.processed!
        end
      end
    end

    def store_offset(offset : UInt64) : Nil
      name = options.name || raise ConfigurationError.new("storing offsets requires a named consumer")
      client = @state_mutex.synchronize { @client }
      effective_client = client && client.open? ? client : @environment.producer_client(stream)
      effective_client.store_offset(name, stream, offset)
      @offset_mutex.synchronize { @last_stored_offset = offset }
    end

    def store_offset(delivery : Delivery) : Nil
      unless delivery.stream == stream
        raise ArgumentError.new("delivery belongs to #{delivery.stream}, not #{stream}")
      end
      store_offset(delivery.offset)
    end

    def stored_offset : UInt64?
      name = options.name || raise ConfigurationError.new("querying offsets requires a named consumer")
      client = @state_mutex.synchronize { @client }
      return client.query_offset(name, stream) if client && client.open?
      @environment.query_offset(name, stream)
    end

    def close : Nil
      return unless mark_closed
      begin
        auto_store_latest
        client, subscription_id = @state_mutex.synchronize { {@client, @subscription_id} }
        if client
          if id = subscription_id
            client.unsubscribe(id) if client.open?
          end
        end
      rescue ex
        Log.debug(exception: ex) { "could not cleanly close consumer" }
      ensure
        remove_disconnect_registration
        remove_metadata_registration
        close_internal
        @environment.unregister_entity(@entity_id) unless @entity_id == 0
        notify(ResourceState::Closed)
      end
    end

    # Called by Delivery/ChunkAcknowledgement. Public only because helper
    # objects cannot access private methods in Crystal.
    def record_processed_offset(offset : UInt64) : Nil
      value_to_store : UInt64? = nil
      @offset_mutex.synchronize do
        @processed_since_store += @offset_tracker.processed(offset)
        if every = options.auto_store_every
          if @processed_since_store >= every
            value_to_store = @offset_tracker.last
            @processed_since_store = 0 if value_to_store
          end
        end
      end
      safely_store(value_to_store) if value_to_store
    end

    def chunk_processed(
      generation : UInt64,
      client : Internal::Client,
      subscription_id : UInt8,
    ) : Nil
      current = @state_mutex.synchronize do
        @state.open? && @generation == generation && @client.same?(client) &&
          @subscription_id == subscription_id
      end
      client.credit(subscription_id, 1_u16) if current
    rescue ex : ConnectionError
      begin_recovery(ex)
    end

    private def attach(client : Internal::Client, offset : OffsetSpecification) : Bool
      effective_offset = options.subscription_offset.try(&.call(stream)) || offset
      generation = @state_mutex.synchronize do
        @generation &+= 1_u64
        @minimum_delivery_offset = exact_offset(effective_offset)
        @generation
      end
      properties = subscription_properties
      subscription_id = client.subscribe(
        stream,
        effective_offset,
        options.initial_credit,
        properties,
        ->(frame : Internal::Wire::Frame) do
          id = frame.body[0]? || raise ProtocolError.new("Deliver frame omitted subscription ID")
          enqueue_frame(DeliveryFrame.new(frame, generation, client, id))
        end,
        ->(active : Bool) { consumer_update(active) },
      )
      accepted = @state_mutex.synchronize do
        unless @state.closed?
          @client = client
          @subscription_id = subscription_id
          true
        else
          false
        end
      end
      unless accepted
        client.unsubscribe(subscription_id) if client.open?
        return false
      end
      handler = client.on_disconnect { |cause| disconnected(client, cause) }
      metadata_handler = client.on_metadata_update do |update|
        if update.stream == stream
          begin_recovery(BrokerError.new(ResponseCode::StreamNotAvailable, "metadata changed for #{stream}"))
        end
      end
      previous, previous_metadata = @state_mutex.synchronize do
        old = @disconnect_registration
        old_metadata = @metadata_registration
        @disconnect_registration = {client, handler}
        @metadata_registration = {client, metadata_handler}
        {old, old_metadata}
      end
      previous.try { |old_client, old_handler| old_client.remove_disconnect_handler(old_handler) }
      previous_metadata.try do |old_client, old_handler|
        old_client.remove_metadata_update_handler(old_handler)
      end
      true
    end

    private def subscription_properties : Hash(String, String)
      properties = {} of String => String
      if name = options.name
        properties["name"] = name
      end
      options.filters.each_with_index { |value, index| properties["filter.#{index}"] = value }
      properties["match-unfiltered"] = options.match_unfiltered.to_s if options.filters.any?
      properties["single-active-consumer"] = "true" if options.single_active_consumer
      if super_stream = options.super_stream
        properties["super-stream"] = super_stream
      end
      properties
    end

    private def enqueue_frame(frame : DeliveryFrame) : Nil
      select
      when @frames.send(frame)
      else
        frame.client.connection.abort(
          ProtocolError.new("consumer Deliver queue exceeded negotiated credit"),
        )
      end
    rescue Channel::ClosedError
    end

    private def start_parser : Nil
      parser = Internal::DeliverParser.new(
        @environment.compression_codecs,
        validate_crc: options.validate_crc,
      )
      spawn(name: "crabbit-consumer-parser-#{stream}") do
        while envelope = @frames.receive?
          begin
            next unless current_generation?(envelope)
            chunk = parser.parse(envelope.frame)
            minimum_offset = @state_mutex.synchronize { @minimum_delivery_offset }
            deliveries = if minimum_offset
                           chunk.deliveries.reject { |delivery| delivery.offset < minimum_offset }
                         else
                           chunk.deliveries
                         end
            acknowledgement = ChunkAcknowledgement.new(
              self,
              deliveries.size,
              envelope.generation,
              envelope.client,
              envelope.subscription_id,
            )
            register_offsets(deliveries)
            if deliveries.empty?
              chunk_processed(
                envelope.generation,
                envelope.client,
                envelope.subscription_id,
              )
              next
            end
            deliveries.each do |raw|
              @deliveries.send(Delivery.new(stream, raw, acknowledgement))
            end
          rescue ex
            unless closed?
              Log.warn(exception: ex) { "consumer could not decode Deliver frame" }
              envelope.client.connection.abort(ex)
              begin_recovery(ex)
            end
          end
        end
      end
    end

    private def start_handlers : Nil
      options.concurrency.times do |worker|
        spawn(name: "crabbit-consumer-handler-#{stream}-#{worker}") do
          while delivery = @deliveries.receive?
            begin
              @handler.not_nil!.call(delivery)
            rescue ex
              Log.error(exception: ex) { "consumer handler failed at offset #{delivery.offset}" }
            ensure
              delivery.processed!
            end
          end
        end
      end
    end

    private def start_auto_store_worker : Nil
      interval = options.auto_store_interval.not_nil!
      spawn(name: "crabbit-consumer-offsets-#{stream}") do
        until closed?
          sleep interval
          auto_store_latest
        end
      end
    end

    private def register_offsets(deliveries : Array(Internal::RawDelivery)) : Nil
      @offset_mutex.synchronize do
        deliveries.each { |delivery| @offset_tracker.register(delivery.offset) }
      end
    end

    private def auto_store_latest : Nil
      return unless options.name && (options.auto_store_every || options.auto_store_interval)
      offset = @offset_mutex.synchronize { @offset_tracker.last }
      safely_store(offset) if offset
    end

    private def safely_store(offset : UInt64?) : Nil
      return unless offset
      already_stored = @offset_mutex.synchronize { @last_stored_offset == offset }
      return if already_stored
      store_offset(offset)
    rescue ex
      Log.warn(exception: ex) { "could not store consumer offset #{offset}" } unless closed?
    end

    private def consumer_update(active : Bool) : OffsetSpecification
      offset = consumer_update_offset(active)
      @state_mutex.synchronize do
        @minimum_delivery_offset = exact_offset(offset) if active
      end
      offset
    rescue ex
      Log.warn(exception: ex) { "consumer update offset lookup failed" }
      offset = resume_offset
      @state_mutex.synchronize do
        @minimum_delivery_offset = exact_offset(offset) if active
      end
      offset
    end

    private def consumer_update_offset(active : Bool) : OffsetSpecification
      if listener = options.on_consumer_update_context
        return listener.call(ConsumerUpdateContext.new(active, stream, self))
      end
      if listener = options.on_consumer_update
        return listener.call(active)
      end
      return OffsetSpecification.none unless active
      if name = options.name
        if stored = @environment.query_offset(name, stream)
          return OffsetSpecification.offset(stored &+ 1_u64)
        end
      end
      resume_offset
    end

    private def resume_offset : OffsetSpecification
      last = @offset_mutex.synchronize { @offset_tracker.last }
      last ? OffsetSpecification.offset(last &+ 1_u64) : options.offset
    end

    private def exact_offset(offset : OffsetSpecification) : UInt64?
      return unless offset.type.offset?
      offset.value.as(UInt64)
    end

    private def current_generation?(envelope : DeliveryFrame) : Bool
      @state_mutex.synchronize do
        @generation == envelope.generation && @client.same?(envelope.client) &&
          @subscription_id == envelope.subscription_id && !@state.closed?
      end
    end

    private def disconnected(source : Internal::Client, cause : Exception) : Nil
      return unless @state_mutex.synchronize { @client.same?(source) }
      begin_recovery(cause)
    end

    private def begin_recovery(cause : Exception) : Nil
      start = @state_mutex.synchronize do
        return if @state.closed? || @recovering
        @state = ResourceState::Recovering
        @recovering = true
        true
      end
      return unless start
      notify(ResourceState::Recovering, cause)
      spawn(name: "crabbit-consumer-recovery-#{stream}") { recover }
    end

    private def recover : Nil
      attempt = 0
      loop do
        return if closed?
        begin
          forget_current_subscription
          client = @environment.consumer_client(stream)
          return unless attach(client, resume_offset)
          raise ConnectionClosedError.new("consumer connection closed during recovery") unless client.open?
          @state_mutex.synchronize do
            @state = ResourceState::Open
            @recovering = false
          end
          notify(ResourceState::Open)
          return
        rescue ex
          Log.warn(exception: ex) { "consumer recovery attempt #{attempt + 1} failed" }
          sleep options.recovery_policy.delay(attempt)
          attempt += 1
        end
      end
    end

    private def transition(state : ResourceState, cause : Exception? = nil) : Nil
      @state_mutex.synchronize { @state = state }
      notify(state, cause)
    end

    private def notify(state : ResourceState, cause : Exception? = nil) : Nil
      if listener = options.on_state_change
        event = ResourceEvent.new("consumer", stream, state, cause)
        spawn(name: "crabbit-consumer-state") { listener.call(event) }
      end
    end

    private def mark_closed : Bool
      @state_mutex.synchronize do
        return false if @state.closed?
        @state = ResourceState::Closed
        @recovering = false
        true
      end
    end

    private def close_internal : Nil
      begin
        @frames.close
      rescue
      end
      begin
        @deliveries.close
      rescue
      end
    end

    private def remove_disconnect_registration : Nil
      registration = @state_mutex.synchronize do
        value = @disconnect_registration
        @disconnect_registration = nil
        value
      end
      registration.try { |client, handler| client.remove_disconnect_handler(handler) }
    end

    private def remove_metadata_registration : Nil
      registration = @state_mutex.synchronize do
        value = @metadata_registration
        @metadata_registration = nil
        value
      end
      registration.try { |client, handler| client.remove_metadata_update_handler(handler) }
    end

    private def forget_current_subscription : Nil
      client, subscription_id = @state_mutex.synchronize { {@client, @subscription_id} }
      client.forget_subscription(subscription_id) if client && subscription_id
    end
  end
end
