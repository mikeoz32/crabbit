module Crabbit
  # Owns Stream connections and creates producers, consumers, and management
  # requests.
  #
  # An environment lazily creates and reuses connections according to the
  # current stream metadata. Closing it also closes all producers, consumers,
  # super-stream resources, and pooled connections created through it.
  #
  # ```
  # environment = Crabbit::Environment.connect(
  #   "rabbitmq-stream://guest:guest@localhost:5552/%2f"
  # )
  # begin
  #   environment.create_stream("events") unless environment.stream_exists?("events")
  # ensure
  #   environment.close
  # end
  # ```
  class Environment
    private enum ConnectionRole
      Locator
      Producer
      Consumer
    end

    # Returns the immutable connection configuration.
    getter configuration : Configuration
    # Returns the compression-codec registry used by producers and consumers.
    getter compression_codecs : CompressionCodecs

    @pool_mutex = Mutex.new
    @clients = {} of Endpoint => Array(Internal::Client)
    @entities_mutex = Mutex.new
    @entities = {} of UInt64 => Proc(Nil)
    @next_entity_id = 0_u64
    @seed_cursor = 0
    @closed = false

    # Creates an environment without opening a connection.
    #
    # Connections are established lazily by management and messaging methods.
    # Pass a custom registry to replace or extend sub-entry compression codecs.
    def initialize(
      @configuration : Configuration = Configuration.new,
      @compression_codecs : CompressionCodecs = CompressionCodecs.new,
    )
    end

    # Parses *uri* and returns an environment.
    #
    # Named *options* are forwarded to `Configuration.parse`.
    def self.connect(uri : String = Configuration::DEFAULT_URI, **options) : self
      new(Configuration.parse(uri, **options))
    end

    # Returns whether this environment was closed.
    def closed? : Bool
      @pool_mutex.synchronize { @closed }
    end

    # Creates *stream* with the supplied retention and placement options.
    #
    # Raises `BrokerError` when RabbitMQ rejects the operation, including when
    # the stream already exists.
    def create_stream(stream : String, options : StreamOptions = StreamOptions.new) : Nil
      locator.create_stream(stream, options)
    end

    # Deletes *stream*.
    def delete_stream(stream : String) : Nil
      locator.delete_stream(stream)
    end

    # Returns broker statistics for *stream*.
    def stream_stats(stream : String) : StreamStats
      locator.stream_stats(stream)
    end

    # Returns the stored consumer offset, or `nil` when none exists.
    def query_offset(reference : String, stream : String) : UInt64?
      producer_client(stream).query_offset(reference, stream)
    end

    # Stores *offset* for the consumer *reference* and *stream*.
    def store_offset(reference : String, stream : String, offset : UInt64) : Nil
      producer_client(stream).store_offset(reference, stream, offset)
    end

    # Returns the last publishing ID recorded for a named publisher.
    #
    # RabbitMQ returns zero when no publisher sequence exists.
    def query_publisher_sequence(reference : String, stream : String) : UInt64
      producer_client(stream).query_publisher_sequence(reference, stream)
    end

    # Returns metadata for every requested stream in input order.
    #
    # Per-stream failures are represented by `StreamMetadata#response_code`.
    def metadata(streams : Enumerable(String)) : Array(StreamMetadata)
      locator.metadata(streams)
    end

    # Returns whether *stream* currently exists.
    #
    # Errors other than `ResponseCode::StreamDoesNotExist` are raised.
    def stream_exists?(stream : String) : Bool
      value = metadata([stream]).first? ||
              raise ProtocolError.new("metadata response omitted stream #{stream}")
      return true if value.response_code.ok?
      return false if value.response_code.stream_does_not_exist?
      value.response_code.raise_unless_ok!("metadata for #{stream}")
      false
    end

    # Resolves a relative or timestamp offset to an absolute stream offset.
    #
    # *properties* are forwarded to RabbitMQ's Resolve Offset Specification
    # command and are useful for broker extensions.
    def resolve_offset(
      stream : String,
      offset : OffsetSpecification,
      properties : Hash(String, String) = {} of String => String,
    ) : UInt64
      locator.resolve_offset(stream, offset, properties)
    end

    # Returns the ordered partition stream names of *super_stream*.
    def partitions(super_stream : String) : Array(String)
      locator.partitions(super_stream)
    end

    # Returns the partitions selected by *routing_key* and broker bindings.
    def route(routing_key : String, super_stream : String) : Array(String)
      locator.route(routing_key, super_stream)
    end

    # Creates a super stream from matching partition and binding-key lists.
    #
    # Each partition must have a corresponding binding key. *arguments* are
    # passed to RabbitMQ unchanged.
    def create_super_stream(
      name : String,
      partitions : Enumerable(String),
      binding_keys : Enumerable(String),
      arguments : Hash(String, String) = {} of String => String,
    ) : Nil
      locator.create_super_stream(name, partitions, binding_keys, arguments)
    end

    # Deletes the super stream *name* and its partitions.
    def delete_super_stream(name : String) : Nil
      locator.delete_super_stream(name)
    end

    # Creates a producer for *stream*.
    def producer(stream : String, options : ProducerOptions = ProducerOptions.new) : Producer
      ensure_open!
      Producer.new(self, stream, options)
    end

    # Creates a pull consumer for *stream*.
    #
    # Retrieve deliveries with `Consumer#receive`, `Consumer#receive?`, or
    # `Consumer#each`.
    def consumer(stream : String, options : ConsumerOptions = ConsumerOptions.new) : Consumer
      ensure_open!
      Consumer.new(self, stream, options)
    end

    # Creates a callback consumer for *stream*.
    #
    # The block runs on handler fibers and each delivery is marked processed
    # after the block returns, even when it raises.
    def consumer(
      stream : String,
      options : ConsumerOptions = ConsumerOptions.new,
      &handler : Delivery ->
    ) : Consumer
      ensure_open!
      Consumer.new(self, stream, options, handler)
    end

    # Creates a producer that routes messages across a super stream.
    def super_stream_producer(
      super_stream : String,
      options : SuperStreamProducerOptions,
    ) : SuperStreamProducer
      ensure_open!
      SuperStreamProducer.new(self, super_stream, options)
    end

    # Creates callback consumers for all current and future partitions of a
    # super stream.
    def super_stream_consumer(
      super_stream : String,
      options : ConsumerOptions,
      &handler : Delivery ->
    ) : SuperStreamConsumer
      ensure_open!
      SuperStreamConsumer.new(self, super_stream, options, handler)
    end

    # Idempotently closes all resources and network connections.
    def close : Nil
      entity_closers = @entities_mutex.synchronize do
        values = @entities.values
        @entities.clear
        values
      end
      clients = @pool_mutex.synchronize do
        return if @closed
        @closed = true
        values = @clients.values.flatten
        @clients.clear
        values
      end
      entity_closers.each do |closer|
        begin
          closer.call
        rescue ex
          Log.debug(exception: ex) { "error while closing entity" }
        end
      end
      clients.each do |client|
        begin
          client.connection.close
        rescue ex
          Log.debug(exception: ex) { "error while closing connection" }
        end
      end
    end

    # :nodoc:
    def register_entity(&closer : ->) : UInt64
      @entities_mutex.synchronize do
        @next_entity_id &+= 1_u64
        @entities[@next_entity_id] = closer
        @next_entity_id
      end
    end

    # :nodoc:
    def unregister_entity(id : UInt64) : Nil
      @entities_mutex.synchronize { @entities.delete(id) }
    end

    # :nodoc:
    def producer_client(stream : String) : Internal::Client
      metadata = metadata_for(stream)
      leader = metadata.leader || raise BrokerError.new(
        ResponseCode::StreamNotAvailable,
        "stream #{stream} has no leader",
      )
      client_for_broker(leader, ConnectionRole::Producer)
    end

    # :nodoc:
    def consumer_client(stream : String) : Internal::Client
      metadata = metadata_for(stream)
      candidates = metadata.replicas.shuffle
      if leader = metadata.leader
        candidates << leader unless candidates.includes?(leader)
      end
      if candidates.empty?
        raise BrokerError.new(
          ResponseCode::StreamNotAvailable,
          "stream #{stream} has no readable broker",
        )
      end

      last_error : ConnectionError? = nil
      candidates.each do |broker|
        begin
          return client_for_broker(broker, ConnectionRole::Consumer)
        rescue ex : ConnectionError
          last_error = ex
        end
      end
      raise last_error.not_nil!
    end

    private def metadata_for(stream : String) : StreamMetadata
      metadata = locator.metadata([stream]).first? ||
                 raise ProtocolError.new("metadata response omitted stream #{stream}")
      metadata.response_code.raise_unless_ok!("metadata for #{stream}")
      metadata
    end

    private def locator : Internal::Client
      ensure_open!
      endpoints = rotated_endpoints
      last_error : Exception? = nil
      endpoints.each do |endpoint|
        begin
          return client_for_endpoint(endpoint, ConnectionRole::Locator)
        rescue ex : ConnectionError
          last_error = ex
        end
      end
      raise last_error || ConnectionError.new("no RabbitMQ Stream endpoint is available")
    end

    private def rotated_endpoints : Array(Endpoint)
      @pool_mutex.synchronize do
        endpoints = configuration.endpoints
        start = @seed_cursor % endpoints.size
        @seed_cursor &+= 1
        Array(Endpoint).new(endpoints.size) { |index| endpoints[(start + index) % endpoints.size] }
      end
    end

    private def client_for_broker(broker : Broker, role : ConnectionRole) : Internal::Client
      target = Endpoint.new(
        broker.host,
        broker.port,
        configuration.tls != nil || configuration.endpoints.any?(&.tls),
      )
      return client_for_endpoint(target, role) unless configuration.load_balancer

      client_for_load_balanced_target(target, role)
    end

    private def client_for_endpoint(endpoint : Endpoint, role : ConnectionRole) : Internal::Client
      ensure_open!
      @pool_mutex.synchronize do
        raise ResourceClosedError.new("environment is closed") if @closed
        clients = (@clients[endpoint] ||= [] of Internal::Client)
        clients.reject! { |client| !client.open? }
        if current = clients.find { |client| suitable?(client, role) }
          return current
        end
        connection = Internal::Connection.new(configuration, endpoint).connect!
        client = Internal::Client.new(connection)
        clients << client
        client
      end
    end

    private def client_for_load_balanced_target(
      target : Endpoint,
      role : ConnectionRole,
    ) : Internal::Client
      @pool_mutex.synchronize do
        raise ResourceClosedError.new("environment is closed") if @closed
        clients = (@clients[target] ||= [] of Internal::Client)
        clients.reject! { |client| !client.open? }
        if current = clients.find { |client| suitable?(client, role) }
          return current
        end
      end

      # A load balancer can send each connection to a different node. Connect
      # through the configured entrypoints until the node advertises the
      # metadata-selected host and port.
      last_error : ConnectionError? = nil
      20.times do |attempt|
        rotated_endpoints.each do |entrypoint|
          begin
            connection = Internal::Connection.new(configuration, entrypoint).connect!
          rescue ex : ConnectionError
            last_error = ex
            next
          end
          properties = connection.connection_properties
          host = properties["advertised_host"]?
          port = properties["advertised_port"]?.try(&.to_i?)
          if host == target.host && port == target.port
            client = Internal::Client.new(connection)
            @pool_mutex.synchronize do
              (@clients[target] ||= [] of Internal::Client) << client
            end
            return client
          end
          connection.close
        end
        sleep Random.rand(500..1_500).milliseconds unless attempt == 19
      end
      detail = last_error.try { |error| "; last connection error: #{error.message}" } || ""
      raise ConnectionError.new(
        "load balancer did not route a connection to #{target.host}:#{target.port}#{detail}",
      )
    end

    private def suitable?(client : Internal::Client, role : ConnectionRole) : Bool
      case role
      when .locator?  then true
      when .producer? then client.publisher_capacity?
      when .consumer? then client.subscription_capacity?
      else                 false
      end
    end

    private def ensure_open! : Nil
      raise ResourceClosedError.new("environment is closed") if closed?
    end
  end
end
