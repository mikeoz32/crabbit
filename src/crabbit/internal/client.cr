# :nodoc:
module Crabbit::Internal
  class Client
    alias ConfirmHandler = Proc(Array(UInt64), Nil)
    alias PublishErrorHandler = Proc(Hash(UInt64, ResponseCode), Nil)
    alias DeliverHandler = Proc(Wire::Frame, Nil)
    alias ConsumerUpdateHandler = Proc(Bool, OffsetSpecification)

    getter connection : Connection

    def open? : Bool
      connection.open?
    end

    def publisher_capacity? : Bool
      @entity_mutex.synchronize { @publisher_ids.size < 256 }
    end

    def subscription_capacity? : Bool
      @entity_mutex.synchronize { @subscription_ids.size < 256 }
    end

    def on_disconnect(&handler : Exception ->) : Connection::DisconnectHandler
      connection.on_disconnect { |cause| handler.call(cause) }
    end

    def remove_disconnect_handler(handler : Connection::DisconnectHandler) : Nil
      connection.remove_disconnect_handler(handler)
    end

    def on_metadata_update(&handler : Wire::MetadataUpdate ->) : Connection::FrameHandler
      connection.on(Wire::Command::MetadataUpdate) do |frame|
        handler.call(Wire::Commands.decode_metadata_update(frame))
      end
    end

    def remove_metadata_update_handler(handler : Connection::FrameHandler) : Nil
      connection.remove_handler(Wire::Command::MetadataUpdate, handler)
    end

    @entity_mutex = Mutex.new
    @publisher_ids = Set(UInt8).new
    @subscription_ids = Set(UInt8).new
    @confirm_handlers = {} of UInt8 => ConfirmHandler
    @publish_error_handlers = {} of UInt8 => PublishErrorHandler
    @deliver_handlers = {} of UInt8 => DeliverHandler
    @consumer_update_handlers = {} of UInt8 => ConsumerUpdateHandler

    def initialize(@connection : Connection)
      register_frame_handlers
    end

    def create_stream(stream : String, options : StreamOptions = StreamOptions.new) : Nil
      response = request_response do |correlation|
        Wire::Commands.create_stream(correlation, stream, options.arguments)
      end
      response.code.raise_unless_ok!("create stream #{stream}")
      response.reader.finish!
    end

    def delete_stream(stream : String) : Nil
      response = request_response { |correlation| Wire::Commands.delete_stream(correlation, stream) }
      response.code.raise_unless_ok!("delete stream #{stream}")
      response.reader.finish!
    end

    def metadata(streams : Enumerable(String)) : Array(StreamMetadata)
      frame = connection.request { |correlation| Wire::Commands.metadata(correlation, streams) }
      Wire::Commands.decode_metadata(frame)
    end

    def stream_stats(stream : String) : StreamStats
      response = request_response { |correlation| Wire::Commands.stream_stats(correlation, stream) }
      response.code.raise_unless_ok!("stream stats #{stream}")
      result = Wire::Commands.decode_stats(response)
      response.reader.finish!
      result
    end

    def query_publisher_sequence(reference : String, stream : String) : UInt64
      response = request_response do |correlation|
        Wire::Commands.query_publisher_sequence(correlation, reference, stream)
      end
      response.code.raise_unless_ok!("query publisher sequence #{reference}")
      value = response.reader.read_u64
      response.reader.finish!
      value
    end

    def query_offset(reference : String, stream : String) : UInt64?
      response = request_response { |correlation| Wire::Commands.query_offset(correlation, reference, stream) }
      if response.code.no_offset?
        response.reader.read_u64
        response.reader.finish!
        return nil
      end
      response.code.raise_unless_ok!("query offset #{reference}")
      value = response.reader.read_u64
      response.reader.finish!
      value
    end

    def store_offset(reference : String, stream : String, offset : UInt64) : Nil
      connection.send(Wire::Commands.store_offset(reference, stream, offset))
    end

    def resolve_offset(
      stream : String,
      offset : OffsetSpecification,
      properties : Hash(String, String) = {} of String => String,
    ) : UInt64
      response = request_response do |correlation|
        Wire::Commands.resolve_offset(correlation, stream, offset, properties)
      end
      response.code.raise_unless_ok!("resolve offset for #{stream}")
      type = OffsetType.from_value(response.reader.read_u16)
      raise ProtocolError.new("resolved offset response returned #{type}") unless type.offset?
      value = response.reader.read_u64
      response.reader.finish!
      value
    rescue ex : ArgumentError
      raise ProtocolError.new("unknown resolved offset type")
    end

    def route(routing_key : String, super_stream : String) : Array(String)
      response = request_response do |correlation|
        Wire::Commands.route(correlation, routing_key, super_stream)
      end
      response.code.raise_unless_ok!("route #{routing_key} on #{super_stream}")
      result = Wire::Commands.decode_strings(response)
      response.reader.finish!
      result
    end

    def partitions(super_stream : String) : Array(String)
      response = request_response { |correlation| Wire::Commands.partitions(correlation, super_stream) }
      response.code.raise_unless_ok!("partitions for #{super_stream}")
      result = Wire::Commands.decode_strings(response)
      response.reader.finish!
      result
    end

    def create_super_stream(
      name : String,
      partitions : Enumerable(String),
      binding_keys : Enumerable(String),
      arguments : Hash(String, String) = {} of String => String,
    ) : Nil
      response = request_response do |correlation|
        Wire::Commands.create_super_stream(correlation, name, partitions, binding_keys, arguments)
      end
      response.code.raise_unless_ok!("create super stream #{name}")
      response.reader.finish!
    end

    def delete_super_stream(name : String) : Nil
      response = request_response { |correlation| Wire::Commands.delete_super_stream(correlation, name) }
      response.code.raise_unless_ok!("delete super stream #{name}")
      response.reader.finish!
    end

    def declare_publisher(
      stream : String,
      reference : String?,
      confirm : ConfirmHandler,
      error : PublishErrorHandler,
    ) : UInt8
      id : UInt8? = allocate_publisher_id
      @entity_mutex.synchronize do
        @confirm_handlers[id.not_nil!] = confirm
        @publish_error_handlers[id.not_nil!] = error
      end
      response = request_response do |correlation|
        Wire::Commands.declare_publisher(correlation, id.not_nil!, reference, stream)
      end
      response.code.raise_unless_ok!("declare publisher on #{stream}")
      response.reader.finish!
      id.not_nil!
    rescue ex
      release_publisher_id(id.not_nil!) if id
      raise ex
    end

    def delete_publisher(id : UInt8) : Nil
      response = request_response { |correlation| Wire::Commands.delete_publisher(correlation, id) }
      response.code.raise_unless_ok!("delete publisher #{id}")
      response.reader.finish!
    ensure
      release_publisher_id(id)
    end

    def forget_publisher(id : UInt8) : Nil
      release_publisher_id(id)
    end

    def subscribe(
      stream : String,
      offset : OffsetSpecification,
      credit : UInt16,
      properties : Hash(String, String),
      deliver : DeliverHandler,
      update : ConsumerUpdateHandler,
    ) : UInt8
      id : UInt8? = allocate_subscription_id
      @entity_mutex.synchronize do
        @deliver_handlers[id.not_nil!] = deliver
        @consumer_update_handlers[id.not_nil!] = update
      end
      response = request_response do |correlation|
        Wire::Commands.subscribe(correlation, id.not_nil!, stream, offset, credit, properties)
      end
      response.code.raise_unless_ok!("subscribe to #{stream}")
      response.reader.finish!
      id.not_nil!
    rescue ex
      release_subscription_id(id.not_nil!) if id
      raise ex
    end

    def credit(subscription_id : UInt8, value : UInt16) : Nil
      connection.send(Wire::Commands.credit(subscription_id, value))
    end

    def unsubscribe(id : UInt8) : Nil
      response = request_response { |correlation| Wire::Commands.unsubscribe(correlation, id) }
      response.code.raise_unless_ok!("unsubscribe #{id}")
      response.reader.finish!
    ensure
      release_subscription_id(id)
    end

    def forget_subscription(id : UInt8) : Nil
      release_subscription_id(id)
    end

    private def request_response(&build : UInt32 -> Bytes) : Wire::Response
      Wire::Commands.response(connection.request { |correlation| build.call(correlation) })
    end

    private def register_frame_handlers : Nil
      connection.on(Wire::Command::PublishConfirm) do |frame|
        confirmation = Wire::Commands.decode_publish_confirmation(frame)
        handler = @entity_mutex.synchronize { @confirm_handlers[confirmation.publisher_id]? }
        handler.try(&.call(confirmation.publishing_ids))
      end
      connection.on(Wire::Command::PublishError) do |frame|
        failure = Wire::Commands.decode_publish_failure(frame)
        handler = @entity_mutex.synchronize { @publish_error_handlers[failure.publisher_id]? }
        handler.try(&.call(failure.errors))
      end
      connection.on(Wire::Command::Deliver) do |frame|
        subscription_id = frame.body[0]?
        handler = subscription_id && @entity_mutex.synchronize { @deliver_handlers[subscription_id]? }
        handler.try(&.call(frame))
      end
      connection.on(Wire::Command::ConsumerUpdate) do |frame|
        update = Wire::Commands.decode_consumer_update(frame)
        handler = @entity_mutex.synchronize { @consumer_update_handlers[update.subscription_id]? }
        spawn(name: "crabbit-consumer-update") do
          begin
            offset = handler ? handler.call(update.active) : OffsetSpecification.next
            connection.send(
              Wire::Commands.consumer_update_response(update.correlation_id, ResponseCode::Ok, offset),
            )
          rescue ex
            Log.warn(exception: ex) { "consumer update listener failed" }
            begin
              connection.send(
                Wire::Commands.consumer_update_response(
                  update.correlation_id,
                  ResponseCode::InternalError,
                  OffsetSpecification.none,
                ),
              )
            rescue
            end
          end
        end
      end
      connection.on(Wire::Command::Credit) do |frame|
        reader = frame.reader
        raw_code = reader.read_u16
        subscription_id = reader.read_u8
        Log.warn { "credit failed for subscription #{subscription_id}: #{raw_code}" }
      end
    end

    private def allocate_publisher_id : UInt8
      allocate_id(@publisher_ids, "publisher")
    end

    private def allocate_subscription_id : UInt8
      allocate_id(@subscription_ids, "subscription")
    end

    private def allocate_id(ids : Set(UInt8), entity : String) : UInt8
      @entity_mutex.synchronize do
        256.times do |raw|
          id = raw.to_u8
          unless ids.includes?(id)
            ids << id
            return id
          end
        end
      end
      raise ConnectionError.new("connection has no free #{entity} IDs")
    end

    private def release_publisher_id(id : UInt8) : Nil
      @entity_mutex.synchronize do
        @publisher_ids.delete(id)
        @confirm_handlers.delete(id)
        @publish_error_handlers.delete(id)
      end
    end

    private def release_subscription_id(id : UInt8) : Nil
      @entity_mutex.synchronize do
        @subscription_ids.delete(id)
        @deliver_handlers.delete(id)
        @consumer_update_handlers.delete(id)
      end
    end
  end
end
