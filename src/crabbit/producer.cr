module Crabbit
  # Final outcome of one logical publish.
  #
  # `confirmed` is true only for a positive broker confirmation. On failure,
  # `code` may contain the broker response and `error` contains the associated
  # exception. A confirmation resolves exactly once.
  record Confirmation,
    publishing_id : UInt64,
    confirmed : Bool,
    code : ResponseCode? = nil,
    error : Exception? = nil

  private alias ConfirmationCallback = Proc(Confirmation, Nil)

  private class ConfirmationCallbackSlot
    getter callback : ConfirmationCallback

    def initialize(@callback : ConfirmationCallback)
    end

    def append(callback : ConfirmationCallback) : Nil
      previous = @callback
      @callback = ->(confirmation : Confirmation) do
        begin
          previous.call(confirmation)
        rescue ex
          Log.error(exception: ex) { "publish confirmation callback failed" }
        end
        callback.call(confirmation)
      end
    end
  end

  private record ConfirmationCallbackJob,
    callback : ConfirmationCallback,
    confirmation : Confirmation

  # User callbacks must not run on the connection reader fiber: they can
  # legitimately publish, close resources, or perform IO that depends on that
  # reader making progress. A single long-lived dispatcher preserves that
  # isolation without allocating a Fiber for every confirmation.
  private class ConfirmationCallbackDispatcher
    {% if flag?(:execution_context) || compare_versions(Crystal::VERSION, "1.21.0") >= 0 %}
      @@execution_context = Fiber::ExecutionContext::Concurrent.new("crabbit-confirmations")
    {% end %}

    @mutex = Mutex.new
    @jobs = Deque(ConfirmationCallbackJob).new
    @signal = Channel(Nil).new(1)
    @closed = false

    def start : Nil
      {% if flag?(:execution_context) || compare_versions(Crystal::VERSION, "1.21.0") >= 0 %}
        @@execution_context.spawn(name: "crabbit-publish-confirmations") { run }
      {% else %}
        spawn(name: "crabbit-publish-confirmations") { run }
      {% end %}
    end

    def dispatch(callback : ConfirmationCallback, confirmation : Confirmation) : Nil
      accepted = @mutex.synchronize do
        if @closed
          false
        else
          @jobs << ConfirmationCallbackJob.new(callback, confirmation)
          true
        end
      end
      if accepted
        wake
      else
        {% if flag?(:execution_context) || compare_versions(Crystal::VERSION, "1.21.0") >= 0 %}
          @@execution_context.spawn(name: "crabbit-late-publish-confirmation") do
            invoke(callback, confirmation)
          end
        {% else %}
          spawn(name: "crabbit-late-publish-confirmation") do
            invoke(callback, confirmation)
          end
        {% end %}
      end
    end

    def close : Nil
      @mutex.synchronize { @closed = true }
      wake
    end

    private def run : Nil
      loop do
        @signal.receive
        while job = @mutex.synchronize { @jobs.shift? }
          invoke(job.callback, job.confirmation)
        end
        break if @mutex.synchronize { @closed && @jobs.empty? }
      end
    end

    private def wake : Nil
      select
      when @signal.send(nil)
      else
      end
    rescue Channel::ClosedError
    end

    private def invoke(callback : ConfirmationCallback, confirmation : Confirmation) : Nil
      callback.call(confirmation)
    rescue ex
      Log.error(exception: ex) { "publish confirmation callback failed" }
    end
  end

  private class PendingPublish
    getter publishing_id : UInt64
    getter bytes : Bytes
    getter filter : String?
    getter payload_format : Internal::Wire::PublishPayloadFormat
    getter created_at : Time::Instant

    @mutex = Mutex.new
    @completed = false
    @sent = false
    @confirmation : Confirmation?
    @callback_slot : ConfirmationCallbackSlot?
    @done : Channel(Nil)?

    def initialize(
      @publishing_id : UInt64,
      @bytes : Bytes,
      @filter : String?,
      @payload_format : Internal::Wire::PublishPayloadFormat,
      @callback_dispatcher : ConfirmationCallbackDispatcher,
    )
      @created_at = Time.instant
    end

    def wire_entry : Internal::Wire::PublishEntry
      Internal::Wire::PublishEntry.new(publishing_id, bytes, filter, payload_format)
    end

    def encoded_bytes : Bytes
      if payload_format.data?
        raise ProtocolError.new("AMQP Data payload reached the sub-entry encoder")
      end
      bytes
    end

    def completed? : Bool
      @mutex.synchronize { @completed }
    end

    def sent? : Bool
      @mutex.synchronize { @sent }
    end

    def mark_sent : Nil
      @mutex.synchronize { @sent = true }
    end

    def complete(confirmation : Confirmation) : Bool
      @mutex.synchronize do
        return false if @completed
        @completed = true
        @confirmation = confirmation
        callback_slot = @callback_slot
        @callback_slot = nil
        # Queue callbacks registered before completion while the state lock
        # still excludes late registrations. The dispatcher never invokes user
        # code synchronously, so callbacks retain registration order without
        # running under this mutex or on the connection reader fiber.
        if callback_slot
          @callback_dispatcher.dispatch(callback_slot.callback, confirmation)
        end
      end
      @done.try(&.close)
      true
    end

    def on_complete(&callback : Confirmation ->) : Nil
      result = @mutex.synchronize do
        if @completed
          @confirmation
        else
          if callback_slot = @callback_slot
            callback_slot.append(callback)
          else
            @callback_slot = ConfirmationCallbackSlot.new(callback)
          end
          nil
        end
      end
      @callback_dispatcher.dispatch(callback, result) if result
    end

    def await(timeout_span : Time::Span) : Confirmation
      completed, confirmation, done = @mutex.synchronize do
        {@completed, @confirmation, (@done ||= Channel(Nil).new)}
      end
      return confirmation.not_nil! if completed

      select
      when done.receive?
        @mutex.synchronize { @confirmation.not_nil! }
      when timeout(timeout_span)
        raise TimeoutError.new(
          "publisher confirmation #{@publishing_id} did not arrive within #{timeout_span}",
        )
      end
    end
  end

  private struct PendingPublishingIds
    include Enumerable(UInt64)

    def initialize(@messages : Array(PendingPublish), @start : Int32, @count : Int32)
    end

    def each(& : UInt64 ->) : Nil
      @count.times do |offset|
        yield @messages[@start + offset].publishing_id
      end
    end
  end

  # Future-like handle for an asynchronous publish confirmation.
  #
  # Use `#await` for fiber-blocking code or `#on_confirm` to receive the result
  # asynchronously. Registering a callback after completion still invokes it.
  class PublishHandle
    # Returns the publishing ID assigned to the message.
    getter publishing_id : UInt64

    # :nodoc:
    def initialize(@pending : PendingPublish, @default_timeout : Time::Span)
      @publishing_id = pending.publishing_id
    end

    # Waits for and returns the final confirmation.
    #
    # Raises `TimeoutError` if the handle has not resolved within *timeout*.
    # This wait timeout does not cancel the underlying publish.
    def await(timeout : Time::Span = @default_timeout) : Confirmation
      @pending.await(timeout)
    end

    # Schedules *callback* to run once with the final confirmation.
    #
    # Callbacks run outside the connection reader fiber. Returns `self`.
    def on_confirm(&callback : Confirmation ->) : self
      @pending.on_complete(&callback)
      self
    end

    # Returns whether the publish already has a final outcome.
    def completed? : Bool
      @pending.completed?
    end
  end

  # Asynchronous publisher for one RabbitMQ stream.
  #
  # Producers batch queued messages, enforce `ProducerOptions#max_unconfirmed`,
  # and recover indefinitely until closed. Publishing queues work and returns a
  # `PublishHandle`; it does not wait for a broker confirmation.
  #
  # Named producers resume the broker sequence and support deduplication.
  # Unnamed producers provide at-least-once recovery and can produce duplicates
  # when a connection fails with an unknown outcome.
  class Producer
    # Returns the target stream name.
    getter stream : String
    # Returns the immutable producer options.
    getter options : ProducerOptions

    @state_mutex = Mutex.new
    @confirm_mutex = Mutex.new
    @state = ResourceState::Recovering
    @recovering = false
    @send_mutex = Mutex.new
    @sequence_mutex = Mutex.new
    @next_publishing_id = 0_u64
    @confirmations = Internal::ConfirmationTracker(PendingPublish).new
    @confirmed = [] of PendingPublish
    @queue : Channel(PendingPublish)
    @permits : Channel(Nil)
    @client : Internal::Client? = nil
    @publisher_id : UInt8? = nil
    @disconnect_registration : Tuple(Internal::Client, Internal::Connection::DisconnectHandler)? = nil
    @metadata_registration : Tuple(Internal::Client, Internal::Connection::FrameHandler)? = nil
    @entity_id = 0_u64
    @callback_dispatcher : ConfirmationCallbackDispatcher

    # Creates and declares a producer.
    #
    # Applications normally call `Environment#producer` so the environment can
    # own and close the resource.
    def initialize(@environment : Environment, @stream : String, @options : ProducerOptions)
      @callback_dispatcher = ConfirmationCallbackDispatcher.new
      raise ConfigurationError.new("stream must not be empty") if stream.empty?
      @queue = Channel(PendingPublish).new(options.max_unconfirmed)
      @permits = Channel(Nil).new(options.max_unconfirmed)
      options.max_unconfirmed.times { @permits.send(nil) }
      connect_initial!
      @entity_id = environment.register_entity { close }
      @callback_dispatcher.start
      start_batch_worker
      start_confirmation_timeout_worker
      transition(ResourceState::Open)
    end

    # Returns the current lifecycle state.
    def state : ResourceState
      @state_mutex.synchronize { @state }
    end

    # Returns whether this producer is currently ready to publish.
    def open? : Bool
      state.open?
    end

    # Returns whether this producer was permanently closed.
    def closed? : Bool
      state.closed?
    end

    # Returns the number of logical messages awaiting a final outcome.
    def unconfirmed_count : Int32
      @confirmations.size
    end

    # Queries RabbitMQ for the last publishing ID of this named producer.
    #
    # Raises `ConfigurationError` for an unnamed producer.
    def last_publishing_id : UInt64
      name = options.name || raise ConfigurationError.new(
        "querying the last publishing ID requires a named producer",
      )
      @environment.query_publisher_sequence(name, stream)
    end

    # Publishes an AMQP *message* and returns immediately with a handle.
    #
    # *filter* selects the server-side filter value and takes precedence over
    # `ProducerOptions#filter_value_extractor`. *publishing_id* overrides the
    # automatically allocated monotonically increasing ID.
    def publish(
      message : Message,
      filter : String? = nil,
      *,
      publishing_id : UInt64? = nil,
    ) : PublishHandle
      effective_filter = filter || options.filter_value_extractor.try(&.call(message))
      enqueue(message.to_amqp, effective_filter, publishing_id)
    end

    # Publishes an AMQP *message* and invokes the block on completion.
    #
    # Returns the same handle that can also be awaited.
    def publish(
      message : Message,
      filter : String? = nil,
      *,
      publishing_id : UInt64? = nil,
      &callback : Confirmation ->
    ) : PublishHandle
      publish(message, filter, publishing_id: publishing_id).on_confirm(&callback)
    end

    # Publishes already encoded AMQP bytes without re-encoding them.
    def publish(
      message : RawMessage,
      filter : String? = nil,
      *,
      publishing_id : UInt64? = nil,
    ) : PublishHandle
      enqueue(message.bytes, filter, publishing_id)
    end

    # Publishes already encoded AMQP bytes and invokes the block on completion.
    def publish(
      message : RawMessage,
      filter : String? = nil,
      *,
      publishing_id : UInt64? = nil,
      &callback : Confirmation ->
    ) : PublishHandle
      publish(message, filter, publishing_id: publishing_id).on_confirm(&callback)
    end

    # Copies *bytes*, wraps them in one AMQP Data section, and publishes the message.
    # The caller may mutate the original slice after this method returns.
    def publish(
      bytes : Bytes,
      filter : String? = nil,
      *,
      publishing_id : UInt64? = nil,
    ) : PublishHandle
      if options.sub_entry_size > 1
        enqueue(AMQP::MessageCodec.encode_data(bytes), filter, publishing_id)
      else
        enqueue(
          bytes.dup,
          filter,
          publishing_id,
          Internal::Wire::PublishPayloadFormat::Data,
        )
      end
    end

    # Wraps *bytes* in one AMQP Data section, publishes it, and invokes the
    # block on completion.
    def publish(
      bytes : Bytes,
      filter : String? = nil,
      *,
      publishing_id : UInt64? = nil,
      &callback : Confirmation ->
    ) : PublishHandle
      publish(bytes, filter, publishing_id: publishing_id).on_confirm(&callback)
    end

    # Encodes *value* as one AMQP Data section and publishes it.
    def publish(
      value : String,
      filter : String? = nil,
      *,
      publishing_id : UInt64? = nil,
    ) : PublishHandle
      publish(value.to_slice, filter, publishing_id: publishing_id)
    end

    # Encodes *value* as one AMQP Data section, publishes it, and invokes the
    # block on completion.
    def publish(
      value : String,
      filter : String? = nil,
      *,
      publishing_id : UInt64? = nil,
      &callback : Confirmation ->
    ) : PublishHandle
      publish(value, filter, publishing_id: publishing_id).on_confirm(&callback)
    end

    # Publishes one message and waits for its final confirmation.
    #
    # This convenience method does not raise negative confirmations; inspect
    # `Confirmation#confirmed` and `Confirmation#error`.
    def publish_confirmed(
      message : Message | RawMessage | Bytes | String,
      timeout : Time::Span = options.confirm_timeout,
    ) : Confirmation
      publish(message).await(timeout)
    end

    # Waits until every currently tracked publish has resolved.
    #
    # Raises `TimeoutError` if unresolved messages remain after *timeout*.
    def wait_for_confirms(timeout : Time::Span = options.confirm_timeout) : Nil
      deadline = Time.instant + timeout
      loop do
        return if unconfirmed_count == 0
        remaining = deadline - Time.instant
        raise TimeoutError.new("producer still has unconfirmed messages after #{timeout}") if remaining <= Time::Span.zero
        sleep Math.min(remaining, 10.milliseconds)
      end
    end

    # Idempotently deletes the publisher and fails unresolved handles.
    def close : Nil
      return unless mark_closed
      begin
        client, publisher_id = @state_mutex.synchronize { {@client, @publisher_id} }
        if client
          if id = publisher_id
            client.delete_publisher(id) if client.open?
          end
        end
      rescue ex
        Log.debug(exception: ex) { "could not delete publisher while closing" }
      ensure
        remove_disconnect_registration
        remove_metadata_registration
        close_internal(ResourceClosedError.new("producer is closed"))
        @environment.unregister_entity(@entity_id) unless @entity_id == 0
        notify(ResourceState::Closed)
      end
    end

    private def enqueue(
      bytes : Bytes,
      filter : String?,
      publishing_id : UInt64? = nil,
      payload_format : Internal::Wire::PublishPayloadFormat = Internal::Wire::PublishPayloadFormat::Encoded,
    ) : PublishHandle
      raise ResourceClosedError.new("producer is closed") if closed?
      if filter && options.sub_entry_size > 1
        raise ConfigurationError.new("filtering and sub-entry batching cannot be combined")
      end
      acquire_permit!
      id = reserve_publishing_id(publishing_id)
      pending = PendingPublish.new(id, bytes, filter, payload_format, @callback_dispatcher)
      unless @confirmations.add(id, pending)
        @permits.send(nil)
        raise ArgumentError.new("publishing ID #{id} is already awaiting confirmation")
      end
      begin
        @queue.send(pending)
      rescue Channel::ClosedError
        finish(id, Confirmation.new(id, false, error: ResourceClosedError.new("producer is closed")))
        raise ResourceClosedError.new("producer is closed")
      end
      PublishHandle.new(pending, options.confirm_timeout)
    end

    private def acquire_permit! : Nil
      if timeout_span = options.enqueue_timeout
        select
        when @permits.receive
        when timeout(timeout_span)
          raise TimeoutError.new("producer max_unconfirmed limit was reached")
        end
      else
        @permits.receive
      end
      if closed?
        @permits.send(nil)
        raise ResourceClosedError.new("producer is closed")
      end
    end

    private def reserve_publishing_id(explicit : UInt64?) : UInt64
      @sequence_mutex.synchronize do
        if id = explicit
          @next_publishing_id = id &+ 1_u64 if id >= @next_publishing_id
          return id
        end

        value = @next_publishing_id
        @next_publishing_id &+= 1_u64
        value
      end
    end

    private def connect_initial! : Nil
      client = @environment.producer_client(stream)
      if name = options.name
        @next_publishing_id = client.query_publisher_sequence(name, stream)
        @next_publishing_id &+= 1_u64 unless @next_publishing_id == 0
      end
      attach(client)
    end

    private def attach(client : Internal::Client) : Bool
      publisher_id = client.declare_publisher(
        stream,
        options.name,
        ->(ids : Internal::Wire::PublishConfirmationIds) { confirm(ids) },
        ->(errors : Hash(UInt64, ResponseCode)) { fail(client, errors) },
      )
      accepted = @state_mutex.synchronize do
        unless @state.closed?
          @client = client
          @publisher_id = publisher_id
          true
        else
          false
        end
      end
      unless accepted
        client.delete_publisher(publisher_id) if client.open?
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

    private def start_batch_worker : Nil
      spawn(name: "crabbit-producer-#{stream}") do
        while first = @queue.receive?
          batch = Array(PendingPublish).new(Math.min(options.batch_size, 1_024))
          begin
            batch << first
            deadline = Time.instant + options.batch_delay
            while batch.size < options.batch_size
              remaining = deadline - Time.instant
              break if remaining <= Time::Span.zero
              select
              when item = @queue.receive?
                break unless item
                batch << item
              when timeout(remaining)
                break
              end
            end
            batch.reject!(&.completed?)
            next if batch.empty?
            wait_until_available
            batch.reject!(&.completed?)
            next if batch.empty?
            transmit(batch) unless closed?
          rescue ex
            unless closed?
              Log.warn(exception: ex) { "producer batch failed; scheduling recovery" }
              begin_recovery(ex)
              batch.each do |item|
                @queue.send(item) unless item.completed? || item.sent?
              end
            end
          end
        end
      end
    end

    private def start_confirmation_timeout_worker : Nil
      interval = Math.min(options.confirm_timeout / 2, 1.second)
      spawn(name: "crabbit-producer-timeouts-#{stream}") do
        until closed?
          sleep interval
          cutoff = Time.instant - options.confirm_timeout
          expired = @confirmations.values.select { |pending| pending.created_at <= cutoff }
          expired.each do |pending|
            error = TimeoutError.new("publish #{pending.publishing_id} was not confirmed")
            finish(
              pending.publishing_id,
              Confirmation.new(pending.publishing_id, false, error: error),
              defer_if_transmitting: true,
            )
          end
        end
      end
    end

    private def wait_until_available : Nil
      until closed? || open?
        sleep 10.milliseconds
      end
    end

    private def transmit(messages : Array(PendingPublish)) : Nil
      @send_mutex.synchronize do
        client, publisher_id = @state_mutex.synchronize { {@client, @publisher_id} }
        client ||= raise ConnectionClosedError.new("producer has no connection")
        publisher_id ||= raise ConnectionClosedError.new("producer is not declared")
        if options.sub_entry_size > 1
          transmit_sub_entries(client, publisher_id, messages)
        else
          transmit_messages(client, publisher_id, messages)
        end
      end
    end

    private def transmit_messages(
      client : Internal::Client,
      publisher_id : UInt8,
      messages : Array(PendingPublish),
    ) : Nil
      each_message_partition(messages, client.connection.negotiated_max_frame_size) do |start, count|
        ids = PendingPublishingIds.new(messages, start, count)
        @confirmations.with_pending_singles(ids, count) do |pending_messages|
          active = pending_messages.map do |pending|
            pending.mark_sent
            pending.wire_entry
          end

          version = active.any?(&.filter) ? client.connection.version(Internal::Wire::Command::Publish, 2_u16) : 1_u16
          client.connection.send_publish(publisher_id, active, version)
        end
      end
    end

    private def each_message_partition(
      messages : Array(PendingPublish),
      max_frame_size : UInt32,
      &send : Int32, Int32 ->
    ) : Nil
      start = 0
      current_size = 13_u64
      messages.each_with_index do |message, index|
        entry_size = message.wire_entry.wire_size(2_u16).to_u64
        candidate_size = current_size + entry_size
        if max_frame_size > 0 && candidate_size > max_frame_size
          if index == start
            raise FrameTooLargeError.new(candidate_size.to_u32!, max_frame_size)
          end
          yield start, index - start
          start = index
          current_size = 13_u64 + entry_size
          if max_frame_size > 0 && current_size > max_frame_size
            raise FrameTooLargeError.new(current_size.to_u32!, max_frame_size)
          end
        else
          current_size = candidate_size
        end
      end
      yield start, messages.size - start if start < messages.size
    end

    private def transmit_sub_entries(
      client : Internal::Client,
      publisher_id : UInt8,
      messages : Array(PendingPublish),
    ) : Nil
      codec = Internal::SubEntryCodec.new(@environment.compression_codecs)
      groups = messages.each_slice(options.sub_entry_size).map do |slice|
        slice.map(&.publishing_id)
      end.to_a
      @confirmations.with_pending_groups(groups) do |prepared|
        entries = prepared.map do |root_id, pending_messages|
          pending_messages.each(&.mark_sent)
          encoded = codec.encode(pending_messages.map(&.encoded_bytes), options.compression)
          {root_id, encoded, pending_messages.map(&.publishing_id)}
        end
        send_partitioned(entries, client.connection.negotiated_max_frame_size) do |slice|
          wire_entries = slice.map { |entry| {entry[0], entry[1]} }
          client.connection.send(Internal::Wire::Commands.publish_sub_entries(publisher_id, wire_entries))
        end
      end
    end

    private def send_partitioned(entries : Array(T), max_frame_size : UInt32, &send_slice : Array(T) ->) : Nil forall T
      capacity = Math.min(entries.size, 1_024)
      current = Array(T).new(capacity)
      current_size = 13_u64
      entries.each do |entry|
        entry_size = yield_entry_size(entry)
        candidate_size = current_size + entry_size
        if max_frame_size > 0 && candidate_size > max_frame_size
          if current.empty?
            raise FrameTooLargeError.new(candidate_size.to_u32!, max_frame_size)
          end
          yield current
          current = Array(T).new(capacity)
          current << entry
          current_size = 13_u64 + entry_size
          single_size = current_size
          if max_frame_size > 0 && single_size > max_frame_size
            raise FrameTooLargeError.new(single_size.to_u32!, max_frame_size)
          end
        else
          current << entry
          current_size = candidate_size
        end
      end
      yield current unless current.empty?
    end

    # Calculate with the exact encoder by invoking the caller against a sizing
    # connection would be invasive. The protocol overhead is fixed and this
    # conservative estimate includes all simple and sub-entry fields.
    private def yield_entry_size(entry : T) : UInt64 forall T
      case entry
      when Internal::Wire::PublishEntry
        entry.wire_size(2_u16).to_u64
      when Tuple(UInt64, Internal::EncodedSubEntry, Array(UInt64))
        19_u64 + entry[1].data.size
      else
        0_u64
      end
    end

    private def confirm(ids : Internal::Wire::PublishConfirmationIds) : Nil
      @confirm_mutex.synchronize do
        @confirmed.clear
        @confirmations.finish_confirmed(ids, @confirmed)
        @confirmed.each do |pending|
          id = pending.publishing_id
          # Release backpressure before invoking a confirmation callback. A
          # callback is then free to publish the next message even when the
          # producer was at max_unconfirmed capacity.
          @permits.send(nil)
          pending.complete(Confirmation.new(id, true, ResponseCode::Ok))
        end
        @confirmed.clear
      end
    end

    private def fail(source : Internal::Client, errors : Hash(UInt64, ResponseCode)) : Nil
      return unless @state_mutex.synchronize { @client.same?(source) }

      publisher_missing = false
      errors.each do |root_id, code|
        if code.publisher_does_not_exist?
          publisher_missing = true
        else
          @confirmations.each_group(root_id) do |id|
            finish(id, Confirmation.new(id, false, code, BrokerError.new(code)))
          end
        end
      end
      if publisher_missing
        begin_recovery(BrokerError.new(ResponseCode::PublisherDoesNotExist))
      end
    end

    private def finish(
      id : UInt64,
      confirmation : Confirmation,
      defer_if_transmitting : Bool = false,
    ) : Nil
      pending = @confirmations.finish(id, defer_if_transmitting)
      if pending
        # Release backpressure before invoking a confirmation callback. A
        # callback is then free to publish the next message even when the
        # producer was at max_unconfirmed capacity.
        @permits.send(nil)
        pending.complete(confirmation)
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
      spawn(name: "crabbit-producer-recovery-#{stream}") { recover }
    end

    private def recover : Nil
      attempt = 0
      loop do
        return if closed?
        begin
          forget_current_publisher
          client = @environment.producer_client(stream)
          return unless attach(client)
          @confirmations.clear_groups
          if options.retry_on_recovery
            pending = @confirmations.values.select(&.sent?).sort_by(&.publishing_id)
            transmit(pending) unless pending.empty?
          else
            pending = @confirmations.values
            pending.each do |item|
              error = ConnectionClosedError.new("message was unconfirmed during recovery")
              finish(item.publishing_id, Confirmation.new(item.publishing_id, false, error: error))
            end
          end
          raise ConnectionClosedError.new("producer connection closed during recovery") unless client.open?
          @state_mutex.synchronize do
            @state = ResourceState::Open
            @recovering = false
          end
          notify(ResourceState::Open)
          return
        rescue ex
          Log.warn(exception: ex) { "producer recovery attempt #{attempt + 1} failed" }
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
        event = ResourceEvent.new("producer", stream, state, cause)
        spawn(name: "crabbit-producer-state") { listener.call(event) }
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

    private def close_internal(cause : Exception) : Nil
      begin
        @queue.close
      rescue
      end
      pending = @confirmations.take_all
      pending.each do |item|
        @permits.send(nil)
        item.complete(Confirmation.new(item.publishing_id, false, error: cause))
      end
      @callback_dispatcher.close
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

    private def forget_current_publisher : Nil
      client, publisher_id = @state_mutex.synchronize { {@client, @publisher_id} }
      client.forget_publisher(publisher_id) if client && publisher_id
    end
  end
end
