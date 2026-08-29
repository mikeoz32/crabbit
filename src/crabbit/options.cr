module Crabbit
  # Exponential-backoff policy used when a producer or consumer reconnects.
  struct RecoveryPolicy
    # Returns the delay before the first retry.
    getter initial_delay : Time::Span
    # Returns the maximum delay between retries.
    getter max_delay : Time::Span
    # Returns the exponential multiplier applied after each failed attempt.
    getter multiplier : Float64
    # Returns the proportional random jitter in the range `0.0..1.0`.
    getter jitter : Float64

    # Creates a recovery policy.
    #
    # Delays grow as `initial_delay * multiplier ** attempt`, are capped at
    # *max_delay*, and are randomized by up to *jitter* in either direction.
    def initialize(
      @initial_delay : Time::Span = 250.milliseconds,
      @max_delay : Time::Span = 30.seconds,
      @multiplier : Float64 = 2.0,
      @jitter : Float64 = 0.2,
    )
      raise ConfigurationError.new("initial recovery delay must not be negative") if initial_delay < Time::Span.zero
      raise ConfigurationError.new("max recovery delay must be positive") unless max_delay > Time::Span.zero
      raise ConfigurationError.new("recovery multiplier must be at least 1") if multiplier < 1.0
      raise ConfigurationError.new("recovery jitter must be between 0 and 1") unless 0.0 <= jitter <= 1.0
    end

    # Returns the randomized delay for the zero-based retry *attempt*.
    def delay(attempt : Int) : Time::Span
      exponent = Math.max(attempt, 0)
      milliseconds = initial_delay.total_milliseconds * (multiplier ** exponent)
      milliseconds = Math.min(milliseconds, max_delay.total_milliseconds)
      factor = jitter == 0 ? 1.0 : 1.0 + Random.rand(-jitter..jitter)
      (milliseconds * factor).milliseconds
    end
  end

  # Lifecycle state of a `Producer` or `Consumer`.
  enum ResourceState
    # The resource is connected and usable.
    Open
    # The resource is reconnecting and redeclaring its protocol entity.
    Recovering
    # The resource was permanently closed by the application.
    Closed
  end

  # Lifecycle notification delivered to an `on_state_change` callback.
  #
  # *resource* is `"producer"` or `"consumer"`; *cause* is present when a
  # failure initiated recovery.
  record ResourceEvent,
    resource : String,
    stream : String,
    state : ResourceState,
    cause : Exception? = nil

  # Configures publisher batching, confirms, filtering, backpressure, and
  # recovery.
  class ProducerOptions
    # Returns the optional publisher reference used for broker deduplication.
    getter name : String?
    # Returns the maximum number of logical messages collected per wire batch.
    getter batch_size : Int32
    # Returns the number of logical messages packed into each sub-entry.
    getter sub_entry_size : Int32
    # Returns the compression used for sub-entry batches.
    getter compression : Compression
    # Returns the hard limit of publishes awaiting confirmation.
    getter max_unconfirmed : Int32
    # Returns how long the batch worker waits for more messages.
    getter batch_delay : Time::Span
    # Returns the default publisher-confirmation timeout.
    getter confirm_timeout : Time::Span
    # Returns the optional timeout for waiting on backpressure capacity.
    getter enqueue_timeout : Time::Span?
    # Returns whether unresolved messages are republished after recovery.
    getter retry_on_recovery : Bool
    # Returns the optional server-side filter value extractor.
    getter filter_value_extractor : Proc(Message, String?)?
    # Returns the reconnect backoff policy.
    getter recovery_policy : RecoveryPolicy
    # Returns the optional asynchronous lifecycle listener.
    getter on_state_change : Proc(ResourceEvent, Nil)?

    # Creates publisher options.
    #
    # A non-nil *name* enables broker-side publishing-ID deduplication and
    # sequence recovery. *batch_size* controls ordinary wire batching;
    # *sub_entry_size* greater than one packs logical messages into compressed
    # sub-entries. Filtering and sub-entry batching cannot be combined.
    #
    # *max_unconfirmed* applies backpressure. With no *enqueue_timeout*, the
    # publishing fiber waits indefinitely for capacity. *confirm_timeout* limits
    # how long an unresolved publish remains pending. When
    # *retry_on_recovery* is true, pending messages are sent again after the
    # publisher reconnects.
    def initialize(
      @name : String? = nil,
      @batch_size : Int32 = 100,
      @sub_entry_size : Int32 = 1,
      @compression : Compression = Compression::None,
      @max_unconfirmed : Int32 = 10_000,
      @batch_delay : Time::Span = 100.milliseconds,
      @confirm_timeout : Time::Span = 30.seconds,
      @enqueue_timeout : Time::Span? = nil,
      @retry_on_recovery : Bool = true,
      @filter_value_extractor : Proc(Message, String?)? = nil,
      @recovery_policy : RecoveryPolicy = RecoveryPolicy.new,
      @on_state_change : Proc(ResourceEvent, Nil)? = nil,
    )
      raise ConfigurationError.new("producer name must not be empty") if name.try(&.empty?)
      raise ConfigurationError.new("producer name cannot exceed 256 characters") if name.try { |value| value.size > 256 }
      raise ConfigurationError.new("batch_size must be positive") unless batch_size > 0
      raise ConfigurationError.new("sub_entry_size must be positive") unless sub_entry_size > 0
      raise ConfigurationError.new("sub_entry_size cannot exceed UInt16") if sub_entry_size > UInt16::MAX
      raise ConfigurationError.new("max_unconfirmed must be positive") unless max_unconfirmed > 0
      raise ConfigurationError.new("batch_delay must not be negative") if batch_delay < Time::Span.zero
      raise ConfigurationError.new("confirm_timeout must be positive") unless confirm_timeout > Time::Span.zero
      if timeout = enqueue_timeout
        raise ConfigurationError.new("enqueue_timeout must not be negative") if timeout < Time::Span.zero
      end
      if filter_value_extractor && sub_entry_size > 1
        raise ConfigurationError.new("filtering and sub-entry batching cannot be combined")
      end
    end
  end

  # Configures subscription position, credit, processing, offset storage,
  # filtering, Single Active Consumer behavior, and recovery.
  class ConsumerOptions
    # Returns the optional consumer reference used for stored offsets and SAC.
    getter name : String?
    # Returns the initial subscription position.
    getter offset : OffsetSpecification
    # Returns the initial number of broker chunks that may be in flight.
    getter initial_credit : UInt16
    # Returns the server-side filter values.
    getter filters : Array(String)
    # Returns whether messages without a filter value also match.
    getter match_unfiltered : Bool
    # Returns whether Single Active Consumer semantics are enabled.
    getter single_active_consumer : Bool
    # Returns the parent super-stream name added to subscription properties.
    getter super_stream : String?
    # Returns the number of callback-processing fibers.
    getter concurrency : Int32
    # Returns the capacity of the logical delivery queue.
    getter buffer_size : Int32
    # Returns whether Deliver chunk CRC32 values are verified.
    getter validate_crc : Bool
    # Returns the optional processed-message threshold for automatic offset storage.
    getter auto_store_every : Int32?
    # Returns the optional interval for automatic offset storage.
    getter auto_store_interval : Time::Span?
    # Returns the reconnect backoff policy.
    getter recovery_policy : RecoveryPolicy
    # Returns the legacy Single Active Consumer update listener.
    getter on_consumer_update : Proc(Bool, OffsetSpecification)?
    # Returns the context-aware Single Active Consumer update listener.
    getter on_consumer_update_context : Proc(ConsumerUpdateContext, OffsetSpecification)?
    # Returns the optional per-partition starting-offset resolver.
    getter subscription_offset : Proc(String, OffsetSpecification)?
    # Returns the optional asynchronous lifecycle listener.
    getter on_state_change : Proc(ResourceEvent, Nil)?
    # Returns the super-stream partition refresh interval.
    getter topology_refresh : Time::Span

    # Creates consumer options.
    #
    # *initial_credit* is measured in chunks, while *buffer_size* is measured in
    # logical messages. Callback consumers use *concurrency* handler fibers;
    # pull consumers ignore it. Broker filtering accepts multiple OR-matched
    # *filters* and can optionally include unfiltered messages.
    #
    # Automatic storage requires *name*. `auto_store_every` stores after a
    # number of processed deliveries, and `auto_store_interval` stores the
    # latest contiguous processed offset periodically. With Single Active
    # Consumer enabled, one of the update callbacks may choose the offset each
    # time the subscription becomes active or inactive.
    def initialize(
      @name : String? = nil,
      @offset : OffsetSpecification = OffsetSpecification.next,
      @initial_credit : UInt16 = 10_u16,
      @filters : Array(String) = [] of String,
      @match_unfiltered : Bool = false,
      @single_active_consumer : Bool = false,
      @super_stream : String? = nil,
      @concurrency : Int32 = 1,
      @buffer_size : Int32 = 1_024,
      @validate_crc : Bool = true,
      @auto_store_every : Int32? = nil,
      @auto_store_interval : Time::Span? = nil,
      @recovery_policy : RecoveryPolicy = RecoveryPolicy.new,
      @on_consumer_update : Proc(Bool, OffsetSpecification)? = nil,
      @on_consumer_update_context : Proc(ConsumerUpdateContext, OffsetSpecification)? = nil,
      @subscription_offset : Proc(String, OffsetSpecification)? = nil,
      @on_state_change : Proc(ResourceEvent, Nil)? = nil,
      @topology_refresh : Time::Span = 30.seconds,
    )
      raise ConfigurationError.new("consumer name must not be empty") if name.try(&.empty?)
      raise ConfigurationError.new("consumer name cannot exceed 256 characters") if name.try { |value| value.size > 256 }
      raise ConfigurationError.new("initial_credit must be positive") unless initial_credit > 0
      raise ConfigurationError.new("concurrency must be positive") unless concurrency > 0
      raise ConfigurationError.new("buffer_size must be positive") unless buffer_size > 0
      raise ConfigurationError.new("topology_refresh must be positive") unless topology_refresh > Time::Span.zero
      if count = auto_store_every
        raise ConfigurationError.new("auto_store_every must be positive") unless count > 0
        raise ConfigurationError.new("auto-store requires a named consumer") unless name
      end
      if interval = auto_store_interval
        raise ConfigurationError.new("auto_store_interval must be positive") unless interval > Time::Span.zero
        raise ConfigurationError.new("auto-store requires a named consumer") unless name
      end
      if single_active_consumer && !name
        raise ConfigurationError.new("single-active-consumer requires a consumer name")
      end
      if on_consumer_update && on_consumer_update_context
        raise ConfigurationError.new(
          "on_consumer_update and on_consumer_update_context cannot both be configured",
        )
      end
      @filters = filters.dup
    end
  end

  # Built-in strategy for mapping a super-stream routing key to partitions.
  enum SuperStreamRouting
    # Selects one partition using RabbitMQ's seeded Murmur3 strategy.
    Hash
    # Uses the broker's Route command and super-stream binding keys.
    RoutingKey
  end

  # Configures a `SuperStreamProducer` and its partition routing.
  class SuperStreamProducerOptions
    # Returns options shared by every partition producer.
    getter producer : ProducerOptions
    # Returns the built-in routing mode.
    getter routing : SuperStreamRouting
    # Returns the routing-key extractor used by built-in routing.
    getter routing_key_extractor : Proc(Message, String)?
    # Returns the optional custom hash function.
    getter hash_function : Proc(String, UInt32)?
    # Returns the optional custom partition-selection strategy.
    getter routing_strategy : Proc(Message, Array(String), Array(String))?
    # Returns how often cached topology may be refreshed.
    getter topology_refresh : Time::Span

    # Creates super-stream producer options.
    #
    # Supply either *routing_key_extractor* or *routing_strategy*. A custom
    # strategy receives the message and current partition names and may return
    # one or more of those partitions. When using `SuperStreamRouting::Hash`,
    # *hash_function* can replace the RabbitMQ-compatible default.
    def initialize(
      @routing_key_extractor : Proc(Message, String)? = nil,
      @routing : SuperStreamRouting = SuperStreamRouting::Hash,
      @producer : ProducerOptions = ProducerOptions.new,
      @hash_function : Proc(String, UInt32)? = nil,
      @routing_strategy : Proc(Message, Array(String), Array(String))? = nil,
      @topology_refresh : Time::Span = 30.seconds,
    )
      raise ConfigurationError.new("topology_refresh must be positive") unless topology_refresh > Time::Span.zero
      unless routing_key_extractor || routing_strategy
        raise ConfigurationError.new("a routing key extractor or custom routing strategy is required")
      end
    end
  end
end
