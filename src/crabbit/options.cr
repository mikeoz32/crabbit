module Crabbit
  struct RecoveryPolicy
    getter initial_delay : Time::Span
    getter max_delay : Time::Span
    getter multiplier : Float64
    getter jitter : Float64

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

    def delay(attempt : Int) : Time::Span
      exponent = Math.max(attempt, 0)
      milliseconds = initial_delay.total_milliseconds * (multiplier ** exponent)
      milliseconds = Math.min(milliseconds, max_delay.total_milliseconds)
      factor = jitter == 0 ? 1.0 : 1.0 + Random.rand(-jitter..jitter)
      (milliseconds * factor).milliseconds
    end
  end

  enum ResourceState
    Open
    Recovering
    Closed
  end

  record ResourceEvent,
    resource : String,
    stream : String,
    state : ResourceState,
    cause : Exception? = nil

  class ProducerOptions
    getter name : String?
    getter batch_size : Int32
    getter sub_entry_size : Int32
    getter compression : Compression
    getter max_unconfirmed : Int32
    getter batch_delay : Time::Span
    getter confirm_timeout : Time::Span
    getter enqueue_timeout : Time::Span?
    getter retry_on_recovery : Bool
    getter filter_value_extractor : Proc(Message, String?)?
    getter recovery_policy : RecoveryPolicy
    getter on_state_change : Proc(ResourceEvent, Nil)?

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

  class ConsumerOptions
    getter name : String?
    getter offset : OffsetSpecification
    getter initial_credit : UInt16
    getter filters : Array(String)
    getter match_unfiltered : Bool
    getter single_active_consumer : Bool
    getter super_stream : String?
    getter concurrency : Int32
    getter buffer_size : Int32
    getter validate_crc : Bool
    getter auto_store_every : Int32?
    getter auto_store_interval : Time::Span?
    getter recovery_policy : RecoveryPolicy
    getter on_consumer_update : Proc(Bool, OffsetSpecification)?
    getter on_consumer_update_context : Proc(ConsumerUpdateContext, OffsetSpecification)?
    getter subscription_offset : Proc(String, OffsetSpecification)?
    getter on_state_change : Proc(ResourceEvent, Nil)?
    getter topology_refresh : Time::Span

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

  enum SuperStreamRouting
    Hash
    RoutingKey
  end

  class SuperStreamProducerOptions
    getter producer : ProducerOptions
    getter routing : SuperStreamRouting
    getter routing_key_extractor : Proc(Message, String)?
    getter hash_function : Proc(String, UInt32)?
    getter routing_strategy : Proc(Message, Array(String), Array(String))?
    getter topology_refresh : Time::Span

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
