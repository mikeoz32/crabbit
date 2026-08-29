module Crabbit
  # Identifies the wire-level starting point of a stream subscription.
  #
  # Prefer the constructors on `OffsetSpecification` instead of instantiating
  # this enum directly.
  enum OffsetType : UInt16
    None      = 0_u16
    First     = 1_u16
    Last      = 2_u16
    Next      = 3_u16
    Offset    = 4_u16
    Timestamp = 5_u16
  end

  # Describes where a consumer starts reading a stream.
  #
  # `first`, `last`, and `next` are relative positions. `offset` addresses an
  # exact stream offset, while `timestamp` asks the broker for the first chunk
  # at or after a point in time.
  struct OffsetSpecification
    # Returns the requested offset mode.
    getter type : OffsetType

    # Returns the numeric offset or Unix timestamp in milliseconds, when the
    # selected mode requires one.
    getter value : Int64 | UInt64 | Nil

    private def initialize(@type : OffsetType, @value : Int64 | UInt64 | Nil = nil)
    end

    # Starts at the first available message in the stream.
    def self.first : self
      new(OffsetType::First)
    end

    # Starts at the beginning of the last committed chunk.
    def self.last : self
      new(OffsetType::Last)
    end

    # Starts after the current end of the stream and receives new messages.
    def self.next : self
      new(OffsetType::Next)
    end

    # Starts at the exact non-negative stream *value*.
    def self.offset(value : Int) : self
      raise ArgumentError.new("offset must not be negative") if value < 0
      new(OffsetType::Offset, value.to_u64)
    end

    # Starts at the first chunk whose timestamp is at or after *value*.
    def self.timestamp(value : Time) : self
      new(OffsetType::Timestamp, value.to_unix_ms)
    end

    # Starts at the first chunk at or after the Unix timestamp *milliseconds*.
    def self.timestamp(milliseconds : Int) : self
      new(OffsetType::Timestamp, milliseconds.to_i64)
    end

    # Requests no initial delivery.
    #
    # This is primarily useful as the inactive result of a Single Active
    # Consumer update callback.
    def self.none : self
      new(OffsetType::None)
    end
  end

  # Network address of a RabbitMQ Stream listener.
  #
  # *tls* controls whether the connection uses TLS. The default plain-text port
  # is 5552; use `.tls` for the conventional TLS port 5551.
  record Endpoint, host : String, port : Int32 = 5552, tls : Bool = false do
    # Creates a TLS endpoint for *host* and *port*.
    def self.tls(host : String, port : Int32 = 5551) : self
      new(host, port, true)
    end
  end

  # Broker address returned by RabbitMQ stream metadata.
  #
  # *reference* is scoped to a metadata response and should not be persisted.
  record Broker, reference : UInt16, host : String, port : Int32 do
    # Converts the advertised broker address to an `Endpoint`.
    def endpoint(tls : Bool = false) : Endpoint
      Endpoint.new(host, port, tls)
    end
  end

  # Metadata for one stream, including its current leader and readable replicas.
  record StreamMetadata,
    stream : String,
    response_code : ResponseCode,
    leader : Broker?,
    replicas : Array(Broker)

  # :nodoc:
  record CommandVersion, key : UInt16, min_version : UInt16, max_version : UInt16 do
    def supports?(version : UInt16) : Bool
      min_version <= version <= max_version
    end

    def highest_common(min : UInt16, max : UInt16) : UInt16?
      lower = Math.max(min_version, min)
      upper = Math.min(max_version, max)
      lower <= upper ? upper : nil
    end
  end

  # Broker-provided statistics for a stream.
  #
  # Keys are RabbitMQ protocol names such as `first_chunk_id`,
  # `last_chunk_id`, `committed_chunk_id`, and `stream_size`. Unknown future
  # keys are preserved in `#values`.
  record StreamStats, values : Hash(String, Int64) do
    # Returns the statistic named *name*, or `nil` when the broker omitted it.
    def [](name : String) : Int64?
      values[name]?
    end
  end

  # Arguments used when creating a stream with `Environment#create_stream`.
  #
  # Typed options are translated to RabbitMQ stream queue arguments. Entries in
  # *arguments* are preserved, and typed options with the same key take
  # precedence.
  struct StreamOptions
    # Returns the finalized RabbitMQ stream argument map.
    getter arguments : Hash(String, String)

    # Creates stream arguments.
    #
    # *max_length_bytes* limits retained bytes, *max_age* limits message age,
    # *segment_size_bytes* selects the segment file size,
    # *initial_cluster_size* controls initial replicas, *leader_locator* selects
    # the queue leader strategy, and *filter_size_bytes* configures the bloom
    # filter size. Numeric limits must be positive.
    def initialize(
      max_length_bytes : Int64? = nil,
      max_age : Time::Span? = nil,
      segment_size_bytes : Int64? = nil,
      initial_cluster_size : Int32? = nil,
      leader_locator : String? = nil,
      filter_size_bytes : Int32? = nil,
      @arguments : Hash(String, String) = {} of String => String,
    )
      raise ConfigurationError.new("max_length_bytes must be positive") if max_length_bytes && max_length_bytes <= 0
      raise ConfigurationError.new("max_age must be positive") if max_age && max_age <= Time::Span.zero
      raise ConfigurationError.new("segment_size_bytes must be positive") if segment_size_bytes && segment_size_bytes <= 0
      raise ConfigurationError.new("initial_cluster_size must be positive") if initial_cluster_size && initial_cluster_size <= 0
      raise ConfigurationError.new("filter_size_bytes must be positive") if filter_size_bytes && filter_size_bytes <= 0
      @arguments = @arguments.dup
      @arguments["max-length-bytes"] = max_length_bytes.to_s if max_length_bytes
      @arguments["max-age"] = span(max_age) if max_age
      @arguments["stream-max-segment-size-bytes"] = segment_size_bytes.to_s if segment_size_bytes
      @arguments["initial-cluster-size"] = initial_cluster_size.to_s if initial_cluster_size
      @arguments["queue-leader-locator"] = leader_locator if leader_locator
      @arguments["filter-size-bytes"] = filter_size_bytes.to_s if filter_size_bytes
    end

    private def span(value : Time::Span) : String
      milliseconds = value.total_milliseconds.to_i64
      "#{milliseconds}ms"
    end
  end
end
