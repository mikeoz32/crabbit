module Crabbit
  enum OffsetType : UInt16
    None      = 0_u16
    First     = 1_u16
    Last      = 2_u16
    Next      = 3_u16
    Offset    = 4_u16
    Timestamp = 5_u16
  end

  struct OffsetSpecification
    getter type : OffsetType
    getter value : Int64 | UInt64 | Nil

    private def initialize(@type : OffsetType, @value : Int64 | UInt64 | Nil = nil)
    end

    def self.first : self
      new(OffsetType::First)
    end

    def self.last : self
      new(OffsetType::Last)
    end

    def self.next : self
      new(OffsetType::Next)
    end

    def self.offset(value : Int) : self
      raise ArgumentError.new("offset must not be negative") if value < 0
      new(OffsetType::Offset, value.to_u64)
    end

    def self.timestamp(value : Time) : self
      new(OffsetType::Timestamp, value.to_unix_ms)
    end

    def self.timestamp(milliseconds : Int) : self
      new(OffsetType::Timestamp, milliseconds.to_i64)
    end

    def self.none : self
      new(OffsetType::None)
    end
  end

  record Endpoint, host : String, port : Int32 = 5552, tls : Bool = false do
    def self.tls(host : String, port : Int32 = 5551) : self
      new(host, port, true)
    end
  end

  record Broker, reference : UInt16, host : String, port : Int32 do
    def endpoint(tls : Bool = false) : Endpoint
      Endpoint.new(host, port, tls)
    end
  end

  record StreamMetadata,
    stream : String,
    response_code : ResponseCode,
    leader : Broker?,
    replicas : Array(Broker)

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

  record StreamStats, values : Hash(String, Int64) do
    def [](name : String) : Int64?
      values[name]?
    end
  end

  struct StreamOptions
    getter arguments : Hash(String, String)

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
