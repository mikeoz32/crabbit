# AMQP 1.0 primitive, container, described-value, and message codecs.
#
# Most applications work with `Crabbit::Message` and only use `Value` for
# annotations, application properties, Sequence bodies, or Value bodies.
module Crabbit::AMQP
  # Runtime type represented by an `AMQP::Value`.
  enum Kind
    Null
    Boolean
    UByte
    UShort
    UInt
    ULong
    Byte
    Short
    Int
    Long
    Float
    Double
    Decimal32
    Decimal64
    Decimal128
    Char
    Timestamp
    UUID
    Binary
    String
    Symbol
    List
    Map
    Array
    Described
  end

  # Opaque IEEE 754 decimal interchange bytes.
  #
  # AMQP decimal32, decimal64, and decimal128 values contain 4, 8, and 16 bytes
  # respectively. Crabbit preserves their wire representation without numeric
  # conversion.
  struct Decimal
    # Returns the decimal interchange bytes owned by this value.
    getter bytes : Bytes

    # Creates an opaque decimal value and validates its exact *expected_size*.
    def initialize(bytes : Bytes, expected_size : Int32)
      raise ArgumentError.new("expected #{expected_size} decimal bytes") unless bytes.size == expected_size
      @bytes = bytes.dup
    end
  end

  # AMQP UUID value stored as 16 network-order bytes.
  struct UUID
    # Returns the 16 UUID bytes.
    getter bytes : StaticArray(UInt8, 16)

    # Creates a UUID from an exact static byte array.
    def initialize(@bytes : StaticArray(UInt8, 16))
    end

    # Copies exactly 16 bytes from *bytes* into a UUID.
    def self.from_slice(bytes : Bytes) : self
      raise ArgumentError.new("UUID requires 16 bytes") unless bytes.size == 16
      value = uninitialized UInt8[16]
      bytes.copy_to(value.to_slice)
      new(value)
    end
  end

  # AMQP described value consisting of a descriptor and its value.
  class Described
    # Returns the descriptor value, commonly an unsigned numeric or symbol.
    getter descriptor : Value
    # Returns the described value.
    getter value : Value

    # Creates a described value.
    def initialize(@descriptor : Value, @value : Value)
    end

    def_equals_and_hash @descriptor, @value
  end

  # Dynamically typed AMQP 1.0 value.
  #
  # `.wrap` maps Crystal primitive values to their canonical AMQP kinds.
  # Explicit constructors provide Symbol, List, Map, Array, and Described
  # values. The constructor validates that *payload* matches *kind*.
  #
  # ```
  # value = Crabbit::AMQP::Value.map({
  #   "attempt" => 3_u32,
  #   "ready"   => true,
  # })
  # encoded = Crabbit::AMQP::Encoder.encode(value)
  # decoded = Crabbit::AMQP::Decoder.new(encoded).read
  # ```
  class Value
    # Union of Crystal payload types represented by AMQP values.
    alias Payload = Nil | Bool | UInt8 | UInt16 | UInt32 | UInt64 |
                    Int8 | Int16 | Int32 | Int64 | Float32 | Float64 |
                    Decimal | Char | Time | UUID | Bytes | String |
                    Array(Value) | Hash(Value, Value) | Described

    # Returns the AMQP type kind.
    getter kind : Kind
    # Returns the typed Crystal payload.
    getter payload : Payload

    # Creates and validates a value for *kind* and *payload*.
    def initialize(@kind : Kind, @payload : Payload = nil)
      validate!
    end

    # Returns the AMQP null value.
    def self.null : self
      new(Kind::Null)
    end

    # Returns *value* unchanged, or wraps a supported Crystal primitive in its
    # canonical AMQP kind.
    def self.wrap(value : Value) : Value
      value
    end

    # :ditto:
    def self.wrap(value : Nil) : Value
      null
    end

    # :ditto:
    def self.wrap(value : Bool) : Value
      new(Kind::Boolean, value)
    end

    # :ditto:
    def self.wrap(value : UInt8) : Value
      new(Kind::UByte, value)
    end

    # :ditto:
    def self.wrap(value : UInt16) : Value
      new(Kind::UShort, value)
    end

    # :ditto:
    def self.wrap(value : UInt32) : Value
      new(Kind::UInt, value)
    end

    # :ditto:
    def self.wrap(value : UInt64) : Value
      new(Kind::ULong, value)
    end

    # :ditto:
    def self.wrap(value : Int8) : Value
      new(Kind::Byte, value)
    end

    # :ditto:
    def self.wrap(value : Int16) : Value
      new(Kind::Short, value)
    end

    # :ditto:
    def self.wrap(value : Int32) : Value
      new(Kind::Int, value)
    end

    # :ditto:
    def self.wrap(value : Int64) : Value
      new(Kind::Long, value)
    end

    # :ditto:
    def self.wrap(value : Float32) : Value
      new(Kind::Float, value)
    end

    # :ditto:
    def self.wrap(value : Float64) : Value
      new(Kind::Double, value)
    end

    # :ditto:
    def self.wrap(value : Char) : Value
      new(Kind::Char, value)
    end

    # :ditto:
    def self.wrap(value : Time) : Value
      new(Kind::Timestamp, value)
    end

    # :ditto:
    def self.wrap(value : UUID) : Value
      new(Kind::UUID, value)
    end

    # :ditto:
    #
    # Binary input is copied.
    def self.wrap(value : Bytes) : Value
      new(Kind::Binary, value.dup)
    end

    # :ditto:
    def self.wrap(value : String) : Value
      new(Kind::String, value)
    end

    # Returns an AMQP symbol after validating it during encoding as ASCII.
    def self.symbol(value : String) : Value
      new(Kind::Symbol, value)
    end

    # Wraps each item in *values* and returns an AMQP list.
    def self.list(values : Enumerable) : Value
      new(Kind::List, values.map { |value| wrap(value) }.to_a)
    end

    # Wraps every key and value and returns an AMQP map.
    def self.map(values : Hash) : Value
      result = {} of Value => Value
      values.each { |key, value| result[wrap(key)] = wrap(value) }
      new(Kind::Map, result)
    end

    # Wraps each item and returns a homogeneous AMQP array.
    #
    # The encoder rejects empty arrays and arrays whose values use different
    # AMQP constructors.
    def self.array(values : Enumerable) : Value
      new(Kind::Array, values.map { |value| wrap(value) }.to_a)
    end

    # Wraps *descriptor* and *value* and returns an AMQP described value.
    def self.described(descriptor, value) : Value
      new(Kind::Described, Described.new(wrap(descriptor), wrap(value)))
    end

    def_equals_and_hash @kind, @payload

    private def validate! : Nil
      valid = case kind
              when .null?             then payload.nil?
              when .boolean?          then payload.is_a?(Bool)
              when .u_byte?           then payload.is_a?(UInt8)
              when .u_short?          then payload.is_a?(UInt16)
              when .u_int?            then payload.is_a?(UInt32)
              when .u_long?           then payload.is_a?(UInt64)
              when .byte?             then payload.is_a?(Int8)
              when .short?            then payload.is_a?(Int16)
              when .int?              then payload.is_a?(Int32)
              when .long?             then payload.is_a?(Int64)
              when .float?            then payload.is_a?(Float32)
              when .double?           then payload.is_a?(Float64)
              when .decimal32?        then payload.is_a?(Decimal) && payload.as(Decimal).bytes.size == 4
              when .decimal64?        then payload.is_a?(Decimal) && payload.as(Decimal).bytes.size == 8
              when .decimal128?       then payload.is_a?(Decimal) && payload.as(Decimal).bytes.size == 16
              when .char?             then payload.is_a?(Char)
              when .timestamp?        then payload.is_a?(Time)
              when .uuid?             then payload.is_a?(UUID)
              when .binary?           then payload.is_a?(Bytes)
              when .string?, .symbol? then payload.is_a?(String)
              when .list?, .array?    then payload.is_a?(Array(Value))
              when .map?              then payload.is_a?(Hash(Value, Value))
              when .described?        then payload.is_a?(Described)
              else                         false
              end
      raise ArgumentError.new("payload #{payload.class} is invalid for AMQP #{kind}") unless valid
    end
  end
end
