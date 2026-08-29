module Crabbit::AMQP
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

  struct Decimal
    getter bytes : Bytes

    def initialize(bytes : Bytes, expected_size : Int32)
      raise ArgumentError.new("expected #{expected_size} decimal bytes") unless bytes.size == expected_size
      @bytes = bytes.dup
    end
  end

  struct UUID
    getter bytes : StaticArray(UInt8, 16)

    def initialize(@bytes : StaticArray(UInt8, 16))
    end

    def self.from_slice(bytes : Bytes) : self
      raise ArgumentError.new("UUID requires 16 bytes") unless bytes.size == 16
      value = uninitialized UInt8[16]
      bytes.copy_to(value.to_slice)
      new(value)
    end
  end

  class Described
    getter descriptor : Value
    getter value : Value

    def initialize(@descriptor : Value, @value : Value)
    end

    def_equals_and_hash @descriptor, @value
  end

  class Value
    alias Payload = Nil | Bool | UInt8 | UInt16 | UInt32 | UInt64 |
                    Int8 | Int16 | Int32 | Int64 | Float32 | Float64 |
                    Decimal | Char | Time | UUID | Bytes | String |
                    Array(Value) | Hash(Value, Value) | Described

    getter kind : Kind
    getter payload : Payload

    def initialize(@kind : Kind, @payload : Payload = nil)
      validate!
    end

    def self.null : self
      new(Kind::Null)
    end

    def self.wrap(value : Value) : Value
      value
    end

    def self.wrap(value : Nil) : Value
      null
    end

    def self.wrap(value : Bool) : Value
      new(Kind::Boolean, value)
    end

    def self.wrap(value : UInt8) : Value
      new(Kind::UByte, value)
    end

    def self.wrap(value : UInt16) : Value
      new(Kind::UShort, value)
    end

    def self.wrap(value : UInt32) : Value
      new(Kind::UInt, value)
    end

    def self.wrap(value : UInt64) : Value
      new(Kind::ULong, value)
    end

    def self.wrap(value : Int8) : Value
      new(Kind::Byte, value)
    end

    def self.wrap(value : Int16) : Value
      new(Kind::Short, value)
    end

    def self.wrap(value : Int32) : Value
      new(Kind::Int, value)
    end

    def self.wrap(value : Int64) : Value
      new(Kind::Long, value)
    end

    def self.wrap(value : Float32) : Value
      new(Kind::Float, value)
    end

    def self.wrap(value : Float64) : Value
      new(Kind::Double, value)
    end

    def self.wrap(value : Char) : Value
      new(Kind::Char, value)
    end

    def self.wrap(value : Time) : Value
      new(Kind::Timestamp, value)
    end

    def self.wrap(value : UUID) : Value
      new(Kind::UUID, value)
    end

    def self.wrap(value : Bytes) : Value
      new(Kind::Binary, value.dup)
    end

    def self.wrap(value : String) : Value
      new(Kind::String, value)
    end

    def self.symbol(value : String) : Value
      new(Kind::Symbol, value)
    end

    def self.list(values : Enumerable) : Value
      new(Kind::List, values.map { |value| wrap(value) }.to_a)
    end

    def self.map(values : Hash) : Value
      result = {} of Value => Value
      values.each { |key, value| result[wrap(key)] = wrap(value) }
      new(Kind::Map, result)
    end

    def self.array(values : Enumerable) : Value
      new(Kind::Array, values.map { |value| wrap(value) }.to_a)
    end

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
