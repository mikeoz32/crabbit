module Crabbit::AMQP
  module TypeCode
    Described  = 0x00_u8
    Null       = 0x40_u8
    BoolTrue   = 0x41_u8
    BoolFalse  = 0x42_u8
    UInt0      = 0x43_u8
    ULong0     = 0x44_u8
    List0      = 0x45_u8
    UByte      = 0x50_u8
    Byte       = 0x51_u8
    SmallUInt  = 0x52_u8
    SmallULong = 0x53_u8
    SmallInt   = 0x54_u8
    SmallLong  = 0x55_u8
    Boolean    = 0x56_u8
    UShort     = 0x60_u8
    Short      = 0x61_u8
    UInt       = 0x70_u8
    Int        = 0x71_u8
    Float      = 0x72_u8
    Char       = 0x73_u8
    Decimal32  = 0x74_u8
    ULong      = 0x80_u8
    Long       = 0x81_u8
    Double     = 0x82_u8
    Timestamp  = 0x83_u8
    Decimal64  = 0x84_u8
    Decimal128 = 0x94_u8
    UUID       = 0x98_u8
    Binary8    = 0xa0_u8
    String8    = 0xa1_u8
    Symbol8    = 0xa3_u8
    Binary32   = 0xb0_u8
    String32   = 0xb1_u8
    Symbol32   = 0xb3_u8
    List8      = 0xc0_u8
    Map8       = 0xc1_u8
    List32     = 0xd0_u8
    Map32      = 0xd1_u8
    Array8     = 0xe0_u8
    Array32    = 0xf0_u8
  end

  struct Encoder
    getter io : IO

    def initialize(@io : IO = IO::Memory.new)
    end

    def self.encode(value : Value) : Bytes
      io = IO::Memory.new
      encode(value, io)
      io.to_slice
    end

    def self.encode(value : Value, io : IO) : Nil
      new(io).write(value)
    end

    def write(value : Value) : self
      case value.kind
      when .null?
        u8(TypeCode::Null)
      when .boolean?
        u8(value.payload.as(Bool) ? TypeCode::BoolTrue : TypeCode::BoolFalse)
      when .u_byte?
        u8(TypeCode::UByte); u8(value.payload.as(UInt8))
      when .u_short?
        u8(TypeCode::UShort); u16(value.payload.as(UInt16))
      when .u_int?
        write_uint(value.payload.as(UInt32))
      when .u_long?
        write_ulong(value.payload.as(UInt64))
      when .byte?
        u8(TypeCode::Byte); i8(value.payload.as(Int8))
      when .short?
        u8(TypeCode::Short); i16(value.payload.as(Int16))
      when .int?
        write_int(value.payload.as(Int32))
      when .long?
        write_long(value.payload.as(Int64))
      when .float?
        u8(TypeCode::Float); f32(value.payload.as(Float32))
      when .double?
        u8(TypeCode::Double); f64(value.payload.as(Float64))
      when .decimal32?
        u8(TypeCode::Decimal32); raw(value.payload.as(Decimal).bytes)
      when .decimal64?
        u8(TypeCode::Decimal64); raw(value.payload.as(Decimal).bytes)
      when .decimal128?
        u8(TypeCode::Decimal128); raw(value.payload.as(Decimal).bytes)
      when .char?
        u8(TypeCode::Char); u32(value.payload.as(Char).ord.to_u32)
      when .timestamp?
        u8(TypeCode::Timestamp); i64(value.payload.as(Time).to_unix_ms)
      when .uuid?
        u8(TypeCode::UUID); raw(value.payload.as(UUID).bytes.to_slice)
      when .binary?
        write_binary(value.payload.as(Bytes))
      when .string?
        write_string(value.payload.as(String), symbol: false)
      when .symbol?
        write_string(value.payload.as(String), symbol: true)
      when .list?
        write_list(value.payload.as(Array(Value)))
      when .map?
        write_map(value.payload.as(Hash(Value, Value)))
      when .array?
        write_array(value.payload.as(Array(Value)))
      when .described?
        described = value.payload.as(Described)
        u8(TypeCode::Described)
        write(described.descriptor)
        write(described.value)
      end
      self
    end

    def write_described(descriptor : UInt64, value : Value) : self
      write_descriptor(descriptor)
      write(value)
      self
    end

    def write_described_binary(descriptor : UInt64, value : Bytes) : self
      write_descriptor(descriptor)
      write_binary(value)
      self
    end

    def write_described_list(descriptor : UInt64, values : Array(Value)) : self
      write_descriptor(descriptor)
      write_list(values)
      self
    end

    def write_described_map(descriptor : UInt64, values : Hash(Value, Value)) : self
      write_descriptor(descriptor)
      write_map(values)
      self
    end

    def write_described_string_map(descriptor : UInt64, values : Hash(String, Value)) : self
      write_descriptor(descriptor)
      write_string_map(values)
      self
    end

    def to_slice : Bytes
      memory = io.as?(IO::Memory)
      raise CodecError.new("encoder output is not an IO::Memory") unless memory
      memory.to_slice.dup
    end

    private def write_descriptor(descriptor : UInt64) : Nil
      u8(TypeCode::Described)
      write_ulong(descriptor)
    end

    private def write_uint(value : UInt32) : Nil
      if value == 0
        u8(TypeCode::UInt0)
      elsif value <= UInt8::MAX
        u8(TypeCode::SmallUInt); u8(value.to_u8)
      else
        u8(TypeCode::UInt); u32(value)
      end
    end

    private def write_ulong(value : UInt64) : Nil
      if value == 0
        u8(TypeCode::ULong0)
      elsif value <= UInt8::MAX
        u8(TypeCode::SmallULong); u8(value.to_u8)
      else
        u8(TypeCode::ULong); u64(value)
      end
    end

    private def write_int(value : Int32) : Nil
      if Int8::MIN <= value <= Int8::MAX
        u8(TypeCode::SmallInt); i8(value.to_i8)
      else
        u8(TypeCode::Int); i32(value)
      end
    end

    private def write_long(value : Int64) : Nil
      if Int8::MIN <= value <= Int8::MAX
        u8(TypeCode::SmallLong); i8(value.to_i8)
      else
        u8(TypeCode::Long); i64(value)
      end
    end

    private def write_binary(value : Bytes) : Nil
      if value.size <= UInt8::MAX
        u8(TypeCode::Binary8); u8(value.size.to_u8)
      else
        u8(TypeCode::Binary32); u32(value.size.to_u32)
      end
      raw(value)
    end

    private def write_string(value : String, symbol : Bool) : Nil
      bytes = symbol ? validated_symbol_bytes(value) : value.to_slice
      if bytes.size <= UInt8::MAX
        u8(symbol ? TypeCode::Symbol8 : TypeCode::String8)
        u8(bytes.size.to_u8)
      else
        u8(symbol ? TypeCode::Symbol32 : TypeCode::String32)
        u32(bytes.size.to_u32)
      end
      raw(bytes)
    end

    private def validated_symbol_bytes(value : String) : Bytes
      bytes = value.to_slice
      if bytes.any? { |byte| byte > 0x7f }
        raise CodecError.new("AMQP symbol must contain only ASCII characters")
      end
      bytes
    end

    private def write_list(values : Array(Value), force32 : Bool = false) : Nil
      if values.empty? && !force32
        u8(TypeCode::List0)
        return
      end
      payload_size = list_payload_size(values)
      size8 = 1_i64 + payload_size
      if !force32 && values.size <= UInt8::MAX && size8 <= UInt8::MAX
        u8(TypeCode::List8); u8(size8.to_u8); u8(values.size.to_u8)
      else
        u8(TypeCode::List32); u32((4_i64 + payload_size).to_u32); u32(values.size.to_u32)
      end
      values.each { |value| write(value) }
    end

    private def write_map(values : Hash(Value, Value), force32 : Bool = false) : Nil
      payload_size = map_payload_size(values)
      count = values.size * 2
      size8 = 1_i64 + payload_size
      if !force32 && count <= UInt8::MAX && size8 <= UInt8::MAX
        u8(TypeCode::Map8); u8(size8.to_u8); u8(count.to_u8)
      else
        u8(TypeCode::Map32); u32((4_i64 + payload_size).to_u32); u32(count.to_u32)
      end
      values.each { |key, value| write(key).write(value) }
    end

    private def write_string_map(values : Hash(String, Value), force32 : Bool = false) : Nil
      payload_size = string_map_payload_size(values)
      count = values.size * 2
      size8 = 1_i64 + payload_size
      if !force32 && count <= UInt8::MAX && size8 <= UInt8::MAX
        u8(TypeCode::Map8); u8(size8.to_u8); u8(count.to_u8)
      else
        u8(TypeCode::Map32); u32((4_i64 + payload_size).to_u32); u32(count.to_u32)
      end
      values.each do |key, value|
        write_string(key, symbol: false)
        write(value)
      end
    end

    private def write_array(values : Array(Value), force32 : Bool = false) : Nil
      raise CodecError.new("AMQP arrays cannot be empty") if values.empty?
      constructor = array_constructor(values)
      constructor_size = array_constructor_size(constructor)
      payload_size = array_payload_size(values, constructor[0])
      size8 = 1_i64 + constructor_size + payload_size
      if !force32 && values.size <= UInt8::MAX && size8 <= UInt8::MAX
        u8(TypeCode::Array8); u8(size8.to_u8); u8(values.size.to_u8)
      else
        u8(TypeCode::Array32)
        u32((4_i64 + constructor_size + payload_size).to_u32)
        u32(values.size.to_u32)
      end
      write_array_constructor(constructor)
      values.each { |value| write_array_payload(value, constructor[0]) }
    end

    private def array_constructor(values : Array(Value)) : Tuple(UInt8, Value?)
      kind = values.first.kind
      unless values.all? { |value| value.kind == kind }
        raise CodecError.new("AMQP arrays must contain values of a single type")
      end
      case kind
      when .null?
        raise CodecError.new("AMQP arrays cannot contain null values")
      when .described?
        described = values.first.payload.as(Described)
        unless values.all? do |value|
                 current = value.payload.as(Described)
                 current.descriptor == described.descriptor && current.value.kind == described.value.kind
               end
          raise CodecError.new("described AMQP arrays require a common descriptor and value type")
        end
        {constructor_for_kind(described.value.kind), described.descriptor}
      else
        {constructor_for_kind(kind), nil}
      end
    end

    private def constructor_for_kind(kind : Kind) : UInt8
      case kind
      when .boolean?    then TypeCode::Boolean
      when .u_byte?     then TypeCode::UByte
      when .u_short?    then TypeCode::UShort
      when .u_int?      then TypeCode::UInt
      when .u_long?     then TypeCode::ULong
      when .byte?       then TypeCode::Byte
      when .short?      then TypeCode::Short
      when .int?        then TypeCode::Int
      when .long?       then TypeCode::Long
      when .float?      then TypeCode::Float
      when .double?     then TypeCode::Double
      when .decimal32?  then TypeCode::Decimal32
      when .decimal64?  then TypeCode::Decimal64
      when .decimal128? then TypeCode::Decimal128
      when .char?       then TypeCode::Char
      when .timestamp?  then TypeCode::Timestamp
      when .uuid?       then TypeCode::UUID
      when .binary?     then TypeCode::Binary32
      when .string?     then TypeCode::String32
      when .symbol?     then TypeCode::Symbol32
      when .list?       then TypeCode::List32
      when .map?        then TypeCode::Map32
      when .array?      then TypeCode::Array32
      else
        raise CodecError.new("unsupported described array value type #{kind}")
      end
    end

    private def write_array_constructor(constructor : Tuple(UInt8, Value?)) : Nil
      if descriptor = constructor[1]
        u8(TypeCode::Described)
        write(descriptor)
      end
      u8(constructor[0])
    end

    protected def write_array_payload(value : Value, code : UInt8) : Nil
      actual = value.kind.described? ? value.payload.as(Described).value : value
      case code
      when TypeCode::Boolean then u8(actual.payload.as(Bool) ? 1_u8 : 0_u8)
      when TypeCode::UByte   then u8(actual.payload.as(UInt8))
      when TypeCode::UShort  then u16(actual.payload.as(UInt16))
      when TypeCode::UInt    then u32(actual.payload.as(UInt32))
      when TypeCode::ULong   then u64(actual.payload.as(UInt64))
      when TypeCode::Byte    then i8(actual.payload.as(Int8))
      when TypeCode::Short   then i16(actual.payload.as(Int16))
      when TypeCode::Int     then i32(actual.payload.as(Int32))
      when TypeCode::Long    then i64(actual.payload.as(Int64))
      when TypeCode::Float   then f32(actual.payload.as(Float32))
      when TypeCode::Double  then f64(actual.payload.as(Float64))
      when TypeCode::Decimal32, TypeCode::Decimal64, TypeCode::Decimal128
        raw(actual.payload.as(Decimal).bytes)
      when TypeCode::Char      then u32(actual.payload.as(Char).ord.to_u32)
      when TypeCode::Timestamp then i64(actual.payload.as(Time).to_unix_ms)
      when TypeCode::UUID      then raw(actual.payload.as(UUID).bytes.to_slice)
      when TypeCode::Binary32
        bytes = actual.payload.as(Bytes); u32(bytes.size.to_u32); raw(bytes)
      when TypeCode::String32
        bytes = actual.payload.as(String).to_slice; u32(bytes.size.to_u32); raw(bytes)
      when TypeCode::Symbol32
        bytes = validated_symbol_bytes(actual.payload.as(String))
        u32(bytes.size.to_u32); raw(bytes)
      when TypeCode::List32
        write_list_payload32(actual.payload.as(Array(Value)))
      when TypeCode::Map32
        write_map_payload32(actual.payload.as(Hash(Value, Value)))
      when TypeCode::Array32
        write_array_payload32(actual.payload.as(Array(Value)))
      else
        raise CodecError.new("unsupported AMQP array constructor 0x#{code.to_s(16)}")
      end
    end

    private def write_list_payload32(values : Array(Value)) : Nil
      payload_size = list_payload_size(values)
      u32((4_i64 + payload_size).to_u32)
      u32(values.size.to_u32)
      values.each { |value| write(value) }
    end

    private def write_map_payload32(values : Hash(Value, Value)) : Nil
      payload_size = map_payload_size(values)
      u32((4_i64 + payload_size).to_u32)
      u32((values.size * 2).to_u32)
      values.each { |key, value| write(key).write(value) }
    end

    private def write_array_payload32(values : Array(Value)) : Nil
      raise CodecError.new("nested AMQP arrays cannot be empty") if values.empty?
      constructor = array_constructor(values)
      constructor_size = array_constructor_size(constructor)
      payload_size = array_payload_size(values, constructor[0])
      u32((4_i64 + constructor_size + payload_size).to_u32)
      u32(values.size.to_u32)
      write_array_constructor(constructor)
      values.each { |value| write_array_payload(value, constructor[0]) }
    end

    private def encoded_size(value : Value) : Int64
      case value.kind
      when .null?, .boolean?
        1_i64
      when .u_byte?, .byte?
        2_i64
      when .u_short?, .short?
        3_i64
      when .u_int?
        number = value.payload.as(UInt32)
        number == 0 ? 1_i64 : (number <= UInt8::MAX ? 2_i64 : 5_i64)
      when .u_long?
        number = value.payload.as(UInt64)
        number == 0 ? 1_i64 : (number <= UInt8::MAX ? 2_i64 : 9_i64)
      when .int?
        number = value.payload.as(Int32)
        Int8::MIN <= number <= Int8::MAX ? 2_i64 : 5_i64
      when .long?
        number = value.payload.as(Int64)
        Int8::MIN <= number <= Int8::MAX ? 2_i64 : 9_i64
      when .float?, .char?, .decimal32?
        5_i64
      when .double?, .timestamp?, .decimal64?
        9_i64
      when .decimal128?, .uuid?
        17_i64
      when .binary?
        variable_size(value.payload.as(Bytes).size)
      when .string?
        variable_size(value.payload.as(String).bytesize)
      when .symbol?
        variable_size(validated_symbol_bytes(value.payload.as(String)).size)
      when .list?
        list_encoded_size(value.payload.as(Array(Value)))
      when .map?
        map_encoded_size(value.payload.as(Hash(Value, Value)))
      when .array?
        array_encoded_size(value.payload.as(Array(Value)))
      when .described?
        described = value.payload.as(Described)
        1_i64 + encoded_size(described.descriptor) + encoded_size(described.value)
      else
        raise CodecError.new("unsupported AMQP value type #{value.kind}")
      end
    end

    private def variable_size(size : Int) : Int64
      size <= UInt8::MAX ? 2_i64 + size : 5_i64 + size
    end

    private def list_payload_size(values : Array(Value)) : Int64
      size = 0_i64
      values.each { |value| size += encoded_size(value) }
      size
    end

    private def map_payload_size(values : Hash(Value, Value)) : Int64
      size = 0_i64
      values.each do |key, value|
        size += encoded_size(key)
        size += encoded_size(value)
      end
      size
    end

    private def string_map_payload_size(values : Hash(String, Value)) : Int64
      size = 0_i64
      values.each do |key, value|
        size += variable_size(key.bytesize)
        size += encoded_size(value)
      end
      size
    end

    private def list_encoded_size(values : Array(Value)) : Int64
      return 1_i64 if values.empty?
      payload_size = list_payload_size(values)
      size8 = 1_i64 + payload_size
      values.size <= UInt8::MAX && size8 <= UInt8::MAX ? 2_i64 + size8 : 9_i64 + payload_size
    end

    private def map_encoded_size(values : Hash(Value, Value)) : Int64
      payload_size = map_payload_size(values)
      count = values.size * 2
      size8 = 1_i64 + payload_size
      count <= UInt8::MAX && size8 <= UInt8::MAX ? 2_i64 + size8 : 9_i64 + payload_size
    end

    private def array_encoded_size(values : Array(Value)) : Int64
      raise CodecError.new("AMQP arrays cannot be empty") if values.empty?
      constructor = array_constructor(values)
      constructor_size = array_constructor_size(constructor)
      payload_size = array_payload_size(values, constructor[0])
      size8 = 1_i64 + constructor_size + payload_size
      values.size <= UInt8::MAX && size8 <= UInt8::MAX ? 2_i64 + size8 : 9_i64 + constructor_size + payload_size
    end

    private def array_constructor_size(constructor : Tuple(UInt8, Value?)) : Int64
      descriptor = constructor[1]
      descriptor ? 2_i64 + encoded_size(descriptor) : 1_i64
    end

    private def array_payload_size(values : Array(Value), code : UInt8) : Int64
      size = 0_i64
      values.each { |value| size += array_element_size(value, code) }
      size
    end

    private def array_element_size(value : Value, code : UInt8) : Int64
      actual = value.kind.described? ? value.payload.as(Described).value : value
      case code
      when TypeCode::Boolean, TypeCode::UByte, TypeCode::Byte
        1_i64
      when TypeCode::UShort, TypeCode::Short
        2_i64
      when TypeCode::UInt, TypeCode::Int, TypeCode::Float, TypeCode::Char, TypeCode::Decimal32
        4_i64
      when TypeCode::ULong, TypeCode::Long, TypeCode::Double, TypeCode::Timestamp, TypeCode::Decimal64
        8_i64
      when TypeCode::Decimal128, TypeCode::UUID
        16_i64
      when TypeCode::Binary32
        4_i64 + actual.payload.as(Bytes).size
      when TypeCode::String32
        4_i64 + actual.payload.as(String).bytesize
      when TypeCode::Symbol32
        4_i64 + validated_symbol_bytes(actual.payload.as(String)).size
      when TypeCode::List32
        8_i64 + list_payload_size(actual.payload.as(Array(Value)))
      when TypeCode::Map32
        8_i64 + map_payload_size(actual.payload.as(Hash(Value, Value)))
      when TypeCode::Array32
        nested = actual.payload.as(Array(Value))
        raise CodecError.new("nested AMQP arrays cannot be empty") if nested.empty?
        constructor = array_constructor(nested)
        8_i64 + array_constructor_size(constructor) + array_payload_size(nested, constructor[0])
      else
        raise CodecError.new("unsupported AMQP array constructor 0x#{code.to_s(16)}")
      end
    end

    protected def raw(value : Bytes) : Nil
      io.write(value)
    end

    {% for bits in [16, 32, 64] %}
      protected def u{{bits}}(value : UInt{{bits}}) : Nil
        io.write_bytes(value, IO::ByteFormat::BigEndian)
      end

      protected def i{{bits}}(value : Int{{bits}}) : Nil
        io.write_bytes(value, IO::ByteFormat::BigEndian)
      end
    {% end %}

    protected def u8(value : UInt8) : Nil
      io.write_byte(value)
    end

    protected def i8(value : Int8) : Nil
      io.write_byte(value.to_u8!)
    end

    protected def f32(value : Float32) : Nil
      io.write_bytes(value, IO::ByteFormat::BigEndian)
    end

    protected def f64(value : Float64) : Nil
      io.write_bytes(value, IO::ByteFormat::BigEndian)
    end
  end

  struct Decoder
    MAX_CONTAINER_ELEMENTS = 1_000_000

    getter position : Int32
    getter bytes : Bytes

    def initialize(@bytes : Bytes, @position : Int32 = 0, *, @zero_copy : Bool = false)
    end

    def eof? : Bool
      position == bytes.size
    end

    def remaining : Int32
      bytes.size - position
    end

    def read : Value
      read_with_code(u8)
    end

    def peek_described_descriptor : UInt64?
      saved_position = position
      return nil unless u8 == TypeCode::Described
      numeric_descriptor_with_code(u8)
    ensure
      @position = saved_position.not_nil!
    end

    def consume_described_descriptor(expected : UInt64) : Nil
      raise CodecError.new("expected AMQP described section") unless u8 == TypeCode::Described
      actual = numeric_descriptor_with_code(u8)
      raise CodecError.new("AMQP section descriptor must be numeric") unless actual
      unless actual == expected
        raise CodecError.new("expected AMQP section descriptor #{expected}, got #{actual}")
      end
    end

    def read_binary : Bytes
      case code = u8
      when TypeCode::Binary8
        binary(u8.to_i32)
      when TypeCode::Binary32
        binary(length32)
      else
        raise CodecError.new("expected AMQP binary, got type code 0x#{code.to_s(16)}")
      end
    rescue ex : OverflowError
      raise CodecError.new("AMQP length exceeds supported memory size: #{ex.message}")
    end

    def read_list_values : Array(Value)
      case code = u8
      when TypeCode::List0
        [] of Value
      when TypeCode::List8
        read_list_values(u8.to_i32, count_width: 1)
      when TypeCode::List32
        read_list_values(length32, count_width: 4)
      else
        raise CodecError.new("expected AMQP list, got type code 0x#{code.to_s(16)}")
      end
    rescue ex : OverflowError
      raise CodecError.new("AMQP length exceeds supported memory size: #{ex.message}")
    end

    def read_map_values : Hash(Value, Value)
      case code = u8
      when TypeCode::Map8
        read_map_values(u8.to_i32, count_width: 1)
      when TypeCode::Map32
        read_map_values(length32, count_width: 4)
      else
        raise CodecError.new("expected AMQP map, got type code 0x#{code.to_s(16)}")
      end
    rescue ex : OverflowError
      raise CodecError.new("AMQP length exceeds supported memory size: #{ex.message}")
    end

    protected def read_with_code(code : UInt8) : Value
      case code
      when TypeCode::Described
        Value.described(read, read)
      when TypeCode::Null
        Value.null
      when TypeCode::BoolTrue
        Value.wrap(true)
      when TypeCode::BoolFalse
        Value.wrap(false)
      when TypeCode::Boolean
        Value.wrap(u8 != 0)
      when TypeCode::UByte
        Value.wrap(u8)
      when TypeCode::UShort
        Value.wrap(u16)
      when TypeCode::UInt0
        Value.wrap(0_u32)
      when TypeCode::SmallUInt
        Value.wrap(u8.to_u32)
      when TypeCode::UInt
        Value.wrap(u32)
      when TypeCode::ULong0
        Value.wrap(0_u64)
      when TypeCode::SmallULong
        Value.wrap(u8.to_u64)
      when TypeCode::ULong
        Value.wrap(u64)
      when TypeCode::Byte
        Value.wrap(i8)
      when TypeCode::Short
        Value.wrap(i16)
      when TypeCode::SmallInt
        Value.wrap(i8.to_i32)
      when TypeCode::Int
        Value.wrap(i32)
      when TypeCode::SmallLong
        Value.wrap(i8.to_i64)
      when TypeCode::Long
        Value.wrap(i64)
      when TypeCode::Float
        Value.wrap(f32)
      when TypeCode::Double
        Value.wrap(f64)
      when TypeCode::Decimal32
        Value.new(Kind::Decimal32, Decimal.new(raw(4), 4))
      when TypeCode::Decimal64
        Value.new(Kind::Decimal64, Decimal.new(raw(8), 8))
      when TypeCode::Decimal128
        Value.new(Kind::Decimal128, Decimal.new(raw(16), 16))
      when TypeCode::Char
        Value.wrap(u32.chr)
      when TypeCode::Timestamp
        Value.wrap(Time.unix_ms(i64))
      when TypeCode::UUID
        Value.wrap(UUID.from_slice(raw(16)))
      when TypeCode::Binary8
        Value.new(Kind::Binary, binary(u8.to_i32))
      when TypeCode::Binary32
        Value.new(Kind::Binary, binary(length32))
      when TypeCode::String8
        Value.wrap(string(u8.to_i32))
      when TypeCode::String32
        Value.wrap(string(length32))
      when TypeCode::Symbol8
        Value.symbol(symbol(u8.to_i32))
      when TypeCode::Symbol32
        Value.symbol(symbol(length32))
      when TypeCode::List0
        Value.new(Kind::List, [] of Value)
      when TypeCode::List8
        read_list(u8.to_i32, count_width: 1)
      when TypeCode::List32
        read_list(length32, count_width: 4)
      when TypeCode::Map8
        read_map(u8.to_i32, count_width: 1)
      when TypeCode::Map32
        read_map(length32, count_width: 4)
      when TypeCode::Array8
        read_array(u8.to_i32, count_width: 1)
      when TypeCode::Array32
        read_array(length32, count_width: 4)
      else
        raise CodecError.new("unknown AMQP type code 0x#{code.to_s(16)} at byte #{position - 1}")
      end
    rescue ex : OverflowError
      raise CodecError.new("AMQP length exceeds supported memory size: #{ex.message}")
    end

    private def read_list(size : Int32, count_width : Int32) : Value
      Value.new(Kind::List, read_list_values(size, count_width))
    end

    private def read_list_values(size : Int32, count_width : Int32) : Array(Value)
      boundary = container_boundary(size)
      count = count_width == 1 ? u8.to_i32 : length32
      validate_container_count!(count)
      values = Array(Value).new(count) { read }
      finish_container!(boundary)
      values
    end

    private def read_map(size : Int32, count_width : Int32) : Value
      Value.new(Kind::Map, read_map_values(size, count_width))
    end

    private def read_map_values(size : Int32, count_width : Int32) : Hash(Value, Value)
      boundary = container_boundary(size)
      count = count_width == 1 ? u8.to_i32 : length32
      validate_container_count!(count)
      raise CodecError.new("AMQP map element count must be even") unless count.even?
      values = {} of Value => Value
      (count // 2).times { values[read] = read }
      finish_container!(boundary)
      values
    end

    private def read_array(size : Int32, count_width : Int32) : Value
      boundary = container_boundary(size)
      count = count_width == 1 ? u8.to_i32 : length32
      validate_container_count!(count)
      raise CodecError.new("AMQP array is missing its element constructor") if count > 0 && position >= boundary
      if count == 0
        finish_container!(boundary)
        raise CodecError.new("AMQP arrays cannot be empty")
      end
      code = u8
      if code == TypeCode::Described
        descriptor = read
        value_code = u8
        values = Array(Value).new(count) { Value.described(descriptor, read_with_code(value_code)) }
        finish_container!(boundary)
        Value.new(Kind::Array, values)
      else
        values = Array(Value).new(count) { read_with_code(code) }
        finish_container!(boundary)
        Value.new(Kind::Array, values)
      end
    end

    private def container_boundary(size : Int32) : Int32
      raise CodecError.new("invalid AMQP container size #{size}") if size < 1
      boundary = position + size
      raise CodecError.new("AMQP container exceeds input") if boundary > bytes.size
      boundary
    end

    private def validate_container_count!(count : Int32) : Nil
      if count > MAX_CONTAINER_ELEMENTS
        raise CodecError.new(
          "AMQP container count #{count} exceeds #{MAX_CONTAINER_ELEMENTS}",
        )
      end
    end

    private def finish_container!(boundary : Int32) : Nil
      raise CodecError.new("AMQP container size mismatch: ended at #{position}, expected #{boundary}") unless position == boundary
    end

    private def string(size : Int32) : String
      String.new(raw(size))
    rescue ex : ArgumentError
      raise CodecError.new("invalid AMQP UTF-8 string: #{ex.message}")
    end

    private def symbol(size : Int32) : String
      value = string(size)
      raise CodecError.new("AMQP symbol contains non-ASCII bytes") unless value.ascii_only?
      value
    end

    private def length32 : Int32
      value = u32
      raise CodecError.new("AMQP value length exceeds Int32") if value > Int32::MAX
      value.to_i32
    end

    private def numeric_descriptor_with_code(code : UInt8) : UInt64?
      case code
      when TypeCode::UByte      then u8.to_u64
      when TypeCode::UInt0      then 0_u64
      when TypeCode::SmallUInt  then u8.to_u64
      when TypeCode::UInt       then u32.to_u64
      when TypeCode::ULong0     then 0_u64
      when TypeCode::SmallULong then u8.to_u64
      when TypeCode::ULong      then u64
      else                           nil
      end
    end

    private def raw(size : Int32) : Bytes
      raise CodecError.new("negative AMQP value size") if size < 0
      raise CodecError.new("truncated AMQP value: need #{size}, have #{remaining}") if size > remaining
      result = bytes[position, size]
      @position += size
      result
    end

    private def binary(size : Int32) : Bytes
      value = raw(size)
      @zero_copy ? value : value.dup
    end

    private def u8 : UInt8
      raise CodecError.new("unexpected end of AMQP value") if eof?
      value = bytes[position]
      @position += 1
      value
    end

    private def i8 : Int8
      u8.to_i8!
    end

    {% for bits in [16, 32, 64] %}
      private def u{{bits}} : UInt{{bits}}
        decode(UInt{{bits}})
      end

      private def i{{bits}} : Int{{bits}}
        decode(Int{{bits}})
      end
    {% end %}

    private def f32 : Float32
      decode(Float32)
    end

    private def f64 : Float64
      decode(Float64)
    end

    private def decode(type : T.class) : T forall T
      size = sizeof(T)
      raise CodecError.new("unexpected end of AMQP value") if remaining < size
      value = IO::ByteFormat::BigEndian.decode(T, bytes[position, size])
      @position += size
      value
    end
  end
end
