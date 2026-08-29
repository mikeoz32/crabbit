# :nodoc:
module Crabbit::Internal::Wire
  class Writer
    getter io : IO::Memory

    def initialize
      @io = IO::Memory.new
    end

    def initialize(initial_capacity : Int32)
      @io = IO::Memory.new(initial_capacity)
    end

    def initialize(@io : IO::Memory)
    end

    def size : Int32
      io.size
    end

    def write_u8(value : UInt8) : self
      io.write_byte(value)
      self
    end

    def write_i8(value : Int8) : self
      io.write_byte(value.to_u8!)
      self
    end

    {% for bits in [16, 32, 64] %}
      def write_u{{bits}}(value : UInt{{bits}}) : self
        io.write_bytes(value, IO::ByteFormat::BigEndian)
        self
      end

      def write_i{{bits}}(value : Int{{bits}}) : self
        io.write_bytes(value, IO::ByteFormat::BigEndian)
        self
      end
    {% end %}

    def write_bytes(value : Bytes?) : self
      if value
        raise ProtocolError.new("byte sequence exceeds Int32") if value.size > Int32::MAX
        write_i32(value.size.to_i32)
        io.write(value)
      else
        write_i32(-1)
      end
      self
    end

    def write_raw(value : Bytes) : self
      io.write(value)
      self
    end

    def write_string(value : String?) : self
      if value
        bytes = value.to_slice
        raise ProtocolError.new("string exceeds Int16 protocol limit") if bytes.size > Int16::MAX
        write_i16(bytes.size.to_i16)
        io.write(bytes)
      else
        write_i16(-1)
      end
      self
    end

    def write_array(values : Enumerable, &block : Writer, typeof(values.first) ->) : self
      ary = values.to_a
      raise ProtocolError.new("array exceeds Int32") if ary.size > Int32::MAX
      write_i32(ary.size.to_i32)
      ary.each { |value| yield self, value }
      self
    end

    def write_string_map(values : Hash(String, String)) : self
      write_i32(values.size.to_i32)
      values.each do |key, value|
        write_string(key)
        write_string(value)
      end
      self
    end

    def to_slice : Bytes
      io.to_slice
    end
  end
end
