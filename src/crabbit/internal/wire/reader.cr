# :nodoc:
module Crabbit::Internal::Wire
  class Reader
    MAX_COLLECTION_ENTRIES = 1_000_000

    getter io : IO::Memory

    def initialize(bytes : Bytes)
      @io = IO::Memory.new(bytes, false)
    end

    def remaining : Int32
      io.size - io.pos
    end

    def read_u8 : UInt8
      io.read_byte || raise ProtocolError.new("unexpected end of frame")
    end

    def read_i8 : Int8
      read_u8.to_i8!
    end

    {% for bits in [16, 32, 64] %}
      def read_u{{bits}} : UInt{{bits}}
        io.read_bytes(UInt{{bits}}, IO::ByteFormat::BigEndian)
      rescue IO::EOFError
        raise ProtocolError.new("unexpected end of frame")
      end

      def read_i{{bits}} : Int{{bits}}
        io.read_bytes(Int{{bits}}, IO::ByteFormat::BigEndian)
      rescue IO::EOFError
        raise ProtocolError.new("unexpected end of frame")
      end
    {% end %}

    def read_bytes : Bytes?
      length = read_i32
      return nil if length == -1
      raise ProtocolError.new("invalid byte sequence length #{length}") if length < 0
      read_exactly(length)
    end

    def read_string : String?
      length = read_i16
      return nil if length == -1
      raise ProtocolError.new("invalid string length #{length}") if length < 0
      String.new(read_exactly(length.to_i32))
    rescue ex : ArgumentError
      raise ProtocolError.new("invalid UTF-8 protocol string: #{ex.message}")
    end

    def read_string! : String
      read_string || raise ProtocolError.new("unexpected null string")
    end

    def read_count(maximum : Int32 = remaining) : Int32
      count = read_i32
      raise ProtocolError.new("invalid array length #{count}") if count < 0
      limit = Math.min(maximum, MAX_COLLECTION_ENTRIES)
      raise ProtocolError.new("array length #{count} exceeds safe limit #{limit}") if count > limit
      count
    end

    def read_string_array : Array(String)
      Array(String).new(read_count) { read_string! }
    end

    def read_string_map : Hash(String, String)
      result = {} of String => String
      read_count.times { result[read_string!] = read_string! }
      result
    end

    def read_exactly(size : Int32) : Bytes
      raise ProtocolError.new("negative read size") if size < 0
      raise ProtocolError.new("frame declares #{size} bytes with only #{remaining} remaining") if size > remaining
      bytes = Bytes.new(size)
      io.read_fully(bytes)
      bytes
    rescue IO::EOFError
      raise ProtocolError.new("unexpected end of frame")
    end

    def finish! : Nil
      raise ProtocolError.new("#{remaining} trailing bytes in frame") unless remaining == 0
    end
  end
end
