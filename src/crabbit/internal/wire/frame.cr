# :nodoc:
module Crabbit::Internal::Wire
  record Frame, key : UInt16, version : UInt16, body : Bytes do
    def response? : Bool
      (key & RESPONSE_MASK) != 0
    end

    def command_key : UInt16
      key & KEY_MASK
    end

    def command : Command
      Command.from_value(command_key)
    rescue ArgumentError
      raise ProtocolError.new("unknown command key #{command_key}")
    end

    def reader : Reader
      Reader.new(body)
    end
  end

  module FrameCodec
    extend self

    def request(command : Command, correlation_id : UInt32, version : UInt16 = 1_u16, &block : Writer ->) : Bytes
      build(command.value, version) do |writer|
        writer.write_u32(correlation_id)
        yield writer
      end
    end

    def command(
      command : Command,
      version : UInt16 = 1_u16,
      response : Bool = false,
      initial_capacity : Int32 = 0,
      &block : Writer ->
    ) : Bytes
      key = response ? command.value | RESPONSE_MASK : command.value
      build(key, version, initial_capacity) { |writer| yield writer }
    end

    def response(command : Command, correlation_id : UInt32, code : ResponseCode, version : UInt16 = 1_u16, &block : Writer ->) : Bytes
      build(command.value | RESPONSE_MASK, version) do |writer|
        writer.write_u32(correlation_id).write_u16(code.value)
        yield writer
      end
    end

    def empty_command(command : Command, version : UInt16 = 1_u16, response : Bool = false) : Bytes
      key = response ? command.value | RESPONSE_MASK : command.value
      build(key, version, 8) { }
    end

    def encode(key : UInt16, version : UInt16, body : Bytes) : Bytes
      size = 4_i64 + body.size
      raise ProtocolError.new("frame exceeds UInt32 size") if size > UInt32::MAX
      total_size = body.size.to_i64 + 8_i64
      raise ProtocolError.new("frame exceeds Int32 memory limit") if total_size > Int32::MAX
      writer = Writer.new(total_size.to_i32)
      writer.write_u32(size.to_u32).write_u16(key).write_u16(version).write_raw(body)
      writer.to_slice
    end

    def decode(bytes : Bytes, max_frame_size : UInt32 = 0_u32) : Frame
      reader = Reader.new(bytes)
      size = reader.read_u32
      total_size = size.to_u64 + 4_u64
      if max_frame_size > 0 && total_size > max_frame_size
        raise FrameTooLargeError.new(total_size.to_u32!, max_frame_size)
      end
      raise ProtocolError.new("frame size exceeds Int32 memory limit") if size > Int32::MAX
      raise ProtocolError.new("frame size must include key and version") if size < 4
      raise ProtocolError.new("declared frame size #{size} does not match #{reader.remaining}") unless size == reader.remaining
      key = reader.read_u16
      version = reader.read_u16
      body = reader.read_exactly(reader.remaining)
      Frame.new(key, version, body)
    end

    def read(io : IO, max_frame_size : UInt32 = 0_u32) : Frame
      prefix = Bytes.new(4)
      io.read_fully(prefix)
      size = IO::ByteFormat::BigEndian.decode(UInt32, prefix)
      total_size = size.to_u64 + 4_u64
      if max_frame_size > 0 && total_size > max_frame_size
        raise FrameTooLargeError.new(total_size.to_u32!, max_frame_size)
      end
      raise ProtocolError.new("frame size exceeds Int32 memory limit") if size > Int32::MAX
      raise ProtocolError.new("frame size must include key and version") if size < 4
      rest = Bytes.new(size.to_i32)
      io.read_fully(rest)
      key = IO::ByteFormat::BigEndian.decode(UInt16, rest)
      version = IO::ByteFormat::BigEndian.decode(UInt16, rest[2, 2])
      Frame.new(key, version, rest[4, rest.size - 4])
    rescue IO::EOFError
      raise ConnectionClosedError.new("connection closed while reading frame")
    end

    private def build(
      key : UInt16,
      version : UInt16,
      initial_capacity : Int32 = 0,
      &block : Writer ->
    ) : Bytes
      capacity = Math.max(initial_capacity, 8)
      writer = Writer.new(capacity)
      writer.write_u32(0_u32).write_u16(key).write_u16(version)
      yield writer

      size = writer.size.to_i64 - 4_i64
      raise ProtocolError.new("frame exceeds UInt32 size") if size > UInt32::MAX
      end_position = writer.io.pos
      writer.io.pos = 0
      writer.write_u32(size.to_u32)
      writer.io.pos = end_position
      writer.to_slice
    end
  end
end
