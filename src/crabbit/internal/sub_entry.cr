module Crabbit::Internal
  record EncodedSubEntry,
    compression : Compression,
    record_count : UInt16,
    uncompressed_size : UInt32,
    data : Bytes

  class SubEntryCodec
    DEFAULT_MAX_UNCOMPRESSED_SIZE = 64 * 1024 * 1024

    def initialize(
      @codecs : CompressionCodecs,
      @max_uncompressed_size : Int32 = DEFAULT_MAX_UNCOMPRESSED_SIZE,
    )
    end

    def encode(messages : Array(Bytes), compression : Compression) : EncodedSubEntry
      raise ArgumentError.new("sub-entry batch cannot be empty") if messages.empty?
      raise ArgumentError.new("sub-entry batch exceeds UInt16 record count") if messages.size > UInt16::MAX
      size = messages.sum { |message| 4_i64 + message.size }
      if size > @max_uncompressed_size
        raise CompressionError.new("sub-entry batch size #{size} exceeds #{@max_uncompressed_size}")
      end
      output = IO::Memory.new
      messages.each do |message|
        output.write_bytes(message.size.to_u32, IO::ByteFormat::BigEndian)
        output.write(message)
      end
      uncompressed = output.to_slice
      compressed = @codecs.fetch(compression).compress(uncompressed)
      EncodedSubEntry.new(compression, messages.size.to_u16, uncompressed.size.to_u32, compressed)
    end

    def decode(entry : EncodedSubEntry) : Array(Bytes)
      if entry.uncompressed_size > @max_uncompressed_size
        raise CompressionError.new(
          "declared sub-entry size #{entry.uncompressed_size} exceeds #{@max_uncompressed_size}",
        )
      end
      uncompressed = @codecs.fetch(entry.compression).decompress(
        entry.data,
        entry.uncompressed_size.to_i32,
      )
      reader = Wire::Reader.new(uncompressed)
      messages = Array(Bytes).new(entry.record_count) do
        size = reader.read_u32
        raise ProtocolError.new("sub-entry message exceeds Int32") if size > Int32::MAX
        reader.read_exactly(size.to_i32)
      end
      reader.finish!
      messages
    end
  end
end
