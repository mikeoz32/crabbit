# :nodoc:
module Crabbit::Internal
  record RawDelivery,
    offset : UInt64,
    timestamp : Time,
    committed_chunk_id : UInt64,
    bytes : Bytes

  record DeliveredChunk,
    subscription_id : UInt8,
    first_offset : UInt64,
    committed_chunk_id : UInt64,
    timestamp : Time,
    physical_entry_count : UInt16,
    record_count : UInt32,
    deliveries : Array(RawDelivery)

  class DeliverParser
    DEFAULT_MAX_CHUNK_SIZE              = 64 * 1024 * 1024
    DEFAULT_MAX_UNCOMPRESSED_CHUNK_SIZE = 256 * 1024 * 1024
    DEFAULT_MAX_RECORDS_PER_CHUNK       = 1_000_000

    def initialize(
      @codecs : CompressionCodecs,
      @max_chunk_size : Int32 = DEFAULT_MAX_CHUNK_SIZE,
      @max_uncompressed_sub_entry_size : Int32 = SubEntryCodec::DEFAULT_MAX_UNCOMPRESSED_SIZE,
      @max_uncompressed_chunk_size : Int32 = DEFAULT_MAX_UNCOMPRESSED_CHUNK_SIZE,
      @validate_crc : Bool = true,
      @max_records_per_chunk : Int32 = DEFAULT_MAX_RECORDS_PER_CHUNK,
    )
      @sub_entries = SubEntryCodec.new(codecs, @max_uncompressed_sub_entry_size)
    end

    def parse(frame : Wire::Frame) : DeliveredChunk
      raise ProtocolError.new("expected Deliver frame") unless frame.command.deliver?
      unless frame.version == 1_u16 || frame.version == 2_u16
        raise ProtocolError.new("unsupported Deliver version #{frame.version}")
      end

      reader = frame.reader
      subscription_id = reader.read_u8
      committed_chunk_id = frame.version >= 2 ? reader.read_u64 : 0_u64
      magic_version = reader.read_u8
      magic = magic_version >> 4
      raise ProtocolError.new("invalid Osiris chunk magic #{magic}") unless magic == 5
      chunk_type = reader.read_u8
      raise ProtocolError.new("unsupported Osiris chunk type #{chunk_type}") unless chunk_type == 0
      physical_entries = reader.read_u16
      record_count = reader.read_u32
      if record_count > @max_records_per_chunk
        raise ProtocolError.new(
          "chunk record count #{record_count} exceeds #{@max_records_per_chunk}",
        )
      end
      timestamp_ms = reader.read_i64
      reader.read_u64 # epoch
      first_offset = reader.read_u64
      expected_crc = reader.read_u32
      data_length = reader.read_u32
      trailer_length = reader.read_u32
      bloom_size = reader.read_u8
      reader.read_exactly(3) # reserved bits

      if data_length > @max_chunk_size
        raise ProtocolError.new("chunk data size #{data_length} exceeds #{@max_chunk_size}")
      end
      if data_length > Int32::MAX
        raise ProtocolError.new("chunk data size exceeds Int32")
      end
      data = reader.read_exactly(data_length.to_i32)
      if @validate_crc
        actual_crc = Digest::CRC32.checksum(data)
        unless actual_crc == expected_crc
          raise ProtocolError.new(
            "chunk CRC mismatch at offset #{first_offset}: expected #{expected_crc}, got #{actual_crc}",
          )
        end
      end

      # The stream plugin currently sends the records portion only. Preserve
      # compatibility with future chunks that append the declared trailer or
      # bloom bytes instead of treating them as a second frame.
      optional_size = trailer_length.to_u64 + bloom_size.to_u64
      if reader.remaining > 0
        if optional_size == reader.remaining.to_u64
          reader.read_exactly(reader.remaining)
        else
          raise ProtocolError.new("unexpected #{reader.remaining} bytes after Osiris chunk data")
        end
      end
      reader.finish!

      deliveries = parse_records(
        data,
        record_count,
        physical_entries,
        first_offset,
        timestamp_ms,
        committed_chunk_id,
      )
      DeliveredChunk.new(
        subscription_id,
        first_offset,
        committed_chunk_id,
        Time.unix_ms(timestamp_ms),
        physical_entries,
        record_count,
        deliveries,
      )
    rescue ex : OverflowError
      raise ProtocolError.new("numeric overflow in Deliver frame: #{ex.message}")
    end

    private def parse_records(
      data : Bytes,
      record_count : UInt32,
      physical_entries : UInt16,
      first_offset : UInt64,
      timestamp_ms : Int64,
      committed_chunk_id : UInt64,
    ) : Array(RawDelivery)
      reader = Wire::Reader.new(data)
      deliveries = Array(RawDelivery).new(record_count.to_i32)
      remaining_records = record_count.to_i64
      entries = 0
      offset = first_offset
      decompression_budget = @max_uncompressed_chunk_size.to_i64

      while remaining_records > 0
        raise ProtocolError.new("chunk ended before all records were decoded") if reader.remaining == 0
        first = reader.read_u8
        entries += 1
        if (first & 0x80) == 0
          size = ((first.to_u32 & 0x7f_u32) << 24) |
                 (reader.read_u8.to_u32 << 16) |
                 (reader.read_u8.to_u32 << 8) |
                 reader.read_u8.to_u32
          raise ProtocolError.new("message size exceeds Int32") if size > Int32::MAX
          payload = reader.read_exactly(size.to_i32)
          deliveries << RawDelivery.new(
            offset,
            Time.unix_ms(timestamp_ms),
            committed_chunk_id,
            payload,
          )
          offset &+= 1_u64
          remaining_records -= 1
        else
          raw_compression = (first & 0x70) >> 4
          compression = begin
            Compression.from_value(raw_compression)
          rescue ArgumentError
            raise ProtocolError.new("unknown sub-entry compression code #{raw_compression}")
          end
          batch_count = reader.read_u16
          raise ProtocolError.new("sub-entry batch cannot declare zero records") if batch_count == 0
          if batch_count.to_i64 > remaining_records
            raise ProtocolError.new("sub-entry record count exceeds remaining chunk records")
          end
          uncompressed_size = reader.read_u32
          data_size = reader.read_u32
          if data_size > Int32::MAX
            raise ProtocolError.new("compressed sub-entry size exceeds Int32")
          end
          decompression_budget -= uncompressed_size
          if compression != Compression::None && decompression_budget < 0
            raise ProtocolError.new(
              "chunk exceeds #{@max_uncompressed_chunk_size} bytes of decompression work",
            )
          end
          compressed = reader.read_exactly(data_size.to_i32)
          entry = EncodedSubEntry.new(compression, batch_count, uncompressed_size, compressed)
          @sub_entries.decode(entry).each do |payload|
            deliveries << RawDelivery.new(
              offset,
              Time.unix_ms(timestamp_ms),
              committed_chunk_id,
              payload,
            )
            offset &+= 1_u64
          end
          remaining_records -= batch_count
        end
      end

      reader.finish!
      unless entries == physical_entries
        raise ProtocolError.new(
          "chunk declared #{physical_entries} physical entries but contained #{entries}",
        )
      end
      unless deliveries.size == record_count
        raise ProtocolError.new(
          "chunk declared #{record_count} records but contained #{deliveries.size}",
        )
      end
      deliveries
    end
  end
end
