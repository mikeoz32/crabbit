require "../spec_helper"

private alias Wire = Crabbit::Internal::Wire

describe Crabbit::Internal::DeliverParser do
  it "parses simple and compressed sub-entry records with CRC validation" do
    codecs = Crabbit::CompressionCodecs.new
    sub_entries = Crabbit::Internal::SubEntryCodec.new(codecs)
    batch = sub_entries.encode(["two".to_slice, "three".to_slice], Crabbit::Compression::Gzip)

    data_writer = Wire::Writer.new
    data_writer.write_u32(3_u32).write_raw("one".to_slice)
    data_writer.write_u8(0x80_u8 | (batch.compression.value << 4))
    data_writer.write_u16(batch.record_count)
    data_writer.write_u32(batch.uncompressed_size)
    data_writer.write_u32(batch.data.size.to_u32)
    data_writer.write_raw(batch.data)
    data = data_writer.to_slice

    frame_bytes = Wire::FrameCodec.command(Wire::Command::Deliver, version: 2_u16) do |writer|
      writer.write_u8(7_u8)
      writer.write_u64(99_u64)
      writer.write_u8(0x50_u8)
      writer.write_u8(0_u8)
      writer.write_u16(2_u16)
      writer.write_u32(3_u32)
      writer.write_i64(1_700_000_000_000_i64)
      writer.write_u64(1_u64)
      writer.write_u64(42_u64)
      writer.write_u32(Digest::CRC32.checksum(data))
      writer.write_u32(data.size.to_u32)
      writer.write_u32(0_u32)
      writer.write_u8(0_u8)
      writer.write_raw(Bytes[0, 0, 0])
      writer.write_raw(data)
    end

    chunk = Crabbit::Internal::DeliverParser.new(codecs).parse(Wire::FrameCodec.decode(frame_bytes))
    chunk.subscription_id.should eq 7_u8
    chunk.committed_chunk_id.should eq 99_u64
    chunk.first_offset.should eq 42_u64
    chunk.deliveries.map(&.offset).should eq [42_u64, 43_u64, 44_u64]
    chunk.deliveries.map(&.bytes).should eq ["one".to_slice, "two".to_slice, "three".to_slice]
  end

  it "rejects corrupt chunk data" do
    data = Bytes[0, 0, 0, 1, 1]
    frame_bytes = Wire::FrameCodec.command(Wire::Command::Deliver) do |writer|
      writer.write_u8(1_u8).write_u8(0x50_u8).write_u8(0_u8)
      writer.write_u16(1_u16).write_u32(1_u32).write_i64(0_i64)
      writer.write_u64(0_u64).write_u64(0_u64)
      writer.write_u32(123_u32).write_u32(data.size.to_u32)
      writer.write_u32(0_u32).write_u32(0_u32).write_raw(data)
    end
    parser = Crabbit::Internal::DeliverParser.new(Crabbit::CompressionCodecs.new)
    expect_raises(Crabbit::ProtocolError, /CRC mismatch/) do
      parser.parse(Wire::FrameCodec.decode(frame_bytes))
    end
  end

  it "rejects hostile record counts before allocating" do
    frame_bytes = Wire::FrameCodec.command(Wire::Command::Deliver) do |writer|
      writer.write_u8(1_u8).write_u8(0x50_u8).write_u8(0_u8)
      writer.write_u16(1_u16).write_u32(1_000_001_u32)
    end
    parser = Crabbit::Internal::DeliverParser.new(Crabbit::CompressionCodecs.new)
    expect_raises(Crabbit::ProtocolError, /record count/) do
      parser.parse(Wire::FrameCodec.decode(frame_bytes))
    end
  end
end
