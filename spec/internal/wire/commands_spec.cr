require "../../spec_helper"

private alias Wire = Crabbit::Internal::Wire

describe Crabbit::Internal::Wire::Commands do
  it "encodes an unnamed publisher reference as an empty string" do
    frame = Wire::FrameCodec.decode(Wire::Commands.declare_publisher(3_u32, 7_u8, nil, "events"))
    reader = frame.reader
    reader.read_u32.should eq 3_u32
    reader.read_u8.should eq 7_u8
    reader.read_string.should eq ""
    reader.read_string.should eq "events"
    reader.finish!
  end

  it "decodes metadata without a top-level response code" do
    bytes = Wire::FrameCodec.command(Wire::Command::Metadata, response: true) do |writer|
      writer.write_u32(9_u32)
      writer.write_i32(1).write_u16(4_u16).write_string("broker").write_u32(5_552_u32)
      writer.write_i32(1).write_string("events")
      writer.write_u16(Crabbit::ResponseCode::Ok.value).write_u16(4_u16).write_i32(0)
    end
    metadata = Wire::Commands.decode_metadata(Wire::FrameCodec.decode(bytes))

    metadata.should eq [
      Crabbit::StreamMetadata.new(
        "events",
        Crabbit::ResponseCode::Ok,
        Crabbit::Broker.new(4_u16, "broker", 5_552),
        [] of Crabbit::Broker,
      ),
    ]
  end

  it "encodes publish version 2 filter values" do
    bytes = Wire::Commands.publish(
      7_u8,
      [{42_u64, Bytes[1, 2, 3], "invoices"}],
      version: 2_u16,
    )
    frame = Wire::FrameCodec.decode(bytes)
    frame.command.should eq Wire::Command::Publish
    frame.version.should eq 2_u16
    reader = frame.reader
    reader.read_u8.should eq 7_u8
    reader.read_count.should eq 1
    reader.read_u64.should eq 42_u64
    reader.read_string.should eq "invoices"
    reader.read_bytes.should eq Bytes[1, 2, 3]
    reader.finish!
  end

  it "encodes every offset specification" do
    {
      Crabbit::OffsetSpecification.first          => {Crabbit::OffsetType::First, nil},
      Crabbit::OffsetSpecification.last           => {Crabbit::OffsetType::Last, nil},
      Crabbit::OffsetSpecification.next           => {Crabbit::OffsetType::Next, nil},
      Crabbit::OffsetSpecification.offset(9)      => {Crabbit::OffsetType::Offset, 9_i64},
      Crabbit::OffsetSpecification.timestamp(123) => {Crabbit::OffsetType::Timestamp, 123_i64},
    }.each do |offset, expected|
      frame = Wire::FrameCodec.decode(Wire::Commands.resolve_offset(1_u32, "s", offset))
      reader = frame.reader
      reader.read_u32
      reader.read_string.should eq "s"
      reader.read_u16.should eq expected[0].value
      case expected[0]
      when .offset?    then reader.read_u64.should eq expected[1].not_nil!.to_u64
      when .timestamp? then reader.read_i64.should eq expected[1]
      end
      reader.read_string_map.should be_empty
      reader.finish!
    end
  end

  it "preserves the full unsigned offset range" do
    offset = Crabbit::OffsetSpecification.offset(UInt64::MAX)
    frame = Wire::FrameCodec.decode(Wire::Commands.resolve_offset(1_u32, "s", offset))
    reader = frame.reader
    reader.read_u32
    reader.read_string
    reader.read_u16.should eq Crabbit::OffsetType::Offset.value
    reader.read_u64.should eq UInt64::MAX
  end
end
