require "../../spec_helper"

private alias Wire = Crabbit::Internal::Wire

describe Crabbit::Internal::Wire::FrameCodec do
  it "encodes and decodes a request frame in network byte order" do
    bytes = Wire::FrameCodec.request(Wire::Command::Open, 0x01020304_u32) do |writer|
      writer.write_string("/")
    end

    bytes.should eq Bytes[
      0x00, 0x00, 0x00, 0x0b,
      0x00, 0x15, 0x00, 0x01,
      0x01, 0x02, 0x03, 0x04,
      0x00, 0x01, 0x2f,
    ]

    frame = Wire::FrameCodec.decode(bytes)
    frame.command.should eq Wire::Command::Open
    frame.response?.should be_false
    reader = frame.reader
    reader.read_u32.should eq 0x01020304_u32
    reader.read_string.should eq "/"
    reader.finish!
  end

  it "rejects truncated and oversized frames" do
    expect_raises(Crabbit::ProtocolError) do
      Wire::FrameCodec.decode(Bytes[0, 0, 0, 8, 0, 1, 0, 1])
    end

    bytes = Wire::FrameCodec.empty_command(Wire::Command::Heartbeat)
    expect_raises(Crabbit::FrameTooLargeError) do
      Wire::FrameCodec.decode(bytes, 7_u32)
    end
  end
end
