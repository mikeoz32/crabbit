require "./spec_helper"

describe Crabbit::CompressionCodecs do
  codecs = Crabbit::CompressionCodecs.new
  source = Bytes.new(16_384) { |index| (index % 17).to_u8 }

  Crabbit::Compression.each do |compression|
    it "round-trips #{compression}" do
      codec = codecs.fetch(compression)
      encoded = codec.compress(source)
      codec.decompress(encoded, source.size).should eq source
    end
  end

  it "rejects a mismatched declared size" do
    encoded = codecs.fetch(Crabbit::Compression::Gzip).compress(source)
    expect_raises(Crabbit::CompressionError) do
      codecs.fetch(Crabbit::Compression::Gzip).decompress(encoded, source.size - 1)
    end
  end
end

describe Crabbit::Internal::SubEntryCodec do
  it "frames and restores length-prefixed messages" do
    codec = Crabbit::Internal::SubEntryCodec.new(Crabbit::CompressionCodecs.new)
    messages = ["one".to_slice, "two".to_slice, Bytes.new(1024, 7_u8)]
    Crabbit::Compression.each do |compression|
      entry = codec.encode(messages, compression)
      entry.record_count.should eq 3_u16
      codec.decode(entry).should eq messages
    end
  end
end
