# Snappy 0.1.7 is the latest release with a valid shard.yml. Its framing CRC
# starts from the wrong register value. Keep this tiny compatibility patch
# until upstream publishes a valid release containing the v0.2.0 CRC fix.
class Compress::Snappy::CRC32C
  def self.masked_crc32c(data : Slice, length : Int32)
    raise ArgumentError.new("length exceeds input") if length < 0 || length > data.size
    crc = 0xffff_ffff_u32
    length.times do |index|
      crc ^= data[index].to_u32
      8.times do
        crc = (crc >> 1) ^ ((crc & 1) == 1 ? 0x82f6_3b78_u32 : 0_u32)
      end
    end
    mask(~crc)
  end
end
