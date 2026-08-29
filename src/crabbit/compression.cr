require "compress/gzip"
require "lz4"
require "snappy"
require "zstd/compress/context"
require "zstd/decompress/context"

require "./compression/snappy_crc32c_patch"

module Crabbit
  enum Compression : UInt8
    None   = 0_u8
    Gzip   = 1_u8
    Snappy = 2_u8
    Lz4    = 3_u8
    Zstd   = 4_u8
  end

  abstract class CompressionCodec
    abstract def compression : Compression
    abstract def compress(source : Bytes) : Bytes
    abstract def decompress(source : Bytes, expected_size : Int32) : Bytes

    protected def validate_size!(result : Bytes, expected_size : Int32) : Bytes
      unless result.size == expected_size
        raise CompressionError.new(
          "#{compression} produced #{result.size} bytes, expected #{expected_size}",
        )
      end
      result
    end

    protected def read_bounded(io : IO, expected_size : Int32) : Bytes
      raise CompressionError.new("negative uncompressed size") if expected_size < 0
      buffer = Bytes.new(expected_size + 1)
      read = 0
      while read < buffer.size
        count = io.read(buffer[read, buffer.size - read])
        break if count == 0
        read += count
      end
      if read != expected_size
        raise CompressionError.new(
          "#{compression} produced #{read} bytes, expected #{expected_size}",
        )
      end
      buffer[0, read].dup
    end
  end

  class NoCompressionCodec < CompressionCodec
    def compression : Compression
      Compression::None
    end

    def compress(source : Bytes) : Bytes
      source.dup
    end

    def decompress(source : Bytes, expected_size : Int32) : Bytes
      validate_size!(source.dup, expected_size)
    end
  end

  class GzipCompressionCodec < CompressionCodec
    def compression : Compression
      Compression::Gzip
    end

    def compress(source : Bytes) : Bytes
      output = IO::Memory.new
      Compress::Gzip::Writer.open(output) { |writer| writer.write(source) }
      output.to_slice.dup
    rescue ex
      raise CompressionError.new("Gzip compression failed: #{ex.message}")
    end

    def decompress(source : Bytes, expected_size : Int32) : Bytes
      input = IO::Memory.new(source, false)
      Compress::Gzip::Reader.open(input) { |reader| read_bounded(reader, expected_size) }
    rescue ex : CompressionError
      raise ex
    rescue ex
      raise CompressionError.new("Gzip decompression failed: #{ex.message}")
    end
  end

  class SnappyCompressionCodec < CompressionCodec
    def compression : Compression
      Compression::Snappy
    end

    def compress(source : Bytes) : Bytes
      output = IO::Memory.new
      Compress::Snappy::Writer.open(output) { |writer| writer.write(source) }
      output.to_slice.dup
    rescue ex
      raise CompressionError.new("Snappy compression failed: #{ex.message}")
    end

    def decompress(source : Bytes, expected_size : Int32) : Bytes
      input = IO::Memory.new(source, false)
      Compress::Snappy::Reader.open(input) { |reader| read_bounded(reader, expected_size) }
    rescue ex : CompressionError
      raise ex
    rescue ex
      raise CompressionError.new("Snappy decompression failed: #{ex.message}")
    end
  end

  class Lz4CompressionCodec < CompressionCodec
    def compression : Compression
      Compression::Lz4
    end

    def compress(source : Bytes) : Bytes
      Compress::LZ4.encode(source).dup
    rescue ex
      raise CompressionError.new("LZ4 compression failed: #{ex.message}")
    end

    def decompress(source : Bytes, expected_size : Int32) : Bytes
      input = IO::Memory.new(source, false)
      Compress::LZ4::Reader.open(input) { |reader| read_bounded(reader, expected_size) }
    rescue ex : CompressionError
      raise ex
    rescue ex
      raise CompressionError.new("LZ4 decompression failed: #{ex.message}")
    end
  end

  class ZstdCompressionCodec < CompressionCodec
    def initialize(@level : Int32 = 3)
    end

    def compression : Compression
      Compression::Zstd
    end

    def compress(source : Bytes) : Bytes
      Zstd::Compress::Context.new(level: @level).compress(source).dup
    rescue ex
      raise CompressionError.new("Zstd compression failed: #{ex.message}")
    end

    def decompress(source : Bytes, expected_size : Int32) : Bytes
      destination = Bytes.new(expected_size)
      result = Zstd::Decompress::Context.new.decompress(source, destination)
      validate_size!(result.dup, expected_size)
    rescue ex : CompressionError
      raise ex
    rescue ex
      raise CompressionError.new("Zstd decompression failed: #{ex.message}")
    end
  end

  class CompressionCodecs
    @mutex = Mutex.new
    @codecs : Hash(Compression, CompressionCodec)

    def initialize
      @codecs = {} of Compression => CompressionCodec
      register(NoCompressionCodec.new)
      register(GzipCompressionCodec.new)
      register(SnappyCompressionCodec.new)
      register(Lz4CompressionCodec.new)
      register(ZstdCompressionCodec.new)
    end

    def initialize(codecs : Enumerable(CompressionCodec))
      @codecs = {} of Compression => CompressionCodec
      codecs.each { |codec| register(codec) }
    end

    def register(codec : CompressionCodec) : self
      @mutex.synchronize { @codecs[codec.compression] = codec }
      self
    end

    def fetch(compression : Compression) : CompressionCodec
      @mutex.synchronize { @codecs[compression]? } ||
        raise CompressionError.new("no codec registered for #{compression}")
    end

    def dup : self
      self.class.new(@mutex.synchronize { @codecs.values.dup })
    end
  end
end
