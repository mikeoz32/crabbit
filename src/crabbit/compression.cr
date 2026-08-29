require "compress/gzip"
require "lz4"
require "snappy"
require "zstd/compress/context"
require "zstd/decompress/context"

require "./compression/snappy_crc32c_patch"

module Crabbit
  # RabbitMQ Stream sub-entry compression algorithm.
  enum Compression : UInt8
    # No compression; messages may still be packed as sub-entries.
    None = 0_u8
    # Gzip compression.
    Gzip = 1_u8
    # Snappy framed-stream compression.
    Snappy = 2_u8
    # LZ4 frame compression.
    Lz4 = 3_u8
    # Zstandard compression.
    Zstd = 4_u8
  end

  # Interface for a Stream sub-entry compression algorithm.
  #
  # Custom implementations can be installed with
  # `CompressionCodecs#register`. Decompression must return exactly
  # *expected_size* bytes or raise `CompressionError`.
  abstract class CompressionCodec
    # Returns the protocol algorithm implemented by this codec.
    abstract def compression : Compression
    # Compresses *source* and returns a newly owned byte slice.
    abstract def compress(source : Bytes) : Bytes
    # Decompresses *source* to exactly *expected_size* bytes.
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

  # Pass-through codec for `Compression::None` sub-entries.
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

  # Gzip implementation of `CompressionCodec`.
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

  # Snappy framed-stream implementation of `CompressionCodec`.
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

  # LZ4 frame implementation of `CompressionCodec`.
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

  # Zstandard implementation of `CompressionCodec`.
  class ZstdCompressionCodec < CompressionCodec
    # Creates a codec using the requested Zstandard compression *level*.
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

  # Thread-safe registry of codecs used for Stream sub-entry batches.
  class CompressionCodecs
    @mutex = Mutex.new
    @codecs : Hash(Compression, CompressionCodec)

    # Creates a registry containing None, Gzip, Snappy, LZ4, and Zstandard.
    def initialize
      @codecs = {} of Compression => CompressionCodec
      register(NoCompressionCodec.new)
      register(GzipCompressionCodec.new)
      register(SnappyCompressionCodec.new)
      register(Lz4CompressionCodec.new)
      register(ZstdCompressionCodec.new)
    end

    # Creates a registry containing only *codecs*.
    def initialize(codecs : Enumerable(CompressionCodec))
      @codecs = {} of Compression => CompressionCodec
      codecs.each { |codec| register(codec) }
    end

    # Registers or replaces the implementation for `codec.compression`.
    #
    # Returns `self` for chaining.
    def register(codec : CompressionCodec) : self
      @mutex.synchronize { @codecs[codec.compression] = codec }
      self
    end

    # Returns the codec registered for *compression*.
    #
    # Raises `CompressionError` when no implementation is installed.
    def fetch(compression : Compression) : CompressionCodec
      @mutex.synchronize { @codecs[compression]? } ||
        raise CompressionError.new("no codec registered for #{compression}")
    end

    # Returns an independent registry containing the current codec objects.
    def dup : self
      self.class.new(@mutex.synchronize { @codecs.values.dup })
    end
  end
end
