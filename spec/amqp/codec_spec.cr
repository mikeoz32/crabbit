require "../spec_helper"

describe Crabbit::AMQP::Encoder do
  it "round-trips every scalar AMQP type" do
    uuid = Crabbit::AMQP::UUID.from_slice(Bytes.new(16) { |index| index.to_u8 })
    values = [
      Crabbit::AMQP::Value.null,
      Crabbit::AMQP::Value.wrap(true),
      Crabbit::AMQP::Value.wrap(false),
      Crabbit::AMQP::Value.wrap(1_u8),
      Crabbit::AMQP::Value.wrap(2_u16),
      Crabbit::AMQP::Value.wrap(0_u32),
      Crabbit::AMQP::Value.wrap(255_u32),
      Crabbit::AMQP::Value.wrap(65_536_u32),
      Crabbit::AMQP::Value.wrap(0_u64),
      Crabbit::AMQP::Value.wrap(UInt64::MAX),
      Crabbit::AMQP::Value.wrap(-1_i8),
      Crabbit::AMQP::Value.wrap(-2_i16),
      Crabbit::AMQP::Value.wrap(-3_i32),
      Crabbit::AMQP::Value.wrap(Int32::MIN),
      Crabbit::AMQP::Value.wrap(-4_i64),
      Crabbit::AMQP::Value.wrap(Int64::MIN),
      Crabbit::AMQP::Value.wrap(1.25_f32),
      Crabbit::AMQP::Value.wrap(2.5_f64),
      Crabbit::AMQP::Value.new(
        Crabbit::AMQP::Kind::Decimal32,
        Crabbit::AMQP::Decimal.new(Bytes.new(4, 1_u8), 4),
      ),
      Crabbit::AMQP::Value.new(
        Crabbit::AMQP::Kind::Decimal64,
        Crabbit::AMQP::Decimal.new(Bytes.new(8, 2_u8), 8),
      ),
      Crabbit::AMQP::Value.new(
        Crabbit::AMQP::Kind::Decimal128,
        Crabbit::AMQP::Decimal.new(Bytes.new(16, 3_u8), 16),
      ),
      Crabbit::AMQP::Value.wrap('λ'),
      Crabbit::AMQP::Value.wrap(Time.unix_ms(1_700_000_000_123_i64)),
      Crabbit::AMQP::Value.wrap(uuid),
      Crabbit::AMQP::Value.wrap(Bytes[1, 2, 3]),
      Crabbit::AMQP::Value.wrap("hello λ"),
      Crabbit::AMQP::Value.symbol("invoices"),
    ]

    values.each do |value|
      encoded = Crabbit::AMQP::Encoder.encode(value)
      decoder = Crabbit::AMQP::Decoder.new(encoded)
      decoder.read.should eq value
      decoder.eof?.should be_true
    end
  end

  it "round-trips compound and described values" do
    map = {
      Crabbit::AMQP::Value.symbol("key") => Crabbit::AMQP::Value.wrap("value"),
    }
    values = [
      Crabbit::AMQP::Value.list([Crabbit::AMQP::Value.wrap(1_u32), Crabbit::AMQP::Value.null]),
      Crabbit::AMQP::Value.new(Crabbit::AMQP::Kind::Map, map),
      Crabbit::AMQP::Value.array([1_u64, 2_u64, 3_u64]),
      Crabbit::AMQP::Value.described(0x75_u64, Bytes[1, 2]),
    ]

    values.each do |value|
      encoded = Crabbit::AMQP::Encoder.encode(value)
      io = IO::Memory.new
      Crabbit::AMQP::Encoder.encode(value, io)

      io.to_slice.should eq encoded
      Crabbit::AMQP::Decoder.new(encoded).read.should eq value
    end
  end

  it "round-trips arrays for every AMQP element family" do
    uuid = Crabbit::AMQP::UUID.from_slice(Bytes.new(16) { |index| index.to_u8 })
    decimal32 = Crabbit::AMQP::Value.new(
      Crabbit::AMQP::Kind::Decimal32,
      Crabbit::AMQP::Decimal.new(Bytes[1, 2, 3, 4], 4),
    )
    map = Crabbit::AMQP::Value.map({"key" => 1_u32})
    list = Crabbit::AMQP::Value.list([1_u32, 2_u32])
    nested = Crabbit::AMQP::Value.array([1_u8, 2_u8])

    arrays = [
      Crabbit::AMQP::Value.array([true, false]),
      Crabbit::AMQP::Value.array([1_u8, 2_u8]),
      Crabbit::AMQP::Value.array([1_u16, 2_u16]),
      Crabbit::AMQP::Value.array([1_u32, UInt32::MAX]),
      Crabbit::AMQP::Value.array([1_u64, UInt64::MAX]),
      Crabbit::AMQP::Value.array([-1_i8, 2_i8]),
      Crabbit::AMQP::Value.array([-1_i16, 2_i16]),
      Crabbit::AMQP::Value.array([-1_i32, Int32::MAX]),
      Crabbit::AMQP::Value.array([-1_i64, Int64::MAX]),
      Crabbit::AMQP::Value.array([1.5_f32, 2.5_f32]),
      Crabbit::AMQP::Value.array([1.5_f64, 2.5_f64]),
      Crabbit::AMQP::Value.new(Crabbit::AMQP::Kind::Array, [decimal32, decimal32]),
      Crabbit::AMQP::Value.array(['a', 'λ']),
      Crabbit::AMQP::Value.array([Time.unix_ms(1_i64), Time.unix_ms(2_i64)]),
      Crabbit::AMQP::Value.array([uuid, uuid]),
      Crabbit::AMQP::Value.array([Bytes[1], Bytes[2, 3]]),
      Crabbit::AMQP::Value.array(["one", "two"]),
      Crabbit::AMQP::Value.new(
        Crabbit::AMQP::Kind::Array,
        [Crabbit::AMQP::Value.symbol("one"), Crabbit::AMQP::Value.symbol("two")],
      ),
      Crabbit::AMQP::Value.new(Crabbit::AMQP::Kind::Array, [list, list]),
      Crabbit::AMQP::Value.new(Crabbit::AMQP::Kind::Array, [map, map]),
      Crabbit::AMQP::Value.new(Crabbit::AMQP::Kind::Array, [nested, nested]),
      Crabbit::AMQP::Value.new(
        Crabbit::AMQP::Kind::Array,
        [
          Crabbit::AMQP::Value.described(0x75_u64, Bytes[1]),
          Crabbit::AMQP::Value.described(0x75_u64, Bytes[2]),
        ],
      ),
    ]

    arrays.each do |value|
      Crabbit::AMQP::Decoder.new(Crabbit::AMQP::Encoder.encode(value)).read.should eq(value)
    end
  end

  it "writes large containers directly with 32-bit size fields" do
    list = Crabbit::AMQP::Value.list(Array.new(130) { "value" })
    map_values = {} of Crabbit::AMQP::Value => Crabbit::AMQP::Value
    130.times do |index|
      map_values[Crabbit::AMQP::Value.wrap("key-#{index}")] = Crabbit::AMQP::Value.wrap(index)
    end
    map = Crabbit::AMQP::Value.new(Crabbit::AMQP::Kind::Map, map_values)
    array = Crabbit::AMQP::Value.array(Array.new(300) { |index| index.to_u32 })

    {
      list  => Crabbit::AMQP::TypeCode::List32,
      map   => Crabbit::AMQP::TypeCode::Map32,
      array => Crabbit::AMQP::TypeCode::Array32,
    }.each do |value, expected_code|
      io = IO::Memory.new
      Crabbit::AMQP::Encoder.encode(value, io)
      encoded = io.to_slice

      encoded.first.should eq expected_code
      decoder = Crabbit::AMQP::Decoder.new(encoded)
      decoder.read.should eq value
      decoder.eof?.should be_true
    end
  end

  it "rejects hostile container counts before allocating" do
    bytes = Bytes[
      Crabbit::AMQP::TypeCode::List32,
      0_u8, 0_u8, 0_u8, 4_u8,
      0_u8, 0x0f_u8, 0x42_u8, 0x41_u8,
    ]
    expect_raises(Crabbit::CodecError, /container count/) do
      Crabbit::AMQP::Decoder.new(bytes).read
    end
  end

  it "rejects non-ASCII symbols inside arrays" do
    value = Crabbit::AMQP::Value.new(
      Crabbit::AMQP::Kind::Array,
      [Crabbit::AMQP::Value.symbol("λ")],
    )

    expect_raises(Crabbit::CodecError, /ASCII/) do
      Crabbit::AMQP::Encoder.encode(value)
    end
  end
end
