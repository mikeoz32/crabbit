require "../spec_helper"

describe Crabbit::AMQP::MessageCodec do
  it "encodes a bare data section without changing its AMQP representation" do
    [Bytes[1, 2, 3], Bytes.new(1_024, 0x61_u8)].each do |payload|
      Crabbit::AMQP::MessageCodec.encode_data(payload).should eq Crabbit::Message.new(payload).to_amqp
    end
  end

  it "round-trips complete AMQP messages" do
    annotations = {
      Crabbit::AMQP::Value.symbol("x-stream-filter-value") => Crabbit::AMQP::Value.wrap("invoices"),
    }
    application_properties = {
      "attempt" => Crabbit::AMQP::Value.wrap(3_i32),
      "active"  => Crabbit::AMQP::Value.wrap(true),
    }
    message = Crabbit::Message.new(
      "hello",
      header: Crabbit::Header.new(durable: true, priority: 5_u8),
      message_annotations: annotations,
      properties: Crabbit::Properties.new(
        message_id: "id-1",
        subject: "created",
        content_type: "application/json",
        creation_time: Time.unix_ms(1_700_000_000_000_i64),
      ),
      application_properties: application_properties,
    )

    decoded = Crabbit::Message.from_amqp(message.to_amqp)
    decoded.body.should eq "hello".to_slice
    decoded.header.not_nil!.durable.should be_true
    decoded.header.not_nil!.priority.should eq 5_u8
    decoded.properties.not_nil!.message_id.should eq "id-1"
    decoded.properties.not_nil!.content_type.should eq "application/json"
    decoded.message_annotations.should eq annotations
    decoded.application_properties.should eq application_properties

    io = IO::Memory.new
    message.to_amqp(io)
    io.to_slice.should eq message.to_amqp
  end

  it "preserves unknown described sections" do
    unknown = Crabbit::AMQP::Value.described(0x1234_u64, Crabbit::AMQP::Value.wrap("opaque"))
    encoded = Crabbit::AMQP::Encoder.encode(unknown)
    decoded = Crabbit::Message.from_amqp(encoded)
    decoded.extra_sections.should eq [unknown]
    decoded.to_amqp.should eq encoded
  end

  it "preserves false optional header fields" do
    message = Crabbit::Message.new(
      "header",
      header: Crabbit::Header.new(durable: false, first_acquirer: false),
    )

    header = Crabbit::Message.from_amqp(message.to_amqp).header.not_nil!
    header.durable.should be_false
    header.first_acquirer.should be_false
  end

  it "copies binary bodies by default and supports explicit zero-copy decoding" do
    encoded = Crabbit::Message.new(Bytes[1, 2, 3]).to_amqp
    copied = Crabbit::Message.from_amqp(encoded)
    shared = Crabbit::Message.from_amqp(encoded, zero_copy: true)

    encoded[encoded.size - 1] = 9_u8
    copied.body.should eq Bytes[1, 2, 3]
    shared.body.should eq Bytes[1, 2, 9]
  end

  it "decodes every header and properties field directly" do
    uuid = Crabbit::AMQP::UUID.from_slice(Bytes.new(16) { |index| index.to_u8 })
    message = Crabbit::Message.new(
      "complete",
      header: Crabbit::Header.new(
        durable: false,
        priority: 7_u8,
        ttl: 30_000_u32,
        first_acquirer: false,
        delivery_count: 12_u32,
      ),
      properties: Crabbit::Properties.new(
        message_id: 42_u64,
        user_id: Bytes[1, 2, 3],
        to: "orders",
        subject: "created",
        reply_to: "replies",
        correlation_id: uuid,
        content_type: "application/json",
        content_encoding: "utf-8",
        absolute_expiry_time: Time.unix_ms(1_700_000_100_000_i64),
        creation_time: Time.unix_ms(1_700_000_000_000_i64),
        group_id: "group-1",
        group_sequence: 9_u32,
        reply_to_group_id: "reply-group",
      ),
    )

    decoded = Crabbit::Message.from_amqp(message.to_amqp, zero_copy: true)
    header = decoded.header.not_nil!
    header.durable.should be_false
    header.priority.should eq 7_u8
    header.ttl.should eq 30_000_u32
    header.first_acquirer.should be_false
    header.delivery_count.should eq 12_u32

    properties = decoded.properties.not_nil!
    properties.message_id.should eq 42_u64
    properties.user_id.should eq Bytes[1, 2, 3]
    properties.to.should eq "orders"
    properties.subject.should eq "created"
    properties.reply_to.should eq "replies"
    properties.correlation_id.should eq uuid
    properties.content_type.should eq "application/json"
    properties.content_encoding.should eq "utf-8"
    properties.absolute_expiry_time.should eq Time.unix_ms(1_700_000_100_000_i64)
    properties.creation_time.should eq Time.unix_ms(1_700_000_000_000_i64)
    properties.group_id.should eq "group-1"
    properties.group_sequence.should eq 9_u32
    properties.reply_to_group_id.should eq "reply-group"
  end
end
