module Crabbit::AMQP
  # :nodoc:
  module SectionDescriptor
    Header                = 0x70_u64
    DeliveryAnnotations   = 0x71_u64
    MessageAnnotations    = 0x72_u64
    Properties            = 0x73_u64
    ApplicationProperties = 0x74_u64
    Data                  = 0x75_u64
    Sequence              = 0x76_u64
    Value                 = 0x77_u64
    Footer                = 0x78_u64
  end

  struct Encoder
    # :nodoc:
    def write_described_header(descriptor : UInt64, header : Crabbit::Header) : self
      count = if header.delivery_count
                5
              elsif !header.first_acquirer.nil?
                4
              elsif header.ttl
                3
              elsif header.priority
                2
              elsif !header.durable.nil?
                1
              else
                0
              end

      payload_size = 0_i64
      payload_size += 1_i64 if count >= 1
      payload_size += optional_ubyte_size(header.priority) if count >= 2
      payload_size += optional_uint_size(header.ttl) if count >= 3
      payload_size += 1_i64 if count >= 4
      payload_size += optional_uint_size(header.delivery_count) if count >= 5

      write_descriptor(descriptor)
      write_known_list_header(count, payload_size)
      write_optional_boolean(header.durable) if count >= 1
      write_optional_ubyte(header.priority) if count >= 2
      write_optional_uint(header.ttl) if count >= 3
      write_optional_boolean(header.first_acquirer) if count >= 4
      write_optional_uint(header.delivery_count) if count >= 5
      self
    end

    # :nodoc:
    def write_described_properties(descriptor : UInt64, properties : Crabbit::Properties) : self
      count = if properties.reply_to_group_id
                13
              elsif properties.group_sequence
                12
              elsif properties.group_id
                11
              elsif properties.creation_time
                10
              elsif properties.absolute_expiry_time
                9
              elsif properties.content_encoding
                8
              elsif properties.content_type
                7
              elsif properties.correlation_id
                6
              elsif properties.reply_to
                5
              elsif properties.subject
                4
              elsif properties.to
                3
              elsif properties.user_id
                2
              elsif properties.message_id
                1
              else
                0
              end

      payload_size = 0_i64
      payload_size += identifier_size(properties.message_id) if count >= 1
      payload_size += optional_binary_size(properties.user_id) if count >= 2
      payload_size += optional_string_size(properties.to) if count >= 3
      payload_size += optional_string_size(properties.subject) if count >= 4
      payload_size += optional_string_size(properties.reply_to) if count >= 5
      payload_size += identifier_size(properties.correlation_id) if count >= 6
      payload_size += optional_symbol_size(properties.content_type) if count >= 7
      payload_size += optional_symbol_size(properties.content_encoding) if count >= 8
      payload_size += optional_timestamp_size(properties.absolute_expiry_time) if count >= 9
      payload_size += optional_timestamp_size(properties.creation_time) if count >= 10
      payload_size += optional_string_size(properties.group_id) if count >= 11
      payload_size += optional_uint_size(properties.group_sequence) if count >= 12
      payload_size += optional_string_size(properties.reply_to_group_id) if count >= 13

      write_descriptor(descriptor)
      write_known_list_header(count, payload_size)
      write_identifier(properties.message_id) if count >= 1
      write_optional_binary(properties.user_id) if count >= 2
      write_optional_string(properties.to) if count >= 3
      write_optional_string(properties.subject) if count >= 4
      write_optional_string(properties.reply_to) if count >= 5
      write_identifier(properties.correlation_id) if count >= 6
      write_optional_symbol(properties.content_type) if count >= 7
      write_optional_symbol(properties.content_encoding) if count >= 8
      write_optional_timestamp(properties.absolute_expiry_time) if count >= 9
      write_optional_timestamp(properties.creation_time) if count >= 10
      write_optional_string(properties.group_id) if count >= 11
      write_optional_uint(properties.group_sequence) if count >= 12
      write_optional_string(properties.reply_to_group_id) if count >= 13
      self
    end

    private def write_known_list_header(count : Int32, payload_size : Int64) : Nil
      if count == 0
        u8(TypeCode::List0)
      elsif (size8 = 1_i64 + payload_size) <= UInt8::MAX
        u8(TypeCode::List8)
        u8(size8.to_u8)
        u8(count.to_u8)
      else
        u8(TypeCode::List32)
        u32((4_i64 + payload_size).to_u32)
        u32(count.to_u32)
      end
    end

    private def optional_ubyte_size(value : UInt8?) : Int64
      value.nil? ? 1_i64 : 2_i64
    end

    private def optional_uint_size(value : UInt32?) : Int64
      return 1_i64 unless value
      value == 0 ? 1_i64 : (value <= UInt8::MAX ? 2_i64 : 5_i64)
    end

    private def identifier_size(value : Crabbit::Properties::Identifier?) : Int64
      case value
      when Nil    then 1_i64
      when String then variable_size(value.bytesize)
      when UInt64
        value == 0 ? 1_i64 : (value <= UInt8::MAX ? 2_i64 : 9_i64)
      when UUID  then 17_i64
      when Bytes then variable_size(value.size)
      else
        raise CodecError.new("unsupported AMQP message identifier type #{value.class}")
      end
    end

    private def optional_binary_size(value : Bytes?) : Int64
      value ? variable_size(value.size) : 1_i64
    end

    private def optional_string_size(value : String?) : Int64
      value ? variable_size(value.bytesize) : 1_i64
    end

    private def optional_symbol_size(value : String?) : Int64
      value ? variable_size(validated_symbol_bytes(value).size) : 1_i64
    end

    private def optional_timestamp_size(value : Time?) : Int64
      value ? 9_i64 : 1_i64
    end

    private def write_optional_boolean(value : Bool?) : Nil
      value.nil? ? u8(TypeCode::Null) : u8(value ? TypeCode::BoolTrue : TypeCode::BoolFalse)
    end

    private def write_optional_ubyte(value : UInt8?) : Nil
      if value
        u8(TypeCode::UByte)
        u8(value)
      else
        u8(TypeCode::Null)
      end
    end

    private def write_optional_uint(value : UInt32?) : Nil
      value ? write_uint(value) : u8(TypeCode::Null)
    end

    private def write_identifier(value : Crabbit::Properties::Identifier?) : Nil
      case value
      when Nil
        u8(TypeCode::Null)
      when String
        write_string(value, symbol: false)
      when UInt64
        write_ulong(value)
      when UUID
        u8(TypeCode::UUID)
        raw(value.bytes.to_slice)
      when Bytes
        write_binary(value)
      end
    end

    private def write_optional_binary(value : Bytes?) : Nil
      value ? write_binary(value) : u8(TypeCode::Null)
    end

    private def write_optional_string(value : String?) : Nil
      value ? write_string(value, symbol: false) : u8(TypeCode::Null)
    end

    private def write_optional_symbol(value : String?) : Nil
      value ? write_string(value, symbol: true) : u8(TypeCode::Null)
    end

    private def write_optional_timestamp(value : Time?) : Nil
      if value
        u8(TypeCode::Timestamp)
        i64(value.to_unix_ms)
      else
        u8(TypeCode::Null)
      end
    end
  end

  struct Decoder
    # :nodoc:
    def read_header : Crabbit::Header
      count, boundary = read_composite_header
      durable = count >= 1 ? read_optional_boolean_field(0) : nil
      priority = count >= 2 ? read_optional_ubyte_field(1) : nil
      ttl = count >= 3 ? read_optional_uint_field(2) : nil
      first_acquirer = count >= 4 ? read_optional_boolean_field(3) : nil
      delivery_count = count >= 5 ? read_optional_uint_field(4) : nil
      (count - 5).times { read } if count > 5
      finish_container!(boundary)

      Crabbit::Header.new(
        durable: durable,
        priority: priority,
        ttl: ttl,
        first_acquirer: first_acquirer,
        delivery_count: delivery_count,
      )
    end

    # :nodoc:
    def read_properties : Crabbit::Properties
      count, boundary = read_composite_header
      message_id = count >= 1 ? read_identifier_field(0) : nil
      user_id = count >= 2 ? read_optional_binary_field(1) : nil
      to = count >= 3 ? read_optional_string_field(2) : nil
      subject = count >= 4 ? read_optional_string_field(3) : nil
      reply_to = count >= 5 ? read_optional_string_field(4) : nil
      correlation_id = count >= 6 ? read_identifier_field(5) : nil
      content_type = count >= 7 ? read_optional_symbol_field(6) : nil
      content_encoding = count >= 8 ? read_optional_symbol_field(7) : nil
      absolute_expiry_time = count >= 9 ? read_optional_timestamp_field(8) : nil
      creation_time = count >= 10 ? read_optional_timestamp_field(9) : nil
      group_id = count >= 11 ? read_optional_string_field(10) : nil
      group_sequence = count >= 12 ? read_optional_uint_field(11) : nil
      reply_to_group_id = count >= 13 ? read_optional_string_field(12) : nil
      (count - 13).times { read } if count > 13
      finish_container!(boundary)

      Crabbit::Properties.new(
        message_id: message_id,
        user_id: user_id,
        to: to,
        subject: subject,
        reply_to: reply_to,
        correlation_id: correlation_id,
        content_type: content_type,
        content_encoding: content_encoding,
        absolute_expiry_time: absolute_expiry_time,
        creation_time: creation_time,
        group_id: group_id,
        group_sequence: group_sequence,
        reply_to_group_id: reply_to_group_id,
      )
    end

    # :nodoc:
    def read_application_properties : Hash(String, Value)
      count, boundary = read_map_header
      result = {} of String => Value
      (count // 2).times do
        result[read_required_string_key] = read
      end
      finish_container!(boundary)
      result
    end

    private def read_composite_header : Tuple(Int32, Int32)
      case code = u8
      when TypeCode::List0
        {0, position}
      when TypeCode::List8
        boundary = container_boundary(u8.to_i32)
        count = u8.to_i32
        validate_container_count!(count)
        {count, boundary}
      when TypeCode::List32
        boundary = container_boundary(length32)
        count = length32
        validate_container_count!(count)
        {count, boundary}
      else
        raise CodecError.new("expected AMQP list, got type code 0x#{code.to_s(16)}")
      end
    end

    private def read_map_header : Tuple(Int32, Int32)
      case code = u8
      when TypeCode::Map8
        boundary = container_boundary(u8.to_i32)
        count = u8.to_i32
      when TypeCode::Map32
        boundary = container_boundary(length32)
        count = length32
      else
        raise CodecError.new("expected AMQP map, got type code 0x#{code.to_s(16)}")
      end
      validate_container_count!(count)
      raise CodecError.new("AMQP map element count must be even") unless count.even?
      {count, boundary}
    end

    private def read_optional_boolean_field(index : Int32) : Bool?
      case code = u8
      when TypeCode::Null      then nil
      when TypeCode::BoolTrue  then true
      when TypeCode::BoolFalse then false
      when TypeCode::Boolean   then u8 != 0
      else                          invalid_field_type(index, "Boolean", code)
      end
    end

    private def read_optional_ubyte_field(index : Int32) : UInt8?
      case code = u8
      when TypeCode::Null  then nil
      when TypeCode::UByte then u8
      else                      invalid_field_type(index, "UByte", code)
      end
    end

    private def read_optional_uint_field(index : Int32) : UInt32?
      case code = u8
      when TypeCode::Null      then nil
      when TypeCode::UInt0     then 0_u32
      when TypeCode::SmallUInt then u8.to_u32
      when TypeCode::UInt      then u32
      else                          invalid_field_type(index, "UInt", code)
      end
    end

    private def read_identifier_field(index : Int32) : Crabbit::Properties::Identifier?
      case code = u8
      when TypeCode::Null       then nil
      when TypeCode::String8    then string(u8.to_i32)
      when TypeCode::String32   then string(length32)
      when TypeCode::ULong0     then 0_u64
      when TypeCode::SmallULong then u8.to_u64
      when TypeCode::ULong      then u64
      when TypeCode::UUID       then UUID.from_slice(raw(16))
      when TypeCode::Binary8    then binary(u8.to_i32)
      when TypeCode::Binary32   then binary(length32)
      else
        invalid_field_type(index, "message identifier", code)
      end
    end

    private def read_optional_binary_field(index : Int32) : Bytes?
      case code = u8
      when TypeCode::Null     then nil
      when TypeCode::Binary8  then binary(u8.to_i32)
      when TypeCode::Binary32 then binary(length32)
      else                         invalid_field_type(index, "Binary", code)
      end
    end

    private def read_optional_string_field(index : Int32) : String?
      case code = u8
      when TypeCode::Null     then nil
      when TypeCode::String8  then string(u8.to_i32)
      when TypeCode::String32 then string(length32)
      else                         invalid_field_type(index, "String", code)
      end
    end

    private def read_optional_symbol_field(index : Int32) : String?
      case code = u8
      when TypeCode::Null     then nil
      when TypeCode::Symbol8  then symbol(u8.to_i32)
      when TypeCode::Symbol32 then symbol(length32)
      else                         invalid_field_type(index, "Symbol", code)
      end
    end

    private def read_optional_timestamp_field(index : Int32) : Time?
      case code = u8
      when TypeCode::Null      then nil
      when TypeCode::Timestamp then Time.unix_ms(i64)
      else                          invalid_field_type(index, "Timestamp", code)
      end
    end

    private def read_required_string_key : String
      case code = u8
      when TypeCode::String8  then string(u8.to_i32)
      when TypeCode::String32 then string(length32)
      else
        raise CodecError.new("application-properties key must be a string, got type code 0x#{code.to_s(16)}")
      end
    end

    private def invalid_field_type(index : Int32, expected : String, code : UInt8) : NoReturn
      raise CodecError.new("field #{index} must be #{expected}, got type code 0x#{code.to_s(16)}")
    end
  end

  # Encoder and decoder for complete AMQP 1.0 messages.
  #
  # Applications generally use `Crabbit::Message#to_amqp` and
  # `Crabbit::Message.from_amqp`, which delegate here.
  module MessageCodec
    extend self

    # Encodes *bytes* as a complete message containing one AMQP Data section.
    #
    # This fast path avoids constructing an intermediate `Crabbit::Message`.
    def encode_data(bytes : Bytes) : Bytes
      overhead = bytes.size <= UInt8::MAX ? 5 : 8
      io = IO::Memory.new(bytes.size + overhead)
      Encoder.new(io).write_described_binary(SectionDescriptor::Data, bytes)
      io.to_slice
    end

    # Encodes a complete *message* to a newly allocated byte slice.
    def encode(message : Message) : Bytes
      io = IO::Memory.new
      encode(message, io)
      io.to_slice
    end

    # Encodes a complete *message* directly into *io*.
    def encode(message : Message, io : IO) : Nil
      encoder = Encoder.new(io)
      if header = message.header
        encoder.write_described_header(SectionDescriptor::Header, header)
      end
      unless message.delivery_annotations.empty?
        encoder.write_described_map(SectionDescriptor::DeliveryAnnotations, message.delivery_annotations)
      end
      unless message.message_annotations.empty?
        encoder.write_described_map(SectionDescriptor::MessageAnnotations, message.message_annotations)
      end
      if properties = message.properties
        encoder.write_described_properties(SectionDescriptor::Properties, properties)
      end
      unless message.application_properties.empty?
        encoder.write_described_string_map(SectionDescriptor::ApplicationProperties, message.application_properties)
      end
      case message.body_kind
      when .data?
        message.data.each { |data| encoder.write_described_binary(SectionDescriptor::Data, data) }
      when .sequence?
        message.sequences.each { |sequence| encoder.write_described_list(SectionDescriptor::Sequence, sequence) }
      when .value?
        encoder.write_described(SectionDescriptor::Value, message.value || Value.null)
      end
      message.extra_sections.each { |value| encoder.write(value) }
      unless message.footer.empty?
        encoder.write_described_map(SectionDescriptor::Footer, message.footer)
      end
      nil
    end

    # Decodes one complete message from *bytes*.
    #
    # Binary values are copied unless *zero_copy* is true. Unknown described
    # sections are preserved in `Crabbit::Message#extra_sections`.
    def decode(bytes : Bytes, *, zero_copy : Bool = false) : Message
      decoder = Decoder.new(bytes, zero_copy: zero_copy)
      header = nil
      delivery_annotations : Hash(Value, Value)? = nil
      message_annotations : Hash(Value, Value)? = nil
      properties = nil
      application_properties : Hash(String, Value)? = nil
      data : Array(Bytes)? = nil
      sequences : Array(Array(Value))? = nil
      body_value = nil
      body_kind = nil
      footer : Hash(Value, Value)? = nil
      extras : Array(Value)? = nil

      until decoder.eof?
        descriptor = decoder.peek_described_descriptor
        case descriptor
        when SectionDescriptor::Header
          decoder.consume_described_descriptor(SectionDescriptor::Header)
          header = decoder.read_header
        when SectionDescriptor::DeliveryAnnotations
          decoder.consume_described_descriptor(SectionDescriptor::DeliveryAnnotations)
          delivery_annotations = decoder.read_map_values
        when SectionDescriptor::MessageAnnotations
          decoder.consume_described_descriptor(SectionDescriptor::MessageAnnotations)
          message_annotations = decoder.read_map_values
        when SectionDescriptor::Properties
          decoder.consume_described_descriptor(SectionDescriptor::Properties)
          properties = decoder.read_properties
        when SectionDescriptor::ApplicationProperties
          decoder.consume_described_descriptor(SectionDescriptor::ApplicationProperties)
          values = decoder.read_application_properties
          if current = application_properties
            current.merge!(values)
          else
            application_properties = values
          end
        when SectionDescriptor::Data
          decoder.consume_described_descriptor(SectionDescriptor::Data)
          enforce_body_kind!(body_kind, BodyKind::Data)
          body_kind = BodyKind::Data
          (data ||= [] of Bytes) << decoder.read_binary
        when SectionDescriptor::Sequence
          decoder.consume_described_descriptor(SectionDescriptor::Sequence)
          enforce_body_kind!(body_kind, BodyKind::Sequence)
          body_kind = BodyKind::Sequence
          (sequences ||= [] of Array(Value)) << decoder.read_list_values
        when SectionDescriptor::Value
          decoder.consume_described_descriptor(SectionDescriptor::Value)
          enforce_body_kind!(body_kind, BodyKind::Value)
          raise CodecError.new("message can contain only one AMQP value body") if body_value
          body_kind = BodyKind::Value
          body_value = decoder.read
        when SectionDescriptor::Footer
          decoder.consume_described_descriptor(SectionDescriptor::Footer)
          footer = decoder.read_map_values
        else
          (extras ||= [] of Value) << decoder.read
        end
      end

      kind = body_kind || BodyKind::Data
      Message.decoded(
        body_kind: kind,
        data: data,
        sequences: sequences,
        value: body_value,
        header: header,
        delivery_annotations: delivery_annotations,
        message_annotations: message_annotations,
        properties: properties,
        application_properties: application_properties,
        footer: footer,
        extra_sections: extras,
        encoded_owner: zero_copy ? bytes : nil,
      )
    end

    private def enforce_body_kind!(current : BodyKind?, incoming : BodyKind) : Nil
      if current && current != incoming
        raise CodecError.new("AMQP message mixes #{current} and #{incoming} body sections")
      end
      if current == BodyKind::Value
        raise CodecError.new("AMQP value body must contain exactly one section")
      end
    end
  end
end
