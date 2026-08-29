module Crabbit
  class Header
    property durable : Bool?
    property priority : UInt8?
    property ttl : UInt32?
    property first_acquirer : Bool?
    property delivery_count : UInt32?

    def initialize(
      @durable : Bool? = nil,
      @priority : UInt8? = nil,
      @ttl : UInt32? = nil,
      @first_acquirer : Bool? = nil,
      @delivery_count : UInt32? = nil,
    )
    end
  end

  class Properties
    alias Identifier = String | UInt64 | AMQP::UUID | Bytes

    property message_id : Identifier?
    property user_id : Bytes?
    property to : String?
    property subject : String?
    property reply_to : String?
    property correlation_id : Identifier?
    property content_type : String?
    property content_encoding : String?
    property absolute_expiry_time : Time?
    property creation_time : Time?
    property group_id : String?
    property group_sequence : UInt32?
    property reply_to_group_id : String?

    def initialize(
      @message_id : Identifier? = nil,
      @user_id : Bytes? = nil,
      @to : String? = nil,
      @subject : String? = nil,
      @reply_to : String? = nil,
      @correlation_id : Identifier? = nil,
      @content_type : String? = nil,
      @content_encoding : String? = nil,
      @absolute_expiry_time : Time? = nil,
      @creation_time : Time? = nil,
      @group_id : String? = nil,
      @group_sequence : UInt32? = nil,
      @reply_to_group_id : String? = nil,
    )
    end
  end

  enum BodyKind
    Data
    Sequence
    Value
  end

  class Message
    getter header : Header?
    getter properties : Properties?
    getter body_kind : BodyKind
    getter value : AMQP::Value?

    @delivery_annotations : Hash(AMQP::Value, AMQP::Value)?
    @message_annotations : Hash(AMQP::Value, AMQP::Value)?
    @application_properties : Hash(String, AMQP::Value)?
    @data : Array(Bytes)?
    @sequences : Array(Array(AMQP::Value))?
    @footer : Hash(AMQP::Value, AMQP::Value)?
    @extra_sections : Array(AMQP::Value)?
    @encoded_owner : Bytes?

    def initialize(
      body : Bytes | String = Bytes.empty,
      @header : Header? = nil,
      @delivery_annotations : Hash(AMQP::Value, AMQP::Value)? = nil,
      @message_annotations : Hash(AMQP::Value, AMQP::Value)? = nil,
      @properties : Properties? = nil,
      @application_properties : Hash(String, AMQP::Value)? = nil,
      @footer : Hash(AMQP::Value, AMQP::Value)? = nil,
    )
      @encoded_owner = nil
      @body_kind = BodyKind::Data
      bytes = body.is_a?(String) ? body.to_slice : body
      @data = [bytes.dup]
      @sequences = nil
      @value = nil
      @extra_sections = nil
    end

    protected def initialize(
      @body_kind : BodyKind,
      @data : Array(Bytes)?,
      @sequences : Array(Array(AMQP::Value))?,
      @value : AMQP::Value?,
      @header : Header? = nil,
      @delivery_annotations : Hash(AMQP::Value, AMQP::Value)? = nil,
      @message_annotations : Hash(AMQP::Value, AMQP::Value)? = nil,
      @properties : Properties? = nil,
      @application_properties : Hash(String, AMQP::Value)? = nil,
      @footer : Hash(AMQP::Value, AMQP::Value)? = nil,
      @extra_sections : Array(AMQP::Value)? = nil,
      @encoded_owner : Bytes? = nil,
    )
    end

    def self.decoded(
      *,
      body_kind : BodyKind,
      data : Array(Bytes)?,
      sequences : Array(Array(AMQP::Value))?,
      value : AMQP::Value?,
      header : Header?,
      delivery_annotations : Hash(AMQP::Value, AMQP::Value)?,
      message_annotations : Hash(AMQP::Value, AMQP::Value)?,
      properties : Properties?,
      application_properties : Hash(String, AMQP::Value)?,
      footer : Hash(AMQP::Value, AMQP::Value)?,
      extra_sections : Array(AMQP::Value)?,
      encoded_owner : Bytes? = nil,
    ) : self
      new(
        body_kind,
        data,
        sequences,
        value,
        header,
        delivery_annotations,
        message_annotations,
        properties,
        application_properties,
        footer,
        extra_sections,
        encoded_owner,
      )
    end

    def delivery_annotations : Hash(AMQP::Value, AMQP::Value)
      @delivery_annotations ||= {} of AMQP::Value => AMQP::Value
    end

    def message_annotations : Hash(AMQP::Value, AMQP::Value)
      @message_annotations ||= {} of AMQP::Value => AMQP::Value
    end

    def application_properties : Hash(String, AMQP::Value)
      @application_properties ||= {} of String => AMQP::Value
    end

    def data : Array(Bytes)
      @data ||= [] of Bytes
    end

    def sequences : Array(Array(AMQP::Value))
      @sequences ||= [] of Array(AMQP::Value)
    end

    def footer : Hash(AMQP::Value, AMQP::Value)
      @footer ||= {} of AMQP::Value => AMQP::Value
    end

    def extra_sections : Array(AMQP::Value)
      @extra_sections ||= [] of AMQP::Value
    end

    def to_amqp : Bytes
      AMQP::MessageCodec.encode(self)
    end

    def to_amqp(io : IO) : Nil
      AMQP::MessageCodec.encode(self, io)
    end

    def self.from_amqp(bytes : Bytes, *, zero_copy : Bool = false) : self
      AMQP::MessageCodec.decode(bytes, zero_copy: zero_copy)
    end

    def self.data(
      parts : Enumerable(Bytes),
      *,
      header : Header? = nil,
      delivery_annotations : Hash(AMQP::Value, AMQP::Value) = {} of AMQP::Value => AMQP::Value,
      message_annotations : Hash(AMQP::Value, AMQP::Value) = {} of AMQP::Value => AMQP::Value,
      properties : Properties? = nil,
      application_properties : Hash(String, AMQP::Value) = {} of String => AMQP::Value,
      footer : Hash(AMQP::Value, AMQP::Value) = {} of AMQP::Value => AMQP::Value,
      extra_sections : Array(AMQP::Value) = [] of AMQP::Value,
    ) : self
      new(
        BodyKind::Data,
        parts.map(&.dup).to_a,
        [] of Array(AMQP::Value),
        nil,
        header,
        delivery_annotations,
        message_annotations,
        properties,
        application_properties,
        footer,
        extra_sections,
      )
    end

    def self.sequence(
      values : Array(AMQP::Value),
      *,
      header : Header? = nil,
      delivery_annotations : Hash(AMQP::Value, AMQP::Value) = {} of AMQP::Value => AMQP::Value,
      message_annotations : Hash(AMQP::Value, AMQP::Value) = {} of AMQP::Value => AMQP::Value,
      properties : Properties? = nil,
      application_properties : Hash(String, AMQP::Value) = {} of String => AMQP::Value,
      footer : Hash(AMQP::Value, AMQP::Value) = {} of AMQP::Value => AMQP::Value,
      extra_sections : Array(AMQP::Value) = [] of AMQP::Value,
    ) : self
      new(
        BodyKind::Sequence,
        [] of Bytes,
        [values],
        nil,
        header,
        delivery_annotations,
        message_annotations,
        properties,
        application_properties,
        footer,
        extra_sections,
      )
    end

    def self.value(
      value : AMQP::Value,
      *,
      header : Header? = nil,
      delivery_annotations : Hash(AMQP::Value, AMQP::Value) = {} of AMQP::Value => AMQP::Value,
      message_annotations : Hash(AMQP::Value, AMQP::Value) = {} of AMQP::Value => AMQP::Value,
      properties : Properties? = nil,
      application_properties : Hash(String, AMQP::Value) = {} of String => AMQP::Value,
      footer : Hash(AMQP::Value, AMQP::Value) = {} of AMQP::Value => AMQP::Value,
      extra_sections : Array(AMQP::Value) = [] of AMQP::Value,
    ) : self
      new(
        BodyKind::Value,
        [] of Bytes,
        [] of Array(AMQP::Value),
        value,
        header,
        delivery_annotations,
        message_annotations,
        properties,
        application_properties,
        footer,
        extra_sections,
      )
    end

    def body : Bytes
      return Bytes.empty unless body_kind.data?
      return data.first if data.size == 1

      size = data.sum(&.size)
      result = Bytes.new(size)
      position = 0
      data.each do |part|
        part.copy_to(result[position, part.size])
        position += part.size
      end
      result
    end
  end

  class RawMessage
    getter bytes : Bytes

    def initialize(bytes : Bytes, copy : Bool = true)
      @bytes = copy ? bytes.dup : bytes
    end
  end
end
