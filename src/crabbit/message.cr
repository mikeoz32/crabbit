module Crabbit
  # AMQP 1.0 header section.
  #
  # All fields are optional. Values left as `nil` are omitted from the trailing
  # portion of the encoded composite where possible.
  class Header
    # Controls whether the message should survive node failure.
    property durable : Bool?
    # Indicates relative message priority.
    property priority : UInt8?
    # Specifies time-to-live in milliseconds.
    property ttl : UInt32?
    # Indicates whether the message was acquired by its first consumer.
    property first_acquirer : Bool?
    # Counts prior unsuccessful delivery attempts.
    property delivery_count : UInt32?

    # Creates an AMQP header section.
    def initialize(
      @durable : Bool? = nil,
      @priority : UInt8? = nil,
      @ttl : UInt32? = nil,
      @first_acquirer : Bool? = nil,
      @delivery_count : UInt32? = nil,
    )
    end
  end

  # AMQP 1.0 properties section containing standard message metadata.
  class Properties
    # Supported AMQP message-id and correlation-id representations.
    alias Identifier = String | UInt64 | AMQP::UUID | Bytes

    # Application-defined stable message identifier.
    property message_id : Identifier?
    # Authenticated user identity represented as AMQP binary.
    property user_id : Bytes?
    # Destination address.
    property to : String?
    # Message subject.
    property subject : String?
    # Reply destination.
    property reply_to : String?
    # Identifier used to correlate related messages.
    property correlation_id : Identifier?
    # MIME content type encoded as an AMQP symbol.
    property content_type : String?
    # MIME content encoding encoded as an AMQP symbol.
    property content_encoding : String?
    # Absolute message expiry time.
    property absolute_expiry_time : Time?
    # Message creation time.
    property creation_time : Time?
    # Application-defined group identifier.
    property group_id : String?
    # Sequence number within `#group_id`.
    property group_sequence : UInt32?
    # Group identifier for replies.
    property reply_to_group_id : String?

    # Creates an AMQP properties section.
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

  # Body section family represented by a `Message`.
  enum BodyKind
    # One or more AMQP Data sections.
    Data
    # One or more AMQP Sequence sections.
    Sequence
    # A single AMQP Value section.
    Value
  end

  # Complete AMQP 1.0 message used as a RabbitMQ Stream payload.
  #
  # A message can contain header, annotations, properties, application
  # properties, one body family, footer, and unknown described sections.
  # `Message.new` creates a Data-body message; use `.data`, `.sequence`, or
  # `.value` for explicit body construction.
  #
  # ```
  # message = Crabbit::Message.new(
  #   "invoice-created",
  #   properties: Crabbit::Properties.new(content_type: "text/plain"),
  #   application_properties: {
  #     "region" => Crabbit::AMQP::Value.wrap("eu"),
  #   },
  # )
  # ```
  class Message
    # Returns the optional AMQP header.
    getter header : Header?
    # Returns the optional AMQP properties section.
    getter properties : Properties?
    # Returns the encoded body family.
    getter body_kind : BodyKind
    # Returns the AMQP Value body, or `nil` for Data and Sequence bodies.
    getter value : AMQP::Value?

    @delivery_annotations : Hash(AMQP::Value, AMQP::Value)?
    @message_annotations : Hash(AMQP::Value, AMQP::Value)?
    @application_properties : Hash(String, AMQP::Value)?
    @data : Array(Bytes)?
    @sequences : Array(Array(AMQP::Value))?
    @footer : Hash(AMQP::Value, AMQP::Value)?
    @extra_sections : Array(AMQP::Value)?
    @encoded_owner : Bytes?

    # Creates a message with one AMQP Data section.
    #
    # The body bytes are copied so later mutation of the caller's buffer does
    # not affect the message.
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

    # :nodoc:
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

    # Returns the mutable delivery-annotations map, creating it when absent.
    def delivery_annotations : Hash(AMQP::Value, AMQP::Value)
      @delivery_annotations ||= {} of AMQP::Value => AMQP::Value
    end

    # Returns the mutable message-annotations map, creating it when absent.
    def message_annotations : Hash(AMQP::Value, AMQP::Value)
      @message_annotations ||= {} of AMQP::Value => AMQP::Value
    end

    # Returns the mutable application-properties map, creating it when absent.
    def application_properties : Hash(String, AMQP::Value)
      @application_properties ||= {} of String => AMQP::Value
    end

    # Returns the mutable list of AMQP Data section payloads.
    def data : Array(Bytes)
      @data ||= [] of Bytes
    end

    # Returns the mutable list of AMQP Sequence section values.
    def sequences : Array(Array(AMQP::Value))
      @sequences ||= [] of Array(AMQP::Value)
    end

    # Returns the mutable footer map, creating it when absent.
    def footer : Hash(AMQP::Value, AMQP::Value)
      @footer ||= {} of AMQP::Value => AMQP::Value
    end

    # Returns unknown described sections preserved during decoding.
    #
    # Values added here are encoded after the standard message sections.
    def extra_sections : Array(AMQP::Value)
      @extra_sections ||= [] of AMQP::Value
    end

    # Encodes the complete message to a newly allocated AMQP byte slice.
    def to_amqp : Bytes
      AMQP::MessageCodec.encode(self)
    end

    # Encodes the complete message directly into *io*.
    #
    # This overload avoids allocating an intermediate encoded `Bytes` buffer.
    def to_amqp(io : IO) : Nil
      AMQP::MessageCodec.encode(self, io)
    end

    # Decodes one complete AMQP message from *bytes*.
    #
    # Binary values are copied by default. With *zero_copy*, Data sections and
    # AMQP binary values may reference *bytes* directly; the caller must keep
    # that buffer alive and must not mutate it while the message is used.
    def self.from_amqp(bytes : Bytes, *, zero_copy : Bool = false) : self
      AMQP::MessageCodec.decode(bytes, zero_copy: zero_copy)
    end

    # Creates a message containing one Data section for every entry in *parts*.
    #
    # Each byte slice is copied.
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

    # Creates a message containing a single AMQP Sequence section.
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

    # Creates a message containing a single AMQP Value section.
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

    # Returns the logical Data body as bytes.
    #
    # A single Data part is returned without copying. Multiple parts are joined
    # into a new slice. Non-Data messages return `Bytes.empty`.
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

  # Already encoded AMQP 1.0 message bytes.
  #
  # Publishing a `RawMessage` bypasses AMQP encoding. The bytes must contain a
  # valid complete AMQP message, not merely an application payload.
  class RawMessage
    # Returns the encoded AMQP message bytes.
    getter bytes : Bytes

    # Creates a raw message and copies *bytes* by default.
    #
    # With *copy* set to `false`, the caller owns the buffer lifetime and must
    # not mutate it until publishing has completed.
    def initialize(bytes : Bytes, copy : Bool = true)
      @bytes = copy ? bytes.dup : bytes
    end
  end
end
