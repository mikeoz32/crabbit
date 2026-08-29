# :nodoc:
module Crabbit::Internal::Wire
  record Response, correlation_id : UInt32, code : ResponseCode, reader : Reader
  record PublishConfirmation, publisher_id : UInt8, publishing_ids : Array(UInt64)
  record PublishFailure, publisher_id : UInt8, errors : Hash(UInt64, ResponseCode)
  record MetadataUpdate, code : UInt16, stream : String
  record Tune, max_frame_size : UInt32, heartbeat_seconds : UInt32
  record ConsumerUpdate, correlation_id : UInt32, subscription_id : UInt8, active : Bool

  enum PublishPayloadFormat
    Encoded
    Data
  end

  record PublishEntry,
    publishing_id : UInt64,
    payload : Bytes,
    filter : String?,
    format : PublishPayloadFormat do
    def self.encoded(publishing_id : UInt64, payload : Bytes, filter : String? = nil) : self
      new(publishing_id, payload, filter, PublishPayloadFormat::Encoded)
    end

    def self.data(publishing_id : UInt64, payload : Bytes, filter : String? = nil) : self
      new(publishing_id, payload, filter, PublishPayloadFormat::Data)
    end

    def encoded_size : Int64
      payload.size.to_i64 + (format.data? ? (payload.size <= UInt8::MAX ? 5_i64 : 8_i64) : 0_i64)
    end

    def wire_size(version : UInt16) : Int64
      size = 12_i64 + encoded_size
      size += 2_i64 + (filter.try(&.bytesize) || 0) if version >= 2
      size
    end

    def write_payload(writer : Writer) : Nil
      if format.data?
        AMQP::Encoder.new(writer.io).write_described_binary(AMQP::SectionDescriptor::Data, payload)
      else
        writer.write_raw(payload)
      end
    end
  end

  module Commands
    extend self

    def peer_properties(correlation_id : UInt32, properties : Hash(String, String)) : Bytes
      FrameCodec.request(Command::PeerProperties, correlation_id) { |w| w.write_string_map(properties) }
    end

    def sasl_handshake(correlation_id : UInt32) : Bytes
      FrameCodec.request(Command::SaslHandshake, correlation_id) { }
    end

    def sasl_authenticate(correlation_id : UInt32, mechanism : String, opaque : Bytes?) : Bytes
      FrameCodec.request(Command::SaslAuthenticate, correlation_id) do |w|
        w.write_string(mechanism).write_bytes(opaque)
      end
    end

    def tune(max_frame_size : UInt32, heartbeat_seconds : UInt32) : Bytes
      FrameCodec.command(Command::Tune, response: true) do |w|
        w.write_u32(max_frame_size).write_u32(heartbeat_seconds)
      end
    end

    def open(correlation_id : UInt32, virtual_host : String) : Bytes
      FrameCodec.request(Command::Open, correlation_id) { |w| w.write_string(virtual_host) }
    end

    def close(correlation_id : UInt32, code : UInt16 = 1_u16, reason : String = "OK") : Bytes
      FrameCodec.request(Command::Close, correlation_id) { |w| w.write_u16(code).write_string(reason) }
    end

    def heartbeat : Bytes
      FrameCodec.empty_command(Command::Heartbeat)
    end

    def exchange_versions(correlation_id : UInt32, versions : Enumerable(CommandVersion)) : Bytes
      FrameCodec.request(Command::ExchangeCommandVersions, correlation_id) do |w|
        entries = versions.to_a
        w.write_i32(entries.size.to_i32)
        entries.each do |entry|
          w.write_u16(entry.key).write_u16(entry.min_version).write_u16(entry.max_version)
        end
      end
    end

    def declare_publisher(correlation_id : UInt32, publisher_id : UInt8, reference : String?, stream : String) : Bytes
      FrameCodec.request(Command::DeclarePublisher, correlation_id) do |w|
        w.write_u8(publisher_id).write_string(reference || "").write_string(stream)
      end
    end

    def delete_publisher(correlation_id : UInt32, publisher_id : UInt8) : Bytes
      FrameCodec.request(Command::DeletePublisher, correlation_id) { |w| w.write_u8(publisher_id) }
    end

    def query_publisher_sequence(correlation_id : UInt32, reference : String, stream : String) : Bytes
      FrameCodec.request(Command::QueryPublisherSequence, correlation_id) do |w|
        w.write_string(reference).write_string(stream)
      end
    end

    def publish(
      publisher_id : UInt8,
      messages : Array(Tuple(UInt64, Bytes, String?)),
      version : UInt16 = 1_u16,
    ) : Bytes
      if version == 1 && messages.any? { |entry| !entry[2].nil? }
        raise ProtocolError.new("publish filters require publish command version 2")
      end
      capacity = 13_i64
      messages.each do |_, message, filter|
        capacity += 12_i64 + message.size
        capacity += 2_i64 + (filter.try(&.bytesize) || 0) if version >= 2
      end
      raise ProtocolError.new("publish frame exceeds Int32 memory limit") if capacity > Int32::MAX

      FrameCodec.command(Command::Publish, version, initial_capacity: capacity.to_i32) do |w|
        w.write_u8(publisher_id).write_i32(messages.size.to_i32)
        messages.each do |publishing_id, message, filter|
          w.write_u64(publishing_id)
          w.write_string(filter) if version >= 2
          w.write_bytes(message)
        end
      end
    end

    def publish_frame_size(messages : Array(PublishEntry), version : UInt16 = 1_u16) : Int32
      validate_publish_filters!(messages, version)
      capacity = 13_i64
      messages.each do |entry|
        if entry.encoded_size > Int32::MAX
          raise ProtocolError.new("publish message exceeds Int32 protocol limit")
        end
        if filter = entry.filter
          if filter.bytesize > Int16::MAX
            raise ProtocolError.new("string exceeds Int16 protocol limit")
          end
        end
        capacity += entry.wire_size(version)
      end
      raise ProtocolError.new("publish frame exceeds Int32 memory limit") if capacity > Int32::MAX
      capacity.to_i32
    end

    # Writes a complete publish frame into caller-owned reusable memory.
    def write_publish(
      io : IO::Memory,
      publisher_id : UInt8,
      messages : Array(PublishEntry),
      version : UInt16 = 1_u16,
    ) : Int32
      frame_size = publish_frame_size(messages, version)
      write_publish(io, publisher_id, messages, version, frame_size)
      frame_size
    end

    def write_publish(
      io : IO::Memory,
      publisher_id : UInt8,
      messages : Array(PublishEntry),
      version : UInt16,
      frame_size : Int32,
    ) : Nil
      io.clear
      writer = Writer.new(io)
      writer.write_u32((frame_size - 4).to_u32)
        .write_u16(Command::Publish.value)
        .write_u16(version)
        .write_u8(publisher_id)
        .write_i32(messages.size.to_i32)
      messages.each do |entry|
        writer.write_u64(entry.publishing_id)
        writer.write_string(entry.filter) if version >= 2
        writer.write_i32(entry.encoded_size.to_i32)
        entry.write_payload(writer)
      end
      unless writer.size == frame_size
        raise ProtocolError.new("publish frame size changed while writing")
      end
    end

    def publish_sub_entries(
      publisher_id : UInt8,
      entries : Array(Tuple(UInt64, Internal::EncodedSubEntry)),
    ) : Bytes
      capacity = 13_i64 + entries.sum { |entry| 19_i64 + entry[1].data.size }
      raise ProtocolError.new("publish frame exceeds Int32 memory limit") if capacity > Int32::MAX

      FrameCodec.command(Command::Publish, initial_capacity: capacity.to_i32) do |w|
        w.write_u8(publisher_id).write_i32(entries.size.to_i32)
        entries.each do |publishing_id, entry|
          w.write_u64(publishing_id)
          w.write_u8(0x80_u8 | (entry.compression.value << 4))
          w.write_u16(entry.record_count)
          w.write_u32(entry.uncompressed_size)
          w.write_u32(entry.data.size.to_u32)
          w.write_raw(entry.data)
        end
      end
    end

    def subscribe(
      correlation_id : UInt32,
      subscription_id : UInt8,
      stream : String,
      offset : OffsetSpecification,
      credit : UInt16,
      properties : Hash(String, String),
    ) : Bytes
      FrameCodec.request(Command::Subscribe, correlation_id) do |w|
        w.write_u8(subscription_id).write_string(stream)
        write_offset(w, offset)
        w.write_u16(credit).write_string_map(properties)
      end
    end

    def credit(subscription_id : UInt8, value : UInt16) : Bytes
      FrameCodec.command(Command::Credit) { |w| w.write_u8(subscription_id).write_u16(value) }
    end

    def store_offset(reference : String, stream : String, offset : UInt64) : Bytes
      FrameCodec.command(Command::StoreOffset) do |w|
        w.write_string(reference).write_string(stream).write_u64(offset)
      end
    end

    def query_offset(correlation_id : UInt32, reference : String, stream : String) : Bytes
      FrameCodec.request(Command::QueryOffset, correlation_id) do |w|
        w.write_string(reference).write_string(stream)
      end
    end

    def unsubscribe(correlation_id : UInt32, subscription_id : UInt8) : Bytes
      FrameCodec.request(Command::Unsubscribe, correlation_id) { |w| w.write_u8(subscription_id) }
    end

    def create_stream(correlation_id : UInt32, stream : String, arguments : Hash(String, String)) : Bytes
      FrameCodec.request(Command::CreateStream, correlation_id) do |w|
        w.write_string(stream).write_string_map(arguments)
      end
    end

    def delete_stream(correlation_id : UInt32, stream : String) : Bytes
      FrameCodec.request(Command::DeleteStream, correlation_id) { |w| w.write_string(stream) }
    end

    def metadata(correlation_id : UInt32, streams : Enumerable(String)) : Bytes
      FrameCodec.request(Command::Metadata, correlation_id) do |w|
        values = streams.to_a
        w.write_i32(values.size.to_i32)
        values.each { |stream| w.write_string(stream) }
      end
    end

    def route(correlation_id : UInt32, routing_key : String, super_stream : String) : Bytes
      FrameCodec.request(Command::Route, correlation_id) do |w|
        w.write_string(routing_key).write_string(super_stream)
      end
    end

    def partitions(correlation_id : UInt32, super_stream : String) : Bytes
      FrameCodec.request(Command::Partitions, correlation_id) { |w| w.write_string(super_stream) }
    end

    def stream_stats(correlation_id : UInt32, stream : String) : Bytes
      FrameCodec.request(Command::StreamStats, correlation_id) { |w| w.write_string(stream) }
    end

    def create_super_stream(
      correlation_id : UInt32,
      name : String,
      partitions : Enumerable(String),
      binding_keys : Enumerable(String),
      arguments : Hash(String, String),
    ) : Bytes
      partition_values = partitions.to_a
      binding_values = binding_keys.to_a
      raise ArgumentError.new("a super stream requires at least one partition") if partition_values.empty?
      raise ArgumentError.new("partitions and binding_keys must have equal sizes") unless partition_values.size == binding_values.size
      FrameCodec.request(Command::CreateSuperStream, correlation_id) do |w|
        w.write_string(name)
        w.write_i32(partition_values.size.to_i32)
        partition_values.each { |value| w.write_string(value) }
        w.write_i32(binding_values.size.to_i32)
        binding_values.each { |value| w.write_string(value) }
        w.write_string_map(arguments)
      end
    end

    def delete_super_stream(correlation_id : UInt32, name : String) : Bytes
      FrameCodec.request(Command::DeleteSuperStream, correlation_id) { |w| w.write_string(name) }
    end

    def resolve_offset(
      correlation_id : UInt32,
      stream : String,
      offset : OffsetSpecification,
      properties : Hash(String, String) = {} of String => String,
    ) : Bytes
      FrameCodec.request(Command::ResolveOffsetSpec, correlation_id) do |w|
        w.write_string(stream)
        write_offset(w, offset)
        w.write_string_map(properties)
      end
    end

    def consumer_update_response(correlation_id : UInt32, code : ResponseCode, offset : OffsetSpecification) : Bytes
      FrameCodec.response(Command::ConsumerUpdate, correlation_id, code) { |w| write_offset(w, offset) }
    end

    def response(frame : Frame) : Response
      reader = frame.reader
      correlation_id = reader.read_u32
      raw_code = reader.read_u16
      code = ResponseCode.from_value(raw_code)
      Response.new(correlation_id, code, reader)
    rescue ArgumentError
      raise ProtocolError.new("unknown response code #{raw_code}")
    end

    def decode_publish_confirmation(frame : Frame) : PublishConfirmation
      reader = frame.reader
      publisher_id = reader.read_u8
      ids = Array(UInt64).new(reader.read_count) { reader.read_u64 }
      reader.finish!
      PublishConfirmation.new(publisher_id, ids)
    end

    def decode_publish_failure(frame : Frame) : PublishFailure
      reader = frame.reader
      publisher_id = reader.read_u8
      errors = {} of UInt64 => ResponseCode
      reader.read_count.times do
        id = reader.read_u64
        raw_code = reader.read_u16
        errors[id] = ResponseCode.from_value(raw_code)
      rescue ArgumentError
        raise ProtocolError.new("unknown publish error response code #{raw_code}")
      end
      reader.finish!
      PublishFailure.new(publisher_id, errors)
    end

    # Metadata is the only correlated response without a top-level response
    # code. Its payload starts with correlation_id followed by broker entries.
    def decode_metadata(frame : Frame) : Array(StreamMetadata)
      reader = frame.reader
      reader.read_u32
      brokers = {} of UInt16 => Broker
      reader.read_count.times do
        reference = reader.read_u16
        host = reader.read_string!
        port = reader.read_u32.to_i32!
        brokers[reference] = Broker.new(reference, host, port)
      end
      result = Array(StreamMetadata).new(reader.read_count) do
        stream = reader.read_string!
        raw_code = reader.read_u16
        code = ResponseCode.from_value(raw_code)
        leader_reference = reader.read_u16
        replica_references = Array(UInt16).new(reader.read_count) { reader.read_u16 }
        leader = leader_reference == UInt16::MAX ? nil : brokers[leader_reference]?
        replicas = replica_references.compact_map { |reference| brokers[reference]? }
        StreamMetadata.new(stream, code, leader, replicas)
      rescue ArgumentError
        raise ProtocolError.new("unknown metadata response code #{raw_code}")
      end
      reader.finish!
      result
    end

    def decode_versions(response : Response) : Array(CommandVersion)
      Array(CommandVersion).new(response.reader.read_count) do
        CommandVersion.new(response.reader.read_u16, response.reader.read_u16, response.reader.read_u16)
      end
    end

    def decode_strings(response : Response) : Array(String)
      response.reader.read_string_array
    end

    def decode_stats(response : Response) : StreamStats
      values = {} of String => Int64
      response.reader.read_count.times do
        values[response.reader.read_string!] = response.reader.read_i64
      end
      StreamStats.new(values)
    end

    def decode_tune(frame : Frame) : Tune
      reader = frame.reader
      tune = Tune.new(reader.read_u32, reader.read_u32)
      reader.finish!
      tune
    end

    def decode_metadata_update(frame : Frame) : MetadataUpdate
      reader = frame.reader
      result = MetadataUpdate.new(reader.read_u16, reader.read_string!)
      reader.finish!
      result
    end

    def decode_consumer_update(frame : Frame) : ConsumerUpdate
      reader = frame.reader
      result = ConsumerUpdate.new(reader.read_u32, reader.read_u8, reader.read_u8 != 0)
      reader.finish!
      result
    end

    private def write_offset(writer : Writer, offset : OffsetSpecification) : Nil
      writer.write_u16(offset.type.value)
      case offset.type
      when .offset?
        writer.write_u64(offset.value.not_nil!.as(UInt64))
      when .timestamp?
        writer.write_i64(offset.value.not_nil!.as(Int64))
      end
    end

    private def validate_publish_filters!(messages : Array(PublishEntry), version : UInt16) : Nil
      if version == 1 && messages.any? { |entry| !entry.filter.nil? }
        raise ProtocolError.new("publish filters require publish command version 2")
      end
    end
  end
end
