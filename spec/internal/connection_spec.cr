require "../spec_helper"

private alias Wire = Crabbit::Internal::Wire

describe Crabbit::Internal::Connection do
  it "falls back to command version 1 when an older broker cannot exchange versions" do
    config = Crabbit::Configuration.new
    connection = Crabbit::Internal::Connection.new(config, config.endpoints.first)
    connection.version(Wire::Command::Publish).should eq 1_u16
  end

  it "performs negotiation and multiplexes correlated responses" do
    server = TCPServer.new("127.0.0.1", 0)
    port = server.local_address.port
    server_done = Channel(Exception?).new(1)

    spawn do
      socket = server.accept
      begin
        frame = Wire::FrameCodec.read(socket)
        response = frame.reader
        correlation = response.read_u32
        response.read_string_map
        socket.write(Wire::FrameCodec.response(Wire::Command::PeerProperties, correlation, Crabbit::ResponseCode::Ok) do |w|
          w.write_string_map({"product" => "RabbitMQ"})
        end)

        frame = Wire::FrameCodec.read(socket)
        correlation = frame.reader.read_u32
        socket.write(Wire::FrameCodec.response(Wire::Command::SaslHandshake, correlation, Crabbit::ResponseCode::Ok) do |w|
          w.write_i32(1).write_string("PLAIN")
        end)

        frame = Wire::FrameCodec.read(socket)
        auth_reader = frame.reader
        correlation = auth_reader.read_u32
        auth_reader.read_string.should eq "PLAIN"
        auth_reader.read_bytes.should eq "\0guest\0guest".to_slice
        socket.write(Wire::FrameCodec.response(Wire::Command::SaslAuthenticate, correlation, Crabbit::ResponseCode::Ok) do |w|
          w.write_bytes(Bytes.empty)
        end)

        socket.write(Wire::FrameCodec.command(Wire::Command::Tune) do |w|
          w.write_u32(1_048_576_u32).write_u32(60_u32)
        end)
        tune_response = Wire::FrameCodec.read(socket)
        tune_response.response?.should be_true
        tune_response.command.should eq Wire::Command::Tune

        frame = Wire::FrameCodec.read(socket)
        correlation = frame.reader.read_u32
        socket.write(Wire::FrameCodec.response(Wire::Command::Open, correlation, Crabbit::ResponseCode::Ok) do |w|
          w.write_string_map({"advertised_host" => "127.0.0.1", "advertised_port" => port.to_s})
        end)

        frame = Wire::FrameCodec.read(socket)
        version_reader = frame.reader
        correlation = version_reader.read_u32
        count = version_reader.read_count
        versions = Array(Crabbit::CommandVersion).new(count) do
          Crabbit::CommandVersion.new(
            version_reader.read_u16,
            version_reader.read_u16,
            version_reader.read_u16,
          )
        end
        socket.write(Wire::FrameCodec.response(Wire::Command::ExchangeCommandVersions, correlation, Crabbit::ResponseCode::Ok) do |w|
          w.write_i32(versions.size.to_i32)
          versions.each { |version| w.write_u16(version.key).write_u16(version.min_version).write_u16(version.max_version) }
        end)

        frame = Wire::FrameCodec.read(socket)
        metadata_reader = frame.reader
        correlation = metadata_reader.read_u32
        metadata_reader.read_string_array.should eq ["events"]
        metadata_reader.finish!
        socket.write(Wire::FrameCodec.command(Wire::Command::Metadata, response: true) do |w|
          w.write_u32(correlation)
          w.write_i32(1).write_u16(7_u16).write_string("127.0.0.1").write_u32(port.to_u32)
          w.write_i32(1).write_string("events")
          w.write_u16(Crabbit::ResponseCode::Ok.value).write_u16(7_u16).write_i32(0)
        end)

        frame = Wire::FrameCodec.read(socket)
        correlation = frame.reader.read_u32
        socket.write(Wire::FrameCodec.response(Wire::Command::StreamStats, correlation, Crabbit::ResponseCode::Ok) do |w|
          w.write_i32(1).write_string("first_chunk_id").write_i64(42_i64)
        end)

        frame = Wire::FrameCodec.read(socket)
        correlation = frame.reader.read_u32
        socket.write(Wire::FrameCodec.response(Wire::Command::Close, correlation, Crabbit::ResponseCode::Ok) { })
        server_done.send(nil)
      rescue ex
        server_done.send(ex)
      ensure
        socket.close
      end
    end

    config = Crabbit::Configuration.new(
      endpoints: [Crabbit::Endpoint.new("127.0.0.1", port)],
      heartbeat: 60.seconds,
    )
    connection = Crabbit::Internal::Connection.new(config, config.endpoints.first).connect!
    connection.server_properties["product"].should eq "RabbitMQ"
    connection.version(Wire::Command::Publish).should eq 2_u16

    client = Crabbit::Internal::Client.new(connection)
    metadata = client.metadata(["events"])
    metadata.first.leader.should eq Crabbit::Broker.new(7_u16, "127.0.0.1", port)
    client.stream_stats("events")["first_chunk_id"].should eq 42_i64
    connection.close

    if error = server_done.receive
      raise error
    end
    server.close
  end
end
