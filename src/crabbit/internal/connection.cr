# :nodoc:
module Crabbit::Internal
  class Connection
    enum State
      New
      Connecting
      Open
      Closing
      Closed
    end

    alias FrameHandler = Proc(Wire::Frame, Nil)
    alias DisconnectHandler = Proc(Exception, Nil)
    alias PendingResult = Wire::Frame | Exception

    getter endpoint : Endpoint
    getter state : State
    getter server_properties = {} of String => String
    getter connection_properties = {} of String => String
    getter negotiated_max_frame_size : UInt32
    getter negotiated_heartbeat : Time::Span
    getter command_versions = {} of Wire::Command => CommandVersion

    @io : IO?
    @state_mutex = Mutex.new
    @write_mutex = Mutex.new
    @pending_mutex = Mutex.new
    @correlation_mutex = Mutex.new
    @handlers_mutex = Mutex.new
    @activity_mutex = Mutex.new
    @pending = {} of UInt32 => Channel(PendingResult)
    @handlers = {} of Wire::Command => Array(FrameHandler)
    @disconnect_handlers = [] of DisconnectHandler
    @next_correlation_id = 0_u32
    @last_read_at = Time.instant
    @last_write_at = Time.instant
    @closed_channel = Channel(Nil).new
    @disconnect_notified = false
    @oauth2_registration : OAuth2SaslAuthenticator::Registration?

    def initialize(@configuration : Configuration, @endpoint : Endpoint)
      @state = State::New
      @negotiated_max_frame_size = configuration.max_frame_size
      @negotiated_heartbeat = configuration.heartbeat
    end

    def connect! : self
      transition!(State::New, State::Connecting)
      @io = open_socket
      perform_handshake!
      @state_mutex.synchronize { @state = State::Open }
      start_reader
      @oauth2_registration.try(&.ready!)
      start_heartbeat if negotiated_heartbeat > Time::Span.zero
      self
    rescue ex
      shutdown(ex, notify: false)
      raise ex
    end

    def open? : Bool
      @state_mutex.synchronize { @state.open? }
    end

    def closed? : Bool
      @state_mutex.synchronize { @state.closed? }
    end

    def on(command : Wire::Command, &handler : Wire::Frame ->) : FrameHandler
      callback = handler
      @handlers_mutex.synchronize do
        (@handlers[command] ||= [] of FrameHandler) << callback
      end
      callback
    end

    def remove_handler(command : Wire::Command, handler : FrameHandler) : Nil
      @handlers_mutex.synchronize { @handlers[command]?.try(&.delete(handler)) }
    end

    def on_disconnect(&handler : Exception ->) : DisconnectHandler
      callback = handler
      @handlers_mutex.synchronize { @disconnect_handlers << callback }
      callback
    end

    def remove_disconnect_handler(handler : DisconnectHandler) : Nil
      @handlers_mutex.synchronize { @disconnect_handlers.delete(handler) }
    end

    def request(timeout : Time::Span = @configuration.request_timeout, &build : UInt32 -> Bytes) : Wire::Frame
      raise ConnectionClosedError.new("connection is not open") unless open?
      correlation_id = next_correlation_id
      pending = Channel(PendingResult).new(1)
      @pending_mutex.synchronize { @pending[correlation_id] = pending }
      begin
        write(build.call(correlation_id))
        result = select
        when value = pending.receive
          value
        when timeout(timeout)
          raise TimeoutError.new("request #{correlation_id} timed out after #{timeout}")
        end
        raise result if result.is_a?(Exception)
        result.as(Wire::Frame)
      ensure
        @pending_mutex.synchronize { @pending.delete(correlation_id) }
      end
    end

    def send(bytes : Bytes) : Nil
      raise ConnectionClosedError.new("connection is not open") unless open?
      write(bytes)
    end

    def version(command : Wire::Command, preferred : UInt16 = Wire::SUPPORTED_VERSIONS[command].end) : UInt16
      local = Wire::SUPPORTED_VERSIONS[command]
      remote = command_versions[command]?
      # Version 1 is the protocol baseline for brokers predating command-version
      # exchange and for commands omitted from a broker's response.
      return local.begin unless remote
      remote.highest_common(local.begin, Math.min(preferred, local.end)) ||
        raise ProtocolError.new("broker and client share no version for #{command}")
    end

    def close : Nil
      should_close = @state_mutex.synchronize do
        if @state.open?
          @state = State::Closing
          true
        else
          false
        end
      end
      return unless should_close

      begin
        correlation_id = next_correlation_id
        pending = Channel(PendingResult).new(1)
        @pending_mutex.synchronize { @pending[correlation_id] = pending }
        write(Wire::Commands.close(correlation_id))
        select
        when pending.receive
        when timeout(1.second)
        end
      rescue
      ensure
        @pending_mutex.synchronize { @pending.delete(correlation_id) } if correlation_id
        shutdown(ConnectionClosedError.new("connection closed by client"), notify: false)
      end
    end

    def wait_closed : Nil
      @closed_channel.receive?
    end

    def abort(cause : Exception) : Nil
      shutdown(cause, notify: true)
    end

    private def perform_handshake! : Nil
      io = socket

      correlation_id = next_correlation_id
      write(Wire::Commands.peer_properties(correlation_id, @configuration.client_properties))
      response = handshake_response(Wire::Command::PeerProperties, correlation_id)
      response.code.raise_unless_ok!("peer properties")
      @server_properties = response.reader.read_string_map
      response.reader.finish!

      correlation_id = next_correlation_id
      write(Wire::Commands.sasl_handshake(correlation_id))
      response = handshake_response(Wire::Command::SaslHandshake, correlation_id)
      response.code.raise_unless_ok!("SASL handshake")
      mechanisms = response.reader.read_string_array
      response.reader.finish!

      authenticator = @configuration.authenticator
      unless mechanisms.includes?(authenticator.mechanism)
        raise AuthenticationError.new(
          "broker does not offer #{authenticator.mechanism}; offered: #{mechanisms.join(", ")}",
        )
      end
      opaque = if oauth2 = authenticator.as?(OAuth2SaslAuthenticator)
                 registration = oauth2.register("#{endpoint.host}:#{endpoint.port}") do |credentials|
                   reauthenticate(oauth2.mechanism, credentials)
                 end
                 @oauth2_registration = registration
                 registration.initial_response
               else
                 authenticator.initial_response
               end
      32.times do |attempt|
        correlation_id = next_correlation_id
        write(Wire::Commands.sasl_authenticate(correlation_id, authenticator.mechanism, opaque))
        response = handshake_response(Wire::Command::SaslAuthenticate, correlation_id)
        if response.code.ok?
          response.reader.read_bytes if response.reader.remaining > 0
          response.reader.finish!
          break
        elsif response.code.sasl_challenge?
          challenge = response.reader.read_bytes || Bytes.empty
          response.reader.finish!
          opaque = authenticator.challenge(challenge)
        else
          raise AuthenticationError.new("SASL authentication failed: #{response.code}")
        end
        raise AuthenticationError.new("SASL challenge loop exceeded 32 rounds") if attempt == 31
      end

      tune_frame = read_handshake_frame
      unless tune_frame.command.tune? && !tune_frame.response?
        raise ProtocolError.new("expected Tune, got #{tune_frame.command}")
      end
      tune = Wire::Commands.decode_tune(tune_frame)
      @negotiated_max_frame_size = negotiate(
        @configuration.max_frame_size,
        tune.max_frame_size,
      )
      heartbeat_seconds = negotiate(
        @configuration.heartbeat.total_seconds.to_u32,
        tune.heartbeat_seconds,
      )
      @negotiated_heartbeat = heartbeat_seconds.seconds
      write(Wire::Commands.tune(@negotiated_max_frame_size, heartbeat_seconds))

      correlation_id = next_correlation_id
      write(Wire::Commands.open(correlation_id, @configuration.virtual_host))
      response = handshake_response(Wire::Command::Open, correlation_id)
      response.code.raise_unless_ok!("open virtual host")
      @connection_properties = response.reader.read_string_map
      response.reader.finish!

      if broker_supports_command_versions?
        local_versions = Wire::SUPPORTED_VERSIONS.map do |command, range|
          CommandVersion.new(command.value, range.begin, range.end)
        end
        correlation_id = next_correlation_id
        write(Wire::Commands.exchange_versions(correlation_id, local_versions))
        response = handshake_response(Wire::Command::ExchangeCommandVersions, correlation_id)
        response.code.raise_unless_ok!("exchange command versions")
        Wire::Commands.decode_versions(response).each do |command_version|
          if command = Wire::Command.from_value?(command_version.key)
            @command_versions[command] = command_version
          end
        end
        response.reader.finish!
      end
    ensure
      if socket = @io
        case socket
        when TCPSocket
          socket.read_timeout = nil
        when OpenSSL::SSL::Socket::Client
          socket.read_timeout = nil
        end
      end
    end

    private def handshake_response(command : Wire::Command, correlation_id : UInt32) : Wire::Response
      frame = read_handshake_frame
      unless frame.response? && frame.command == command
        raise ProtocolError.new("expected #{command} response, got #{frame.command}")
      end
      response = Wire::Commands.response(frame)
      unless response.correlation_id == correlation_id
        raise ProtocolError.new(
          "expected correlation #{correlation_id}, got #{response.correlation_id}",
        )
      end
      response
    end

    private def read_handshake_frame : Wire::Frame
      frame = Wire::FrameCodec.read(socket, @configuration.max_frame_size)
      touch_read
      frame
    end

    private def reauthenticate(mechanism : String, opaque : Bytes) : Nil
      response = Wire::Commands.response(
        request { |correlation| Wire::Commands.sasl_authenticate(correlation, mechanism, opaque) },
      )
      if response.code.ok?
        response.reader.read_bytes if response.reader.remaining > 0
        response.reader.finish!
        return
      end
      response.reader.read_bytes if response.reader.remaining > 0
      response.reader.finish!
      raise AuthenticationError.new("SASL re-authentication failed: #{response.code}")
    end

    private def open_socket : IO
      tcp = TCPSocket.new(
        endpoint.host,
        endpoint.port,
        connect_timeout: @configuration.connection_timeout,
      )
      tcp.tcp_nodelay = true
      tcp.read_timeout = @configuration.request_timeout
      return tcp unless endpoint.tls

      tls = @configuration.tls || TLSConfig.new
      hostname = tls.verify_hostname ? endpoint.host : nil
      OpenSSL::SSL::Socket::Client.new(
        tcp,
        context: tls.context,
        sync_close: true,
        hostname: hostname,
      )
    rescue ex
      raise ConnectionError.new("cannot connect to #{endpoint.host}:#{endpoint.port}: #{ex.message}")
    end

    private def start_reader : Nil
      spawn(name: "crabbit-reader-#{endpoint.host}:#{endpoint.port}") do
        begin
          while open?
            frame = Wire::FrameCodec.read(socket, negotiated_max_frame_size)
            touch_read
            dispatch(frame)
          end
        rescue ex
          shutdown(ex, notify: true) unless closed?
        end
      end
    end

    private def start_heartbeat : Nil
      interval = negotiated_heartbeat / 2
      spawn(name: "crabbit-heartbeat-#{endpoint.host}:#{endpoint.port}") do
        while open?
          sleep interval
          break unless open?
          now = Time.instant
          last_read, last_write = @activity_mutex.synchronize { {@last_read_at, @last_write_at} }
          if now - last_read > negotiated_heartbeat * 2
            shutdown(TimeoutError.new("missed RabbitMQ Stream heartbeat"), notify: true)
            break
          end
          send(Wire::Commands.heartbeat) if now - last_write >= interval
        end
      rescue ex
        shutdown(ex, notify: true) unless closed?
      end
    end

    private def dispatch(frame : Wire::Frame) : Nil
      command = frame.command
      if frame.response? && !command.credit? && !command.tune?
        correlation_id = frame.reader.read_u32
        pending = @pending_mutex.synchronize { @pending.delete(correlation_id) }
        if pending
          pending.send(frame)
        else
          Log.debug { "late or unknown response correlation_id=#{correlation_id}" }
        end
        return
      end

      case command
      when .heartbeat?
        return
      when .close?
        handle_server_close(frame)
        return
      else
        handlers = @handlers_mutex.synchronize { @handlers[command]?.try(&.dup) }
        if handlers
          handlers.each { |handler| handler.call(frame) }
        else
          Log.debug { "no handler registered for #{command}" }
        end
      end
    end

    private def handle_server_close(frame : Wire::Frame) : Nil
      reader = frame.reader
      correlation_id = reader.read_u32
      code = reader.read_u16
      reason = reader.read_string!
      reader.finish!
      write(Wire::FrameCodec.response(Wire::Command::Close, correlation_id, ResponseCode::Ok) { })
      shutdown(ConnectionClosedError.new("broker closed connection (#{code}): #{reason}"), notify: true)
    end

    private def write(bytes : Bytes) : Nil
      @write_mutex.synchronize do
        socket.write(bytes)
        socket.flush
        touch_write
      end
    rescue ex : IO::Error
      error = ConnectionClosedError.new("connection write failed: #{ex.message}")
      shutdown(error, notify: true)
      raise error
    end

    private def socket : IO
      @io || raise ConnectionClosedError.new("socket is closed")
    end

    private def touch_read : Nil
      @activity_mutex.synchronize { @last_read_at = Time.instant }
    end

    private def touch_write : Nil
      @activity_mutex.synchronize { @last_write_at = Time.instant }
    end

    private def next_correlation_id : UInt32
      @correlation_mutex.synchronize do
        loop do
          @next_correlation_id &+= 1_u32
          return @next_correlation_id unless @pending_mutex.synchronize { @pending.has_key?(@next_correlation_id) }
        end
      end
    end

    private def negotiate(client : UInt32, server : UInt32) : UInt32
      return server if client == 0
      return client if server == 0
      Math.min(client, server)
    end

    private def broker_supports_command_versions? : Bool
      version = server_properties["version"]?
      return true unless version
      numbers = version.split('.', 3).map { |part| part.to_i? || 0 }
      major = numbers[0]? || 0
      minor = numbers[1]? || 0
      major > 3 || (major == 3 && minor >= 11)
    end

    private def transition!(from : State, to : State) : Nil
      @state_mutex.synchronize do
        unless @state == from
          raise ConnectionError.new("invalid connection state transition #{@state} -> #{to}")
        end
        @state = to
      end
    end

    private def shutdown(cause : Exception, notify : Bool) : Nil
      changed = @state_mutex.synchronize do
        unless @state.closed?
          @state = State::Closed
          true
        else
          false
        end
      end
      return unless changed

      begin
        @io.try(&.close)
      rescue
      ensure
        @io = nil
      end
      @oauth2_registration.try(&.close)
      @oauth2_registration = nil
      pending = @pending_mutex.synchronize do
        values = @pending.values
        @pending.clear
        values
      end
      pending.each { |channel| channel.send(cause) }
      @closed_channel.close
      notify_disconnect(cause) if notify
    end

    private def notify_disconnect(cause : Exception) : Nil
      handlers = @handlers_mutex.synchronize do
        return if @disconnect_notified
        @disconnect_notified = true
        @disconnect_handlers.dup
      end
      handlers.each do |handler|
        spawn(name: "crabbit-disconnect-handler") { handler.call(cause) }
      end
    end
  end
end
