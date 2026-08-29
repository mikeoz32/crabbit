module Crabbit
  struct OAuth2Config
    getter token_endpoint : URI
    getter client_id : String
    getter client_secret : String
    getter grant_type : String
    getter parameters : Hash(String, String)
    getter tls_context : OpenSSL::SSL::Context::Client?
    getter connection_timeout : Time::Span
    getter request_timeout : Time::Span
    getter refresh_ratio : Float64
    getter refresh_retry_delay : Time::Span
    getter refresh_retry_max_delay : Time::Span
    getter allow_insecure_transport : Bool

    def initialize(
      token_endpoint : String | URI,
      @client_id : String,
      @client_secret : String,
      @grant_type : String = "client_credentials",
      @parameters : Hash(String, String) = {} of String => String,
      @tls_context : OpenSSL::SSL::Context::Client? = nil,
      @connection_timeout : Time::Span = 30.seconds,
      @request_timeout : Time::Span = 60.seconds,
      @refresh_ratio : Float64 = 0.8,
      @refresh_retry_delay : Time::Span = 1.second,
      @refresh_retry_max_delay : Time::Span = 30.seconds,
      @allow_insecure_transport : Bool = false,
    )
      @token_endpoint = token_endpoint.is_a?(URI) ? token_endpoint : parse_uri(token_endpoint)
      unless {"http", "https"}.includes?(@token_endpoint.scheme)
        raise ConfigurationError.new("OAuth 2 token endpoint must use http or https")
      end
      if @token_endpoint.scheme == "http" && !allow_insecure_transport
        raise ConfigurationError.new(
          "OAuth 2 token endpoint must use https; set allow_insecure_transport only for development",
        )
      end
      raise ConfigurationError.new("OAuth 2 token endpoint host is required") unless @token_endpoint.host
      raise ConfigurationError.new("OAuth 2 client_id must not be empty") if client_id.empty?
      raise ConfigurationError.new("OAuth 2 client_secret must not be empty") if client_secret.empty?
      raise ConfigurationError.new("OAuth 2 grant_type must not be empty") if grant_type.empty?
      raise ConfigurationError.new("OAuth 2 connection_timeout must be positive") unless connection_timeout > Time::Span.zero
      raise ConfigurationError.new("OAuth 2 request_timeout must be positive") unless request_timeout > Time::Span.zero
      unless 0.0 < refresh_ratio < 1.0
        raise ConfigurationError.new("OAuth 2 refresh_ratio must be greater than 0 and less than 1")
      end
      raise ConfigurationError.new("OAuth 2 refresh_retry_delay must be positive") unless refresh_retry_delay > Time::Span.zero
      if refresh_retry_max_delay < refresh_retry_delay
        raise ConfigurationError.new(
          "OAuth 2 refresh_retry_max_delay must be at least refresh_retry_delay",
        )
      end
      if tls_context && @token_endpoint.scheme != "https"
        raise ConfigurationError.new("OAuth 2 TLS context requires an https token endpoint")
      end
      @parameters = parameters.dup
    end

    private def parse_uri(value : String) : URI
      URI.parse(value)
    rescue ex : URI::Error
      raise ConfigurationError.new("invalid OAuth 2 token endpoint: #{ex.message}")
    end
  end

  record OAuth2Token, value : String, expires_at : Time::Instant

  abstract class OAuth2TokenProvider
    abstract def request : OAuth2Token
  end

  class OAuth2HttpTokenProvider < OAuth2TokenProvider
    getter config : OAuth2Config

    def initialize(@config : OAuth2Config)
    end

    def request : OAuth2Token
      form = config.parameters.dup
      form["grant_type"] = config.grant_type
      body = URI::Params.encode(form)
      headers = HTTP::Headers{
        "Authorization" => "Basic #{basic_credentials}",
        "Accept"        => "application/json",
        "Content-Type"  => "application/x-www-form-urlencoded",
      }
      tls = config.token_endpoint.scheme == "https" ? (config.tls_context || true) : nil
      response = HTTP::Client.new(config.token_endpoint, tls) do |client|
        client.connect_timeout = config.connection_timeout
        client.read_timeout = config.request_timeout
        client.write_timeout = config.request_timeout
        client.post(config.token_endpoint.request_target, headers, body)
      end
      unless response.status_code == 200
        raise OAuth2Error.new(
          "OAuth 2 token endpoint returned HTTP #{response.status_code}",
        )
      end
      content_type = response.headers["Content-Type"]?
      unless content_type.try(&.downcase.includes?("json"))
        raise OAuth2Error.new(
          "OAuth 2 token endpoint did not return JSON (Content-Type #{content_type.inspect})",
        )
      end
      parse_token(response.body)
    rescue ex : OAuth2Error
      raise ex
    rescue ex
      raise OAuth2Error.new("could not retrieve OAuth 2 token: #{ex.message}")
    end

    private def parse_token(body : String) : OAuth2Token
      object = JSON.parse(body).as_h
      value = object["access_token"]?.try(&.as_s?)
      raise OAuth2Error.new("OAuth 2 token response omitted access_token") unless value
      raise OAuth2Error.new("OAuth 2 access_token must not be empty") if value.empty?

      expires_value = object["expires_in"]?
      expires_in = expires_value.try(&.as_i64?) || expires_value.try(&.as_f?).try(&.to_i64)
      unless expires_in && expires_in > 0
        raise OAuth2Error.new("OAuth 2 token response must contain a positive expires_in")
      end
      OAuth2Token.new(value, Time.instant + expires_in.seconds)
    rescue ex : JSON::ParseException | TypeCastError
      raise OAuth2Error.new("invalid OAuth 2 token response: #{ex.message}")
    end

    private def basic_credentials : String
      client_id = URI.encode_www_form(config.client_id)
      client_secret = URI.encode_www_form(config.client_secret)
      Base64.strict_encode("#{client_id}:#{client_secret}")
    end
  end

  class OAuth2SaslAuthenticator < SaslAuthenticator
    alias RefreshCallback = Proc(Bytes, Nil)

    private enum Maintenance
      RefreshToken
      Reauthenticate
    end

    class Registration
      getter name : String

      @mutex = Mutex.new
      @closed = false
      @ready = false

      protected def initialize(
        @manager : OAuth2SaslAuthenticator,
        @id : UInt64,
        @name : String,
      )
      end

      def initial_response : Bytes
        @mutex.synchronize do
          raise ResourceClosedError.new("OAuth 2 registration is closed") if @closed
          @manager.connect(@id)
        end
      end

      # Marks the physical connection as able to process asynchronous
      # SaslAuthenticate responses. Connection calls this only after its reader
      # fiber is running.
      def ready! : Nil
        activate = @mutex.synchronize do
          raise ResourceClosedError.new("OAuth 2 registration is closed") if @closed
          unless @ready
            @ready = true
            true
          else
            false
          end
        end
        @manager.activate(@id) if activate
      end

      def close : Nil
        unregister = @mutex.synchronize do
          unless @closed
            @closed = true
            true
          else
            false
          end
        end
        @manager.unregister(@id) if unregister
      end
    end

    private class RegistrationState
      getter name : String
      getter callback : RefreshCallback
      property generation : UInt64?
      property active = false

      def initialize(@name : String, @callback : RefreshCallback)
      end
    end

    getter config : OAuth2Config

    @mutex = Mutex.new
    @request_mutex = Mutex.new
    @registrations = {} of UInt64 => RegistrationState
    @next_registration_id = 0_u64
    @token : OAuth2Token?
    @token_generation = 0_u64
    @refresh_at : Time::Instant?
    @reauthenticate_at : Time::Instant?
    @token_retry_attempt = 0
    @reauthentication_retry_attempt = 0
    @worker_running = false
    @signal = Channel(Nil).new(1)

    def initialize(
      @config : OAuth2Config,
      @provider : OAuth2TokenProvider = OAuth2HttpTokenProvider.new(config),
    )
    end

    def mechanism : String
      "PLAIN"
    end

    # Direct use is supported, but Environment connections use registrations so
    # that refreshed tokens can be pushed to every open socket.
    def initial_response : Bytes
      token = current_token
      plain_response(token.value)
    end

    def register(name : String, &callback : Bytes ->) : Registration
      id = @mutex.synchronize do
        @next_registration_id &+= 1_u64
        @registrations[@next_registration_id] = RegistrationState.new(name, callback)
        @next_registration_id
      end
      Registration.new(self, id, name)
    end

    def registration_count : Int32
      @mutex.synchronize { @registrations.size }
    end

    def token_generation : UInt64
      @mutex.synchronize { @token_generation }
    end

    def registrations_current? : Bool
      @mutex.synchronize do
        @registrations.values.all? do |registration|
          !registration.active || registration.generation == @token_generation
        end
      end
    end

    protected def connect(id : UInt64) : Bytes
      @mutex.synchronize do
        @registrations[id]? || raise ResourceClosedError.new(
          "OAuth 2 registration is closed",
        )
      end
      current_token
      selected = @mutex.synchronize do
        registration = @registrations[id]? || raise ResourceClosedError.new(
          "OAuth 2 registration is closed",
        )
        current = @token || raise OAuth2Error.new("OAuth 2 token is unavailable")
        registration.generation = @token_generation
        current
      end
      plain_response(selected.value)
    end

    protected def activate(id : UInt64) : Nil
      start_worker = @mutex.synchronize do
        registration = @registrations[id]? || raise ResourceClosedError.new(
          "OAuth 2 registration is closed",
        )
        registration.active = true
        if registration.generation != @token_generation
          schedule_reauthentication_locked(Time.instant)
        end
        start_worker_locked
      end
      spawn(name: "crabbit-oauth2-refresh") { refresh_loop } if start_worker
      wake
    end

    protected def unregister(id : UInt64) : Nil
      @mutex.synchronize do
        @registrations.delete(id)
        unless stale_active_registration_locked?
          @reauthenticate_at = nil
          @reauthentication_retry_attempt = 0
        end
      end
      wake
    end

    private def current_token : OAuth2Token
      if token = valid_token
        return token
      end

      token, installed = @request_mutex.synchronize do
        if current = valid_token
          {current, false}
        else
          requested = @provider.request
          installed_token = @mutex.synchronize { install_token_locked(requested) }
          {installed_token, true}
        end
      end
      wake if installed
      token
    end

    private def valid_token : OAuth2Token?
      @mutex.synchronize do
        if token = @token
          token if token.expires_at > Time.instant
        end
      end
    end

    private def install_token_locked(token : OAuth2Token) : OAuth2Token
      remaining = token.expires_at - Time.instant
      raise OAuth2Error.new("OAuth 2 token is already expired") unless remaining > Time::Span.zero
      @token = token
      @token_generation &+= 1_u64
      @refresh_at = Time.instant + remaining * config.refresh_ratio
      @token_retry_attempt = 0
      @reauthentication_retry_attempt = 0
      schedule_reauthentication_locked(Time.instant) if stale_active_registration_locked?
      token
    end

    private def start_worker_locked : Bool
      return false if @worker_running
      @worker_running = true
      true
    end

    private def refresh_loop : Nil
      loop do
        work = next_maintenance
        return unless work
        maintenance, scheduled_at, generation = work
        delay = scheduled_at - Time.instant
        if delay > Time::Span.zero
          select
          when timeout(delay)
          when @signal.receive
            next
          end
        end
        case maintenance
        when .refresh_token?  then refresh_token(generation)
        when .reauthenticate? then reauthenticate_current(generation)
        end
      end
    rescue ex
      Log.error(exception: ex) { "OAuth 2 refresh worker stopped unexpectedly" }
      restart = @mutex.synchronize do
        @worker_running = false
        @registrations.values.any?(&.active) && start_worker_locked
      end
      spawn(name: "crabbit-oauth2-refresh") { refresh_loop } if restart
    end

    private def next_maintenance : Tuple(Maintenance, Time::Instant, UInt64)?
      @mutex.synchronize do
        unless @registrations.values.any?(&.active)
          @worker_running = false
          return nil
        end

        refresh_at = @refresh_at || (Time.instant + config.refresh_retry_delay)
        if reauthenticate_at = @reauthenticate_at
          if reauthenticate_at <= refresh_at
            return {Maintenance::Reauthenticate, reauthenticate_at, @token_generation}
          end
        end
        {Maintenance::RefreshToken, refresh_at, @token_generation}
      end
    end

    private def refresh_token(expected_generation : UInt64) : Nil
      installed = @request_mutex.synchronize do
        should_refresh = @mutex.synchronize do
          @token_generation == expected_generation &&
            @refresh_at.try { |at| at <= Time.instant }
        end
        next false unless should_refresh

        token = @provider.request
        @mutex.synchronize do
          if @token_generation == expected_generation
            install_token_locked(token)
            true
          else
            false
          end
        end
      end
      wake if installed
    rescue ex
      retry_scheduled = @mutex.synchronize do
        if @token_generation == expected_generation
          delay = retry_delay(@token_retry_attempt)
          @token_retry_attempt = Math.min(@token_retry_attempt + 1, 20)
          @refresh_at = Time.instant + delay
          true
        else
          false
        end
      end
      if retry_scheduled
        Log.warn(exception: ex) { "could not refresh OAuth 2 token; retrying" }
        wake
      end
    end

    private def reauthenticate_current(expected_generation : UInt64) : Nil
      token, registrations = @mutex.synchronize do
        unless @token_generation == expected_generation
          return
        end
        @reauthenticate_at = nil
        token = @token || return
        selected_registrations = @registrations.to_a.select do |_id, registration|
          registration.active && registration.generation != expected_generation
        end
        {token, selected_registrations}
      end
      return if registrations.empty?

      opaque = plain_response(token.value)
      results = Channel(Tuple(UInt64, Exception?)).new(registrations.size)
      names = {} of UInt64 => String
      registrations.each do |id, registration|
        names[id] = registration.name
        spawn(name: "crabbit-oauth2-reauthenticate") do
          error : Exception? = nil
          still_stale = @mutex.synchronize do
            if current = @registrations[id]?
              current.active && current.generation != expected_generation
            else
              false
            end
          end
          if still_stale
            begin
              registration.callback.call(opaque)
            rescue ex
              error = ex
            end
          end
          results.send({id, error})
        end
      end

      successful = [] of UInt64
      registrations.size.times do
        id, error = results.receive
        if error
          Log.warn(exception: error) do
            "could not refresh OAuth 2 credentials for #{names[id]}"
          end
        else
          successful << id
        end
      end

      @mutex.synchronize do
        if @token_generation == expected_generation
          successful.each do |id|
            if current = @registrations[id]?
              current.generation = expected_generation if current.active
            end
          end
          if stale_active_registration_locked?
            delay = retry_delay(@reauthentication_retry_attempt)
            @reauthentication_retry_attempt = Math.min(
              @reauthentication_retry_attempt + 1,
              20,
            )
            schedule_reauthentication_locked(Time.instant + delay)
          else
            @reauthentication_retry_attempt = 0
          end
        elsif stale_active_registration_locked?
          schedule_reauthentication_locked(Time.instant)
        end
      end
      wake
    end

    private def stale_active_registration_locked? : Bool
      @registrations.values.any? do |registration|
        registration.active && registration.generation != @token_generation
      end
    end

    private def schedule_reauthentication_locked(at : Time::Instant) : Nil
      current = @reauthenticate_at
      @reauthenticate_at = at if current.nil? || at < current
    end

    private def retry_delay(attempt : Int32) : Time::Span
      multiplier = 1_i64 << Math.min(attempt, 20)
      delay = config.refresh_retry_delay * multiplier
      delay > config.refresh_retry_max_delay ? config.refresh_retry_max_delay : delay
    end

    private def plain_response(token : String) : Bytes
      "\0\0#{token}".to_slice.dup
    end

    private def wake : Nil
      select
      when @signal.send(nil)
      else
      end
    rescue Channel::ClosedError
    end
  end
end
