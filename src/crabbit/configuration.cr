module Crabbit
  # Built-in SASL authentication mechanisms supported during connection setup.
  enum SaslMechanism
    # Username/password PLAIN authentication.
    Plain
    # EXTERNAL authentication, typically backed by a TLS client certificate.
    External

    # Returns the uppercase protocol mechanism name.
    def protocol_name : String
      to_s.upcase
    end
  end

  # Extensibility point for SASL authentication.
  #
  # Implement `#mechanism` and `#initial_response`; override `#challenge` only
  # for challenge-response mechanisms. Pass the implementation as
  # `Configuration#sasl_authenticator`.
  abstract class SaslAuthenticator
    # Returns the SASL mechanism name sent to RabbitMQ.
    abstract def mechanism : String
    # Returns the initial opaque SASL response.
    abstract def initial_response : Bytes

    # Handles a server challenge and returns the next opaque response.
    #
    # The base implementation raises `AuthenticationError` because PLAIN and
    # EXTERNAL do not support challenges.
    def challenge(data : Bytes) : Bytes
      raise AuthenticationError.new("#{mechanism} does not support SASL challenges")
    end
  end

  # SASL PLAIN authenticator using a username and password.
  class PlainSaslAuthenticator < SaslAuthenticator
    # Creates a PLAIN authenticator.
    def initialize(@username : String, @password : String)
    end

    # Returns `"PLAIN"`.
    def mechanism : String
      "PLAIN"
    end

    # Returns the PLAIN initial response containing the configured credentials.
    def initial_response : Bytes
      "\0#{@username}\0#{@password}".to_slice.dup
    end
  end

  # SASL EXTERNAL authenticator with an empty initial response.
  class ExternalSaslAuthenticator < SaslAuthenticator
    # Returns `"EXTERNAL"`.
    def mechanism : String
      "EXTERNAL"
    end

    # Returns an empty initial response.
    def initial_response : Bytes
      Bytes.empty
    end
  end

  # TLS settings shared by Stream connections.
  struct TLSConfig
    # Returns the OpenSSL client context used for certificate verification and
    # optional client certificates.
    getter context : OpenSSL::SSL::Context::Client
    # Returns whether the advertised endpoint hostname is verified.
    getter verify_hostname : Bool

    # Creates TLS settings.
    #
    # Hostname verification should only be disabled in controlled development
    # environments. Configure custom trust roots and client certificates on
    # *context* before opening the environment.
    def initialize(
      @context : OpenSSL::SSL::Context::Client = OpenSSL::SSL::Context::Client.new,
      @verify_hostname : Bool = true,
    )
    end
  end

  # Immutable connection, authentication, timeout, and protocol configuration.
  #
  # `Configuration.parse` is convenient for one URI endpoint. Construct this
  # type directly to supply multiple entrypoints, custom SASL, or load-balancer
  # mode.
  struct Configuration
    # Default local RabbitMQ Stream URI.
    DEFAULT_URI = "rabbitmq-stream://guest:guest@localhost:5552/%2f"

    # Returns seed endpoints used for metadata and load-balanced connections.
    getter endpoints : Array(Endpoint)
    # Returns the SASL PLAIN username.
    getter username : String
    # Returns the SASL PLAIN password.
    getter password : String
    # Returns the RabbitMQ virtual host.
    getter virtual_host : String
    # Returns the selected built-in SASL mechanism.
    getter sasl : SaslMechanism
    # Returns the optional custom SASL authenticator.
    getter sasl_authenticator : SaslAuthenticator?
    # Returns the optional OAuth 2 configuration.
    getter oauth2 : OAuth2Config?
    # Returns TLS settings, or `nil` for plain Stream connections.
    getter tls : TLSConfig?
    # Returns the requested heartbeat interval; zero disables heartbeats.
    getter heartbeat : Time::Span
    # Returns the requested maximum frame size; zero means no local limit.
    getter max_frame_size : UInt32
    # Returns the TCP/TLS connection timeout.
    getter connection_timeout : Time::Span
    # Returns the timeout for correlated protocol requests.
    getter request_timeout : Time::Span
    # Returns client properties sent during Peer Properties negotiation.
    getter client_properties : Hash(String, String)
    # Returns whether broker connections must be reached through seed endpoints.
    getter load_balancer : Bool

    @oauth2_authenticator : OAuth2SaslAuthenticator?

    # Creates a connection configuration.
    #
    # Crabbit rotates through *endpoints* for locator connections. With
    # *load_balancer*, it also opens producer and consumer connections through
    # those entrypoints until RabbitMQ advertises the metadata-selected node.
    #
    # *client_properties* augment the default product, version, platform, and
    # information fields. Supplying *oauth2* creates a shared
    # `OAuth2SaslAuthenticator`; *oauth2* and *sasl_authenticator* are mutually
    # exclusive.
    def initialize(
      @endpoints : Array(Endpoint) = [Endpoint.new("localhost")],
      @username : String = "guest",
      @password : String = "guest",
      @virtual_host : String = "/",
      @sasl : SaslMechanism = SaslMechanism::Plain,
      @sasl_authenticator : SaslAuthenticator? = nil,
      @oauth2 : OAuth2Config? = nil,
      @tls : TLSConfig? = nil,
      @heartbeat : Time::Span = 60.seconds,
      @max_frame_size : UInt32 = 1_048_576_u32,
      @connection_timeout : Time::Span = 10.seconds,
      @request_timeout : Time::Span = 10.seconds,
      @client_properties : Hash(String, String) = {} of String => String,
      @load_balancer : Bool = false,
    )
      raise ConfigurationError.new("at least one endpoint is required") if endpoints.empty?
      endpoints.each do |endpoint|
        raise ConfigurationError.new("endpoint host must not be empty") if endpoint.host.empty?
        unless 1 <= endpoint.port <= 65_535
          raise ConfigurationError.new("endpoint port must be between 1 and 65535")
        end
      end
      raise ConfigurationError.new("max_frame_size must be 0 or at least 1 KiB") if 0 < max_frame_size < 1024
      raise ConfigurationError.new("heartbeat must not be negative") if heartbeat < Time::Span.zero
      if heartbeat.total_seconds > UInt32::MAX
        raise ConfigurationError.new("heartbeat exceeds the protocol UInt32 range")
      end
      raise ConfigurationError.new("connection_timeout must be positive") unless connection_timeout > Time::Span.zero
      raise ConfigurationError.new("request_timeout must be positive") unless request_timeout > Time::Span.zero
      if oauth2 && sasl_authenticator
        raise ConfigurationError.new("oauth2 and sasl_authenticator cannot both be configured")
      end
      if oauth2 && !sasl.plain?
        raise ConfigurationError.new("OAuth 2 requires the PLAIN SASL mechanism")
      end
      if oauth2 && endpoints.any? { |endpoint| !endpoint.tls } && !oauth2.allow_insecure_transport
        raise ConfigurationError.new(
          "OAuth 2 requires TLS for every Stream endpoint; set allow_insecure_transport only for development",
        )
      end
      @endpoints = endpoints.dup
      @client_properties = default_properties.merge(client_properties)
      @oauth2_authenticator = oauth2.try { |config| OAuth2SaslAuthenticator.new(config) }
    end

    # Returns the effective authenticator for this configuration.
    def authenticator : SaslAuthenticator
      sasl_authenticator || @oauth2_authenticator || case sasl
      when .plain?    then PlainSaslAuthenticator.new(username, password)
      when .external? then ExternalSaslAuthenticator.new
      else
        raise ConfigurationError.new("unsupported SASL mechanism #{sasl}")
      end
    end

    # Parses a RabbitMQ Stream URI into a configuration.
    #
    # Supported schemes are `rabbitmq-stream` and `rabbitmq-stream+tls`.
    # Username, password, and virtual host are percent-decoded. Additional
    # named *options* are forwarded to `.new`.
    #
    # ```
    # tls = Crabbit::TLSConfig.new(custom_context)
    # config = Crabbit::Configuration.parse(
    #   "rabbitmq-stream+tls://user:secret@rabbit.example/%2f",
    #   tls: tls,
    #   heartbeat: 30.seconds,
    # )
    # ```
    def self.parse(uri : String = DEFAULT_URI, tls : TLSConfig? = nil, **options) : self
      parsed = URI.parse(uri)
      scheme = parsed.scheme
      tls_enabled = case scheme
                    when "rabbitmq-stream"     then false
                    when "rabbitmq-stream+tls" then true
                    else
                      raise ConfigurationError.new("unsupported URI scheme #{scheme.inspect}")
                    end
      host = parsed.host || raise ConfigurationError.new("URI host is required")
      port = parsed.port || (tls_enabled ? 5551 : 5552)
      username = parsed.user ? URI.decode(parsed.user.not_nil!) : "guest"
      password = parsed.password ? URI.decode(parsed.password.not_nil!) : "guest"
      path = parsed.path
      virtual_host = path.empty? || path == "/" ? "/" : URI.decode(path.lchop('/'))
      if tls && !tls_enabled
        raise ConfigurationError.new("TLS configuration requires a rabbitmq-stream+tls URI")
      end
      effective_tls = tls_enabled ? (tls || TLSConfig.new) : nil
      new(
        **options,
        endpoints: [Endpoint.new(host, port, tls_enabled)],
        username: username,
        password: password,
        virtual_host: virtual_host,
        tls: effective_tls,
      )
    rescue ex : URI::Error
      raise ConfigurationError.new("invalid RabbitMQ Stream URI: #{ex.message}")
    end

    private def default_properties : Hash(String, String)
      {
        "product"     => "crabbit",
        "version"     => VERSION,
        "platform"    => "Crystal #{Crystal::VERSION}",
        "information" => "Native RabbitMQ Stream client for Crystal",
      }
    end
  end
end
