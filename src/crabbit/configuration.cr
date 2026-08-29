module Crabbit
  enum SaslMechanism
    Plain
    External

    def protocol_name : String
      to_s.upcase
    end
  end

  abstract class SaslAuthenticator
    abstract def mechanism : String
    abstract def initial_response : Bytes

    def challenge(data : Bytes) : Bytes
      raise AuthenticationError.new("#{mechanism} does not support SASL challenges")
    end
  end

  class PlainSaslAuthenticator < SaslAuthenticator
    def initialize(@username : String, @password : String)
    end

    def mechanism : String
      "PLAIN"
    end

    def initial_response : Bytes
      "\0#{@username}\0#{@password}".to_slice.dup
    end
  end

  class ExternalSaslAuthenticator < SaslAuthenticator
    def mechanism : String
      "EXTERNAL"
    end

    def initial_response : Bytes
      Bytes.empty
    end
  end

  struct TLSConfig
    getter context : OpenSSL::SSL::Context::Client
    getter verify_hostname : Bool

    def initialize(
      @context : OpenSSL::SSL::Context::Client = OpenSSL::SSL::Context::Client.new,
      @verify_hostname : Bool = true,
    )
    end
  end

  struct Configuration
    DEFAULT_URI = "rabbitmq-stream://guest:guest@localhost:5552/%2f"

    getter endpoints : Array(Endpoint)
    getter username : String
    getter password : String
    getter virtual_host : String
    getter sasl : SaslMechanism
    getter sasl_authenticator : SaslAuthenticator?
    getter oauth2 : OAuth2Config?
    getter tls : TLSConfig?
    getter heartbeat : Time::Span
    getter max_frame_size : UInt32
    getter connection_timeout : Time::Span
    getter request_timeout : Time::Span
    getter client_properties : Hash(String, String)
    getter load_balancer : Bool

    @oauth2_authenticator : OAuth2SaslAuthenticator?

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

    def authenticator : SaslAuthenticator
      sasl_authenticator || @oauth2_authenticator || case sasl
      when .plain?    then PlainSaslAuthenticator.new(username, password)
      when .external? then ExternalSaslAuthenticator.new
      else
        raise ConfigurationError.new("unsupported SASL mechanism #{sasl}")
      end
    end

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
