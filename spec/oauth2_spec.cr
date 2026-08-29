require "./spec_helper"

private class FakeOAuth2TokenProvider < Crabbit::OAuth2TokenProvider
  getter requests = 0

  @mutex = Mutex.new

  def initialize(
    @tokens : Array(Tuple(String, Time::Span)),
    @request_delay : Time::Span = Time::Span.zero,
  )
  end

  def request : Crabbit::OAuth2Token
    sleep @request_delay if @request_delay > Time::Span.zero
    value, lifetime = @mutex.synchronize do
      @requests += 1
      @tokens.shift? || raise Crabbit::OAuth2Error.new("no fake OAuth 2 token available")
    end
    Crabbit::OAuth2Token.new(value, Time.instant + lifetime)
  end
end

private class FlakyOAuth2TokenProvider < Crabbit::OAuth2TokenProvider
  getter requests = 0

  @mutex = Mutex.new

  def request : Crabbit::OAuth2Token
    request = @mutex.synchronize do
      @requests += 1
      @requests
    end
    case request
    when 1
      Crabbit::OAuth2Token.new("token-1", Time.instant + 200.milliseconds)
    when 2
      raise Crabbit::OAuth2Error.new("temporary identity provider outage")
    else
      Crabbit::OAuth2Token.new("token-2", Time.instant + 5.seconds)
    end
  end
end

private def oauth_config(
  endpoint : String,
  *,
  refresh_ratio : Float64 = 0.8,
  refresh_retry_delay : Time::Span = 20.milliseconds,
) : Crabbit::OAuth2Config
  Crabbit::OAuth2Config.new(
    endpoint,
    client_id: "client",
    client_secret: "secret",
    parameters: {"audience" => "rabbitmq"},
    refresh_ratio: refresh_ratio,
    refresh_retry_delay: refresh_retry_delay,
    allow_insecure_transport: endpoint.starts_with?("http://"),
  )
end

describe Crabbit::OAuth2HttpTokenProvider do
  it "requests and parses an RFC 6749 access token" do
    request = Channel(Tuple(String?, String)).new(1)
    server = HTTP::Server.new do |context|
      request.send({context.request.headers["Authorization"]?, context.request.body.not_nil!.gets_to_end})
      context.response.content_type = "application/json"
      context.response.print(%({"access_token":"token-value","expires_in":60}))
    end
    address = server.bind_tcp("127.0.0.1", 0)
    spawn { server.listen }

    begin
      provider = Crabbit::OAuth2HttpTokenProvider.new(
        oauth_config("http://127.0.0.1:#{address.port}/oauth/token"),
      )
      before = Time.instant
      token = provider.request
      authorization, body = request.receive

      authorization.should eq("Basic #{Base64.strict_encode("client:secret")}")
      URI::Params.parse(body)["grant_type"].should eq("client_credentials")
      URI::Params.parse(body)["audience"].should eq("rabbitmq")
      token.value.should eq("token-value")
      token.expires_at.should be > before + 59.seconds
    ensure
      server.close
    end
  end

  it "form-encodes OAuth Basic client credentials" do
    request = Channel(String?).new(1)
    server = HTTP::Server.new do |context|
      request.send(context.request.headers["Authorization"]?)
      context.response.content_type = "application/json"
      context.response.print(%({"access_token":"token-value","expires_in":60}))
    end
    address = server.bind_tcp("127.0.0.1", 0)

    begin
      spawn { server.listen }
      config = Crabbit::OAuth2Config.new(
        "http://127.0.0.1:#{address.port}/oauth/token",
        client_id: "client:id +λ",
        client_secret: "secret:% +λ",
        allow_insecure_transport: true,
      )
      Crabbit::OAuth2HttpTokenProvider.new(config).request

      encoded_id = URI.encode_www_form(config.client_id)
      encoded_secret = URI.encode_www_form(config.client_secret)
      request.receive.should eq("Basic #{Base64.strict_encode("#{encoded_id}:#{encoded_secret}")}")
    ensure
      server.close
    end
  end

  it "rejects a non-JSON token response" do
    server = HTTP::Server.new do |context|
      context.response.content_type = "text/plain"
      context.response.print("not-json")
    end
    address = server.bind_tcp("127.0.0.1", 0)
    spawn { server.listen }

    begin
      provider = Crabbit::OAuth2HttpTokenProvider.new(
        oauth_config("http://127.0.0.1:#{address.port}/oauth/token"),
      )
      expect_raises(Crabbit::OAuth2Error, /did not return JSON/) { provider.request }
    ensure
      server.close
    end
  end
end

describe Crabbit::OAuth2SaslAuthenticator do
  it "shares a token and pushes refreshed credentials to registrations" do
    provider = FakeOAuth2TokenProvider.new([
      {"token-1", 200.milliseconds},
      {"token-2", 5.seconds},
    ])
    authenticator = Crabbit::OAuth2SaslAuthenticator.new(
      oauth_config(
        "http://localhost/token",
        refresh_ratio: 0.25,
        refresh_retry_delay: 20.milliseconds,
      ),
      provider,
    )
    refreshed = Channel(Bytes).new(1)
    registration = authenticator.register("connection-1") do |credentials|
      refreshed.send(credentials)
    end

    String.new(registration.initial_response).should eq("\0\0token-1")
    registration.ready!
    update = select
    when credentials = refreshed.receive
      credentials
    when timeout(2.seconds)
      raise "OAuth 2 refresh did not arrive"
    end
    String.new(update).should eq("\0\0token-2")
    provider.requests.should eq(2)
    authenticator.registration_count.should eq(1)

    registration.close
    authenticator.registration_count.should eq(0)
  end

  it "does not refresh a registration until its connection is ready" do
    provider = FakeOAuth2TokenProvider.new([
      {"token-1", 80.milliseconds},
      {"token-2", 5.seconds},
    ])
    authenticator = Crabbit::OAuth2SaslAuthenticator.new(
      oauth_config("http://localhost/token", refresh_ratio: 0.25),
      provider,
    )
    refreshed = Channel(Bytes).new(1)
    registration = authenticator.register("connecting") { |credentials| refreshed.send(credentials) }

    String.new(registration.initial_response).should eq("\0\0token-1")
    sleep 100.milliseconds
    provider.requests.should eq(1)
    select
    when refreshed.receive
      raise "OAuth 2 refreshed a connection before it became ready"
    when timeout(10.milliseconds)
    end

    registration.ready!
    update = select
    when credentials = refreshed.receive
      credentials
    when timeout(2.seconds)
      raise "OAuth 2 refresh did not start after the connection became ready"
    end
    String.new(update).should eq("\0\0token-2")
    registration.close
  end

  it "retries a failed live re-authentication without requesting another token" do
    provider = FakeOAuth2TokenProvider.new([
      {"token-1", 200.milliseconds},
      {"token-2", 5.seconds},
    ])
    authenticator = Crabbit::OAuth2SaslAuthenticator.new(
      oauth_config("http://localhost/token", refresh_ratio: 0.2),
      provider,
    )
    attempts = 0
    attempts_mutex = Mutex.new
    refreshed = Channel(Bytes).new(1)
    registration = authenticator.register("flaky-connection") do |credentials|
      attempt = attempts_mutex.synchronize do
        attempts += 1
        attempts
      end
      raise IO::Error.new("transient re-authentication failure") if attempt == 1
      refreshed.send(credentials)
    end

    registration.initial_response
    registration.ready!
    update = select
    when credentials = refreshed.receive
      credentials
    when timeout(2.seconds)
      raise "OAuth 2 re-authentication was not retried"
    end
    String.new(update).should eq("\0\0token-2")
    attempts_mutex.synchronize { attempts }.should eq(2)
    provider.requests.should eq(2)
    deadline = Time.instant + 1.second
    until authenticator.registrations_current?
      raise "OAuth 2 registration did not become current" if Time.instant >= deadline
      sleep 1.millisecond
    end
    registration.close
  end

  it "retries token endpoint failures with the active connection registered" do
    provider = FlakyOAuth2TokenProvider.new
    authenticator = Crabbit::OAuth2SaslAuthenticator.new(
      oauth_config("http://localhost/token", refresh_ratio: 0.2),
      provider,
    )
    refreshed = Channel(Bytes).new(1)
    registration = authenticator.register("connection-1") { |credentials| refreshed.send(credentials) }

    String.new(registration.initial_response).should eq("\0\0token-1")
    registration.ready!
    update = select
    when credentials = refreshed.receive
      credentials
    when timeout(2.seconds)
      raise "OAuth 2 token retrieval was not retried"
    end

    String.new(update).should eq("\0\0token-2")
    provider.requests.should eq(3)
    registration.close
  end

  it "uses a single token request for concurrent connections" do
    provider = FakeOAuth2TokenProvider.new([
      {"shared-token", 5.seconds},
    ], 50.milliseconds)
    authenticator = Crabbit::OAuth2SaslAuthenticator.new(
      oauth_config("http://localhost/token"),
      provider,
    )
    first = authenticator.register("first") { |_credentials| }
    second = authenticator.register("second") { |_credentials| }
    responses = Channel(Bytes).new(2)

    spawn { responses.send(first.initial_response) }
    spawn { responses.send(second.initial_response) }
    2.times { String.new(responses.receive).should eq("\0\0shared-token") }
    provider.requests.should eq(1)

    first.close
    second.close
  end

  it "uses one shared authenticator for every connection in a configuration" do
    oauth2 = oauth_config("https://identity.example/token")
    configuration = Crabbit::Configuration.new(
      endpoints: [Crabbit::Endpoint.new("rabbitmq.example", 5551, true)],
      oauth2: oauth2,
      tls: Crabbit::TLSConfig.new,
    )

    configuration.authenticator.should be_a(Crabbit::OAuth2SaslAuthenticator)
    configuration.authenticator.same?(configuration.authenticator).should be_true
  end

  it "validates OAuth 2 configuration conflicts" do
    oauth2 = Crabbit::OAuth2Config.new(
      "https://identity.example/token",
      client_id: "client",
      client_secret: "secret",
      allow_insecure_transport: true,
    )
    expect_raises(Crabbit::ConfigurationError, /cannot both/) do
      Crabbit::Configuration.new(
        oauth2: oauth2,
        sasl_authenticator: Crabbit::PlainSaslAuthenticator.new("guest", "guest"),
      )
    end
    expect_raises(Crabbit::ConfigurationError, /requires the PLAIN/) do
      Crabbit::Configuration.new(oauth2: oauth2, sasl: Crabbit::SaslMechanism::External)
    end
    expect_raises(Crabbit::ConfigurationError, /refresh_ratio/) do
      oauth_config("https://identity.example/token", refresh_ratio: 0.0)
    end
    expect_raises(Crabbit::ConfigurationError, /refresh_ratio/) do
      oauth_config("https://identity.example/token", refresh_ratio: 1.0)
    end
    expect_raises(Crabbit::ConfigurationError, /refresh_retry_max_delay/) do
      Crabbit::OAuth2Config.new(
        "https://identity.example/token",
        client_id: "client",
        client_secret: "secret",
        refresh_retry_delay: 2.seconds,
        refresh_retry_max_delay: 1.second,
      )
    end
  end

  it "requires secure token and Stream transports by default" do
    expect_raises(Crabbit::ConfigurationError, /must use https/) do
      Crabbit::OAuth2Config.new(
        "http://identity.example/token",
        client_id: "client",
        client_secret: "secret",
      )
    end

    oauth2 = Crabbit::OAuth2Config.new(
      "https://identity.example/token",
      client_id: "client",
      client_secret: "secret",
    )
    expect_raises(Crabbit::ConfigurationError, /requires TLS/) do
      Crabbit::Configuration.new(oauth2: oauth2)
    end
    Crabbit::Configuration.new(
      endpoints: [Crabbit::Endpoint.new("rabbitmq.example", 5551, true)],
      oauth2: oauth2,
      tls: Crabbit::TLSConfig.new,
    ).oauth2.should eq(oauth2)
  end
end
