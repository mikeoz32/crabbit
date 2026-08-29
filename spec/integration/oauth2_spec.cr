require "../spec_helper"
require "file/tempfile"
require "openssl/hmac"

private OAUTH2_SIGNING_KEY = "crabbit-oauth2-integration-secret"

private def oauth2_base64(value : String | Bytes) : String
  Base64.urlsafe_encode(value, padding: false)
end

private def oauth2_jwt(scope : String, expires_in : Time::Span = 30.seconds) : String
  header = JSON.build do |json|
    json.object do
      json.field "alg", "HS256"
      json.field "kid", "crabbit-test-key"
      json.field "typ", "JWT"
    end
  end
  payload = JSON.build do |json|
    json.object do
      json.field "aud", "rabbitmq"
      json.field "sub", "crabbit-oauth2-integration"
      json.field "scope", scope
      json.field "iat", Time.utc.to_unix
      json.field "exp", (Time.utc + expires_in).to_unix
    end
  end
  signing_input = "#{oauth2_base64(header)}.#{oauth2_base64(payload)}"
  signature = OpenSSL::HMAC.digest(OpenSSL::Algorithm::SHA256, OAUTH2_SIGNING_KEY, signing_input)
  "#{signing_input}.#{oauth2_base64(signature)}"
end

private def wait_for_oauth2_refresh(
  authenticator : Crabbit::OAuth2SaslAuthenticator,
  generation : UInt64,
  timeout_span : Time::Span,
) : Nil
  deadline = Time.instant + timeout_span
  until authenticator.token_generation >= generation && authenticator.registrations_current?
    if Time.instant >= deadline
      raise Crabbit::TimeoutError.new(
        "OAuth 2 credentials did not reach generation #{generation}",
      )
    end
    sleep 10.milliseconds
  end
end

private def oauth2_tls_contexts : Tuple(
  OpenSSL::SSL::Context::Server,
  OpenSSL::SSL::Context::Client,
  String,
)
  directory = File.tempname("crabbit-oauth2-tls")
  Dir.mkdir(directory)
  certificate = File.join(directory, "token-server.crt")
  private_key = File.join(directory, "token-server.key")
  openssl_output = IO::Memory.new
  status = Process.run(
    "openssl",
    [
      "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
      "-subj", "/CN=127.0.0.1",
      "-addext", "subjectAltName=IP:127.0.0.1",
      "-keyout", private_key,
      "-out", certificate,
    ],
    output: openssl_output,
    error: openssl_output,
  )
  unless status.success?
    cleanup_oauth2_tls_contexts(directory)
    raise "could not generate OAuth 2 test certificate: #{openssl_output}"
  end

  server = OpenSSL::SSL::Context::Server.new
  server.certificate_chain = certificate
  server.private_key = private_key
  client = OpenSSL::SSL::Context::Client.new
  client.ca_certificates = certificate
  {server, client, directory}
end

private def cleanup_oauth2_tls_contexts(directory : String) : Nil
  Dir.each_child(directory) do |child|
    File.delete(File.join(directory, child))
  end
  Dir.delete(directory)
end

if ENV["CRABBIT_INTEGRATION"]? == "1"
  describe "RabbitMQ Stream OAuth 2 integration" do
    it "retrieves, refreshes, and re-authenticates an open connection" do
      requests = Channel(Int32).new(4)
      request_mutex = Mutex.new
      request_count = 0
      token_server = HTTP::Server.new do |context|
        context.request.headers["Authorization"]?.should eq(
          "Basic #{Base64.strict_encode("crabbit-client:crabbit-secret")}",
        )
        params = URI::Params.parse(context.request.body.not_nil!.gets_to_end)
        params["grant_type"].should eq("client_credentials")
        count = request_mutex.synchronize do
          request_count += 1
          request_count
        end
        scope = if count == 1
                  "rabbitmq.read:*/*"
                else
                  "rabbitmq.configure:*/* rabbitmq.read:*/* rabbitmq.write:*/*"
                end
        token = oauth2_jwt(scope)
        context.response.content_type = "application/json"
        context.response.print({access_token: token, expires_in: count == 1 ? 2 : 30}.to_json)
        requests.send(count)
      end
      address = token_server.bind_tcp("127.0.0.1", 0)
      spawn { token_server.listen }

      oauth2 = Crabbit::OAuth2Config.new(
        "http://127.0.0.1:#{address.port}/oauth/token",
        client_id: "crabbit-client",
        client_secret: "crabbit-secret",
        refresh_ratio: 0.1,
        refresh_retry_delay: 100.milliseconds,
        allow_insecure_transport: true,
      )
      environment = Crabbit::Environment.connect(
        "rabbitmq-stream://localhost:5553/%2f",
        oauth2: oauth2,
        request_timeout: 15.seconds,
      )
      authenticator = environment.configuration.authenticator.as(Crabbit::OAuth2SaslAuthenticator)
      stream = "crabbit-oauth2-#{Random.rand(UInt64).to_s(16)}"

      begin
        environment.metadata(["oauth2-connection-bootstrap"])
        requests.receive.should eq(1)
        select
        when refresh_request = requests.receive
          refresh_request.should eq(2)
        when timeout(5.seconds)
          raise Crabbit::TimeoutError.new("OAuth 2 token was not refreshed")
        end
        wait_for_oauth2_refresh(authenticator, 2_u64, 5.seconds)

        # The first JWT only had read permission. Creation through the same
        # locator connection proves the refreshed JWT was re-authenticated.
        environment.create_stream(stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))
        producer = environment.producer(stream)
        producer.publish("oauth2-refresh-ok").await(20.seconds).confirmed.should be_true
        producer.close
      ensure
        begin
          environment.delete_stream(stream)
        rescue Crabbit::Error
        end
        environment.close
        token_server.close
      end
      authenticator.registration_count.should eq(0)
    end

    it "refreshes multiple open connections through a verified HTTPS token endpoint" do
      requests = Channel(Int32).new(4)
      request_mutex = Mutex.new
      request_count = 0
      token_server = HTTP::Server.new do |context|
        count = request_mutex.synchronize do
          request_count += 1
          request_count
        end
        scope = if count == 1
                  "rabbitmq.read:*/*"
                else
                  "rabbitmq.configure:*/* rabbitmq.read:*/* rabbitmq.write:*/*"
                end
        context.response.content_type = "application/json"
        context.response.print({
          access_token: oauth2_jwt(scope),
          expires_in:   count == 1 ? 4 : 30,
        }.to_json)
        requests.send(count)
      end
      server_tls, client_tls, tls_directory = oauth2_tls_contexts
      address = token_server.bind_tls("127.0.0.1", 0, server_tls)
      spawn { token_server.listen }

      oauth2 = Crabbit::OAuth2Config.new(
        "https://127.0.0.1:#{address.port}/oauth/token",
        client_id: "crabbit-client",
        client_secret: "crabbit-secret",
        tls_context: client_tls,
        refresh_ratio: 0.2,
        refresh_retry_delay: 100.milliseconds,
        # The token endpoint is verified HTTPS. This opt-in is only for the
        # plaintext Stream listener of the isolated integration broker.
        allow_insecure_transport: true,
      )
      endpoint = Crabbit::Endpoint.new("localhost", 5553)
      configuration = Crabbit::Configuration.new(
        endpoints: [endpoint],
        oauth2: oauth2,
        request_timeout: 15.seconds,
      )
      authenticator = configuration.authenticator.as(Crabbit::OAuth2SaslAuthenticator)
      first_connection = Crabbit::Internal::Connection.new(configuration, endpoint).connect!
      second_connection = Crabbit::Internal::Connection.new(configuration, endpoint).connect!
      first = Crabbit::Internal::Client.new(first_connection)
      second = Crabbit::Internal::Client.new(second_connection)
      first_stream = "crabbit-oauth2-first-#{Random.rand(UInt64).to_s(16)}"
      second_stream = "crabbit-oauth2-second-#{Random.rand(UInt64).to_s(16)}"

      begin
        requests.receive.should eq(1)
        authenticator.registration_count.should eq(2)
        select
        when refresh_request = requests.receive
          refresh_request.should eq(2)
        when timeout(5.seconds)
          raise Crabbit::TimeoutError.new("OAuth 2 token was not refreshed")
        end
        wait_for_oauth2_refresh(authenticator, 2_u64, 5.seconds)

        # Both sockets authenticated with the initial read-only token. Creating
        # through each one proves both received the refreshed permissions.
        first.create_stream(first_stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))
        second.create_stream(second_stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))
      ensure
        begin
          first.delete_stream(first_stream)
        rescue Crabbit::Error
        end
        begin
          second.delete_stream(second_stream)
        rescue Crabbit::Error
        end
        first_connection.close
        second_connection.close
        token_server.close
        cleanup_oauth2_tls_contexts(tls_directory)
      end
      authenticator.registration_count.should eq(0)
    end
  end
end
