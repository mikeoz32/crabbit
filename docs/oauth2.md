# OAuth 2 authentication

Crabbit supports OAuth 2 access-token retrieval, proactive refresh, and live re-authentication of open RabbitMQ Stream connections. One authenticator and token are shared by all connections in an environment.

## Client credentials

```crystal
oauth2 = Crabbit::OAuth2Config.new(
  "https://identity.example/oauth/token",
  client_id: "billing-service",
  client_secret: ENV["OAUTH2_CLIENT_SECRET"],
  parameters: {
    "audience" => "rabbitmq",
    "scope"    => "rabbitmq.read:*/* rabbitmq.write:*/*",
  },
)

environment = Crabbit::Environment.connect(
  "rabbitmq-stream+tls://rabbitmq.example:5551/%2f",
  oauth2: oauth2,
)
```

The default HTTP provider sends a form request containing `grant_type=client_credentials` plus custom parameters. Client credentials use HTTP Basic authentication. A successful response must be JSON with a non-empty `access_token` and positive numeric `expires_in`.

Provider-specific grants can change `grant_type` and parameters, as long as the endpoint accepts the same HTTP response contract.

## Transport security

By default:

- the token endpoint must use HTTPS;
- every RabbitMQ Stream endpoint must use TLS;
- the token endpoint and Stream endpoint perform certificate verification.

The identity provider and RabbitMQ may use different certificate authorities. Configure token-endpoint trust independently:

```crystal
identity_tls = OpenSSL::SSL::Context::Client.new
identity_tls.ca_certificates = "/etc/my-app/identity-ca.pem"

rabbit_tls = OpenSSL::SSL::Context::Client.new
rabbit_tls.ca_certificates = "/etc/my-app/rabbitmq-ca.pem"

oauth2 = Crabbit::OAuth2Config.new(
  "https://identity.example/oauth/token",
  client_id: "billing-service",
  client_secret: ENV["OAUTH2_CLIENT_SECRET"],
  tls_context: identity_tls,
)

configuration = Crabbit::Configuration.new(
  endpoints: [Crabbit::Endpoint.tls("rabbitmq.example")],
  oauth2: oauth2,
  tls: Crabbit::TLSConfig.new(rabbit_tls),
)
```

`allow_insecure_transport: true` permits HTTP token endpoints and plain Stream endpoints only for isolated local testing. It exposes bearer credentials and must not be used in production.

## Refresh lifecycle

The token is retrieved lazily when the first Stream connection authenticates. If the provider advertises lifetime `L`, refresh starts after `L * refresh_ratio`; the default ratio is `0.8`.

After installing a new token, Crabbit sends SASL Authenticate on every active Stream connection. A connection remains registered until it closes. Token retrieval and connection re-authentication failures are retried independently with exponential backoff capped by `refresh_retry_max_delay`.

Failure behavior is intentionally non-destructive:

- a failed proactive token refresh keeps the current token and schedules another attempt;
- a failed re-authentication retries stale connections while successful ones remain current;
- a token that is already expired when returned is rejected;
- an initial token request failure prevents that connection from authenticating.

## Timeouts

`OAuth2Config#connection_timeout` limits opening the identity-provider connection. `request_timeout` applies to HTTP reads and writes. These are independent from RabbitMQ `Configuration` timeouts.

## Custom token provider

Implement `OAuth2TokenProvider` for non-HTTP grants or integration with an external credential agent:

```crystal
class CredentialAgentProvider < Crabbit::OAuth2TokenProvider
  def request : Crabbit::OAuth2Token
    token, lifetime = CredentialAgent.fetch
    Crabbit::OAuth2Token.new(token, Time.instant + lifetime)
  rescue ex
    raise Crabbit::OAuth2Error.new("credential agent failed: #{ex.message}")
  end
end

config = Crabbit::OAuth2Config.new(
  "https://identity.invalid/oauth/token",
  client_id: "unused-by-agent",
  client_secret: "unused-by-agent",
)
authenticator = Crabbit::OAuth2SaslAuthenticator.new(
  config,
  CredentialAgentProvider.new,
)

configuration = Crabbit::Configuration.new(
  endpoints: [Crabbit::Endpoint.tls("rabbitmq.example")],
  tls: Crabbit::TLSConfig.new(rabbit_tls),
  sasl_authenticator: authenticator,
)
```

Custom-provider configurations should still use verified TLS. When the authenticator is passed directly rather than through `oauth2:`, the application is responsible for enforcing that transport policy.
