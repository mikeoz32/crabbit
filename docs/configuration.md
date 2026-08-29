# Configuration and connections

## URI form

For one entrypoint, pass a URI to `Environment.connect` or `Configuration.parse`:

```text
rabbitmq-stream://username:password@host:5552/virtual-host
rabbitmq-stream+tls://username:password@host:5551/virtual-host
```

Credentials and the virtual host are percent-decoded. The default virtual host `/` should be encoded as `/%2f` when it appears explicitly in a URI.

```crystal
environment = Crabbit::Environment.connect(
  "rabbitmq-stream://app:secret@rabbit.example:5552/%2f",
  heartbeat: 30.seconds,
  request_timeout: 15.seconds,
)
```

Named options are forwarded to `Configuration.new`.

## Multiple entrypoints

Construct `Configuration` directly for a cluster or DNS-independent seed list:

```crystal
configuration = Crabbit::Configuration.new(
  endpoints: [
    Crabbit::Endpoint.new("rabbit-a.example", 5552),
    Crabbit::Endpoint.new("rabbit-b.example", 5552),
    Crabbit::Endpoint.new("rabbit-c.example", 5552),
  ],
  username: "app",
  password: ENV["RABBITMQ_PASSWORD"],
  virtual_host: "/",
)

environment = Crabbit::Environment.new(configuration)
```

Crabbit rotates locator attempts through these endpoints. Producer connections prefer the stream leader. Consumer connections try shuffled replicas and then the leader, continuing after unreachable candidates.

## TLS

The TLS URI scheme creates a default verified OpenSSL client context:

```crystal
environment = Crabbit::Environment.connect(
  "rabbitmq-stream+tls://app:secret@rabbit.example:5551/%2f"
)
```

Configure a private CA through `TLSConfig`:

```crystal
context = OpenSSL::SSL::Context::Client.new
context.ca_certificates = "/etc/my-app/rabbitmq-ca.pem"

tls = Crabbit::TLSConfig.new(context, verify_hostname: true)
environment = Crabbit::Environment.connect(
  "rabbitmq-stream+tls://app:secret@rabbit.example:5551/%2f",
  tls: tls,
)
```

For mutual TLS, also set `context.certificate_chain` and `context.private_key`, then select `SaslMechanism::External` if RabbitMQ maps the client certificate through SASL EXTERNAL.

Do not disable hostname verification in production. `verify_hostname: false` is only appropriate for isolated development environments whose certificates cannot represent the test hostname.

## TCP load balancers

RabbitMQ metadata advertises the node that owns a leader or replica. Direct connections use that advertised address. When clients can only reach a TCP load balancer, enable load-balancer mode:

```crystal
configuration = Crabbit::Configuration.new(
  endpoints: [Crabbit::Endpoint.new("streams-lb.example", 5552)],
  username: "app",
  password: ENV["RABBITMQ_PASSWORD"],
  load_balancer: true,
)
```

Crabbit repeatedly opens connections through configured entrypoints until the selected backend's `advertised_host` and `advertised_port` match the metadata target. The load balancer must therefore distribute new TCP connections across broker nodes and RabbitMQ must advertise stable node addresses.

## Timeouts and frames

- `connection_timeout` limits TCP and TLS establishment.
- `request_timeout` limits correlated protocol requests such as metadata and management commands.
- `heartbeat` negotiates inactivity detection with RabbitMQ; zero disables heartbeats.
- `max_frame_size` is the requested inbound/outbound frame limit. Zero requests no local limit; a non-zero value must be at least 1 KiB.

Publisher confirmation and enqueue deadlines belong to `ProducerOptions`, not `Configuration`.

## Client properties

Crabbit sends `product`, `version`, `platform`, and `information` during Peer Properties negotiation. Add application-specific properties without losing the defaults:

```crystal
configuration = Crabbit::Configuration.new(
  client_properties: {
    "connection_name" => "billing-stream-publisher",
    "application"     => "billing-api",
  },
)
```

## SASL

PLAIN is the default. EXTERNAL uses an empty initial response and is typically paired with a TLS client certificate:

```crystal
configuration = Crabbit::Configuration.new(
  endpoints: [Crabbit::Endpoint.tls("rabbit.example")],
  tls: Crabbit::TLSConfig.new(context),
  sasl: Crabbit::SaslMechanism::External,
)
```

For a custom challenge-response mechanism, subclass `SaslAuthenticator` and implement `mechanism`, `initial_response`, and, when necessary, `challenge`.

OAuth 2 has additional transport rules and refresh behavior documented in [OAuth 2](oauth2.md).

## Fibers and execution contexts

All network waits use Crystal's non-blocking `IO`. Crabbit does not require an application-owned event loop. Crystal 1.20 can opt into execution contexts with `-Dpreview_mt -Dexecution_context`; Crystal 1.21 enables them by default. Confirmation callbacks are dispatched outside the connection reader and use a dedicated concurrent context where the feature is available.
