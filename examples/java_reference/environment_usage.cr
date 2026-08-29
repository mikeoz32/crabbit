require "./support"

# Environment creation with the default URI and with one explicit URI.
default_configuration = Crabbit::Configuration.parse
explicit_configuration = Crabbit::Configuration.parse(
  "rabbitmq-stream://guest:guest@localhost:5552/%2f",
)

# Java's `uris(...)` maps to an explicit endpoint list in Crabbit.
cluster_configuration = Crabbit::Configuration.new(
  endpoints: [
    Crabbit::Endpoint.new("host1", 5552),
    Crabbit::Endpoint.new("host2", 5552),
    Crabbit::Endpoint.new("host3", 5552),
  ],
)

# Java's address resolver example describes a TCP load balancer. Crabbit keeps
# the advertised broker address for matching and opens sockets via these
# entrypoints when `load_balancer` is enabled.
load_balancer_configuration = Crabbit::Configuration.new(
  endpoints: [Crabbit::Endpoint.new("my-load-balancer", 5552)],
  load_balancer: true,
)

# A custom CA context is used when CRABBIT_EXAMPLE_CA_CERT points to a PEM file.
if ca_certificate = ENV["CRABBIT_EXAMPLE_CA_CERT"]?
  context = OpenSSL::SSL::Context::Client.new
  context.ca_certificates = ca_certificate
  Crabbit::Configuration.parse(
    "rabbitmq-stream+tls://guest:guest@localhost:5551/%2f",
    tls: Crabbit::TLSConfig.new(context),
  )
end

# The trust-everything form is useful only for local development.
insecure_context = OpenSSL::SSL::Context::Client.new
insecure_context.verify_mode = OpenSSL::SSL::VerifyMode::NONE
insecure_tls_configuration = Crabbit::Configuration.parse(
  "rabbitmq-stream+tls://guest:guest@localhost:5551/%2f",
  tls: Crabbit::TLSConfig.new(insecure_context, verify_hostname: false),
)

# OAuth 2 client-credentials retrieval, shared refresh, and connection
# re-authentication are configured at the environment level.
oauth2_configuration = Crabbit::Configuration.parse(
  "rabbitmq-stream+tls://localhost:5551/%2f",
  oauth2: Crabbit::OAuth2Config.new(
    "https://localhost:8443/uaa/oauth/token/",
    client_id: "rabbitmq",
    client_secret: "rabbitmq",
    grant_type: "password",
    parameters: {
      "username" => "rabbit_super",
      "password" => "rabbit_super",
    },
  ),
)

# Keep the examples observable so the compiler cannot discard their setup.
{
  default_configuration,
  explicit_configuration,
  cluster_configuration,
  load_balancer_configuration,
  insecure_tls_configuration,
  oauth2_configuration,
}.each { |configuration| raise "missing endpoint" if configuration.endpoints.empty? }

environment = JavaReferenceExamples.environment
plain_stream = JavaReferenceExamples.unique_name("environment")
size_stream = JavaReferenceExamples.unique_name("retention-size")
age_stream = JavaReferenceExamples.unique_name("retention-age")

begin
  environment.create_stream(plain_stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))
  environment.create_stream(
    size_stream,
    Crabbit::StreamOptions.new(
      max_length_bytes: 10_i64 * 1024 * 1024 * 1024,
      segment_size_bytes: 500_i64 * 1024 * 1024,
      initial_cluster_size: 1,
    ),
  )
  environment.create_stream(
    age_stream,
    Crabbit::StreamOptions.new(
      max_age: 6.hours,
      segment_size_bytes: 500_i64 * 1024 * 1024,
      initial_cluster_size: 1,
    ),
  )
  puts "Created plain, size-retained, and time-retained streams"
ensure
  JavaReferenceExamples.delete_stream(environment, plain_stream)
  JavaReferenceExamples.delete_stream(environment, size_stream)
  JavaReferenceExamples.delete_stream(environment, age_stream)
  environment.close
end
