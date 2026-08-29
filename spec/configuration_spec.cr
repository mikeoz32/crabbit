require "./spec_helper"

describe Crabbit::Configuration do
  it "parses stream URIs including the encoded default virtual host" do
    config = Crabbit::Configuration.parse("rabbitmq-stream://user:secret@broker:6000/%2f")
    config.username.should eq "user"
    config.password.should eq "secret"
    config.virtual_host.should eq "/"
    config.endpoints.should eq [Crabbit::Endpoint.new("broker", 6000, false)]
  end

  it "parses TLS stream URIs" do
    config = Crabbit::Configuration.parse("rabbitmq-stream+tls://broker/vhost")
    config.tls.should_not be_nil
    config.virtual_host.should eq "vhost"
    config.endpoints.first.tls.should be_true
    config.endpoints.first.port.should eq 5551
  end

  it "preserves a custom TLS configuration when parsing a URI" do
    context = OpenSSL::SSL::Context::Client.new
    tls = Crabbit::TLSConfig.new(context, verify_hostname: false)
    config = Crabbit::Configuration.parse("rabbitmq-stream+tls://broker/vhost", tls: tls)

    config.tls.not_nil!.context.same?(context).should be_true
    config.tls.not_nil!.verify_hostname.should be_false
  end

  it "rejects invalid transport limits" do
    expect_raises(Crabbit::ConfigurationError) do
      Crabbit::Configuration.new(endpoints: [Crabbit::Endpoint.new("localhost", 0)])
    end
    expect_raises(Crabbit::ConfigurationError) do
      Crabbit::Configuration.new(request_timeout: Time::Span.zero)
    end
  end
end
