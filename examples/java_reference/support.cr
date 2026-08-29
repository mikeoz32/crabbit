require "../../src/crabbit"

module JavaReferenceExamples
  URI = ENV["CRABBIT_STREAM_URI"]? ||
        "rabbitmq-stream://guest:guest@localhost:5552/%2f"

  extend self

  def environment : Crabbit::Environment
    Crabbit::Environment.connect(URI)
  end

  def unique_name(prefix : String) : String
    "crabbit-#{prefix}-#{Process.pid}-#{Time.utc.to_unix_ms}-#{Random.rand(1_000_000)}"
  end

  def await(channel : Channel(T), timeout_span : Time::Span = 20.seconds) : T forall T
    select
    when value = channel.receive
      value
    when timeout(timeout_span)
      raise Crabbit::TimeoutError.new("example timed out after #{timeout_span}")
    end
  end

  def await_count(
    channel : Channel(T),
    count : Int,
    timeout_span : Time::Span = 20.seconds,
  ) : Array(T) forall T
    deadline = Time.instant + timeout_span
    Array(T).new(count) do
      remaining = deadline - Time.instant
      raise Crabbit::TimeoutError.new("example timed out after #{timeout_span}") if remaining <= Time::Span.zero
      await(channel, remaining)
    end
  end

  def confirmed!(confirmation : Crabbit::Confirmation) : Nil
    return if confirmation.confirmed
    raise confirmation.error || Crabbit::BrokerError.new(
      confirmation.code || Crabbit::ResponseCode::InternalError,
    )
  end

  def string_property(message : Crabbit::Message, key : String) : String?
    value = message.application_properties[key]?
    return unless value
    value.payload.as?(String)
  end

  def delete_stream(environment : Crabbit::Environment, stream : String) : Nil
    environment.delete_stream(stream)
  rescue Crabbit::BrokerError | Crabbit::ConnectionError
  end

  def delete_super_stream(environment : Crabbit::Environment, stream : String) : Nil
    environment.delete_super_stream(stream)
  rescue Crabbit::BrokerError | Crabbit::ConnectionError
  end
end
