require "json"
require "../src/crabbit"

module CrabbitBenchmark
  extend self

  def percentile(values : Array(Int64), percentile : Float64) : Int64
    return 0_i64 if values.empty?
    sorted = values.sort
    index = ((sorted.size - 1) * percentile).round.to_i
    sorted[index]
  end

  def report(
    name : String,
    count : Int32,
    payload_size : Int32,
    elapsed : Time::Span,
    allocated : UInt64,
    latencies : Array(Int64),
  ) : Nil
    seconds = elapsed.total_seconds
    result = {
      "implementation"          => "crabbit",
      "benchmark"               => name,
      "messages"                => count,
      "payload_bytes"           => payload_size,
      "messages_per_second"     => (count / seconds).round(2),
      "mib_per_second"          => ((count.to_f * payload_size / 1_048_576) / seconds).round(2),
      "elapsed_milliseconds"    => elapsed.total_milliseconds.round(2),
      "allocated_bytes"         => allocated,
      "allocated_per_message"   => (allocated.to_f / count).round(2),
      "latency_p50_nanoseconds" => percentile(latencies, 0.50),
      "latency_p95_nanoseconds" => percentile(latencies, 0.95),
      "latency_p99_nanoseconds" => percentile(latencies, 0.99),
    }
    puts result.to_json
  end

  def codec(count : Int32, payload_size : Int32) : Nil
    payload = Bytes.new(payload_size, 0x61_u8)
    message = Crabbit::Message.new(payload)
    encoded = message.to_amqp
    1_000.times { message.to_amqp }

    GC.collect
    before = GC.stats.total_bytes
    latencies = Array(Int64).new(count)
    started = Time.instant
    count.times do
      operation_started = Time.instant
      message.to_amqp
      latencies << (Time.instant - operation_started).total_nanoseconds.to_i64
    end
    elapsed = Time.instant - started
    report("amqp-encode-bytes", count, payload_size, elapsed, GC.stats.total_bytes - before, latencies)

    io = IO::Memory.new(encoded.size)
    1_000.times do
      io.clear
      message.to_amqp(io)
    end
    GC.collect
    before = GC.stats.total_bytes
    latencies.clear
    started = Time.instant
    count.times do
      operation_started = Time.instant
      io.clear
      message.to_amqp(io)
      latencies << (Time.instant - operation_started).total_nanoseconds.to_i64
    end
    elapsed = Time.instant - started
    report("amqp-encode-io", count, payload_size, elapsed, GC.stats.total_bytes - before, latencies)

    rich_message = Crabbit::Message.new(
      payload,
      header: Crabbit::Header.new(durable: true, priority: 5_u8, ttl: 60_000_u32),
      properties: Crabbit::Properties.new(
        message_id: "benchmark-message",
        subject: "codec",
        content_type: "application/octet-stream",
        creation_time: Time.unix_ms(1_700_000_000_000_i64),
      ),
      application_properties: {
        "attempt" => Crabbit::AMQP::Value.wrap(1_i32),
        "active"  => Crabbit::AMQP::Value.wrap(true),
      },
    )
    rich_encoded = rich_message.to_amqp
    rich_io = IO::Memory.new(rich_encoded.size)
    1_000.times do
      rich_io.clear
      rich_message.to_amqp(rich_io)
    end
    GC.collect
    before = GC.stats.total_bytes
    latencies.clear
    started = Time.instant
    count.times do
      operation_started = Time.instant
      rich_io.clear
      rich_message.to_amqp(rich_io)
      latencies << (Time.instant - operation_started).total_nanoseconds.to_i64
    end
    elapsed = Time.instant - started
    report("amqp-encode-io-sections", count, payload_size, elapsed, GC.stats.total_bytes - before, latencies)

    1_000.times { Crabbit::Message.from_amqp(encoded) }
    GC.collect
    before = GC.stats.total_bytes
    latencies.clear
    started = Time.instant
    count.times do
      operation_started = Time.instant
      Crabbit::Message.from_amqp(encoded)
      latencies << (Time.instant - operation_started).total_nanoseconds.to_i64
    end
    elapsed = Time.instant - started
    report("amqp-decode-copy", count, payload_size, elapsed, GC.stats.total_bytes - before, latencies)

    1_000.times { Crabbit::Message.from_amqp(encoded, zero_copy: true) }
    GC.collect
    before = GC.stats.total_bytes
    latencies.clear
    started = Time.instant
    count.times do
      operation_started = Time.instant
      Crabbit::Message.from_amqp(encoded, zero_copy: true)
      latencies << (Time.instant - operation_started).total_nanoseconds.to_i64
    end
    elapsed = Time.instant - started
    report("amqp-decode-zero-copy", count, payload_size, elapsed, GC.stats.total_bytes - before, latencies)

    1_000.times { Crabbit::Message.from_amqp(rich_encoded, zero_copy: true) }
    GC.collect
    before = GC.stats.total_bytes
    latencies.clear
    started = Time.instant
    count.times do
      operation_started = Time.instant
      Crabbit::Message.from_amqp(rich_encoded, zero_copy: true)
      latencies << (Time.instant - operation_started).total_nanoseconds.to_i64
    end
    elapsed = Time.instant - started
    report("amqp-decode-zero-copy-sections", count, payload_size, elapsed, GC.stats.total_bytes - before, latencies)
  end

  def broker(
    count : Int32,
    payload_size : Int32,
    batch_size : Int32,
    max_unconfirmed : Int32,
    variant : String,
  ) : Nil
    uri = ENV["CRABBIT_BENCH_URI"]? || "rabbitmq-stream://crabbit:crabbit@localhost:5552/%2f"
    stream = ENV["CRABBIT_BENCH_STREAM"]? || "crabbit-benchmark"
    environment = Crabbit::Environment.connect(uri)
    begin
      environment.create_stream(stream, Crabbit::StreamOptions.new(initial_cluster_size: 1))
    rescue ex : Crabbit::BrokerError
      raise ex unless ex.code.stream_already_exists?
    end
    producer = environment.producer(
      stream,
      Crabbit::ProducerOptions.new(
        batch_size: batch_size,
        max_unconfirmed: max_unconfirmed,
        confirm_timeout: 60.seconds,
      ),
    )
    payload = Bytes.new(payload_size, 0x61_u8)
    raw_message = Crabbit::RawMessage.new(Crabbit::Message.new(payload).to_amqp, copy: false)
    before = GC.stats.total_bytes
    started = Time.instant

    latencies = case variant
                when "bytes-callback"
                  callback_latencies = Channel(Int64).new(count)
                  count.times do
                    message_started = Time.instant
                    producer.publish(payload) do |confirmation|
                      raise confirmation.error.not_nil! unless confirmation.confirmed
                      callback_latencies.send((Time.instant - message_started).total_nanoseconds.to_i64)
                    end
                  end
                  Array(Int64).new(count) { callback_latencies.receive }
                when "bytes-throughput"
                  count.times { producer.publish(payload) }
                  producer.wait_for_confirms(60.seconds)
                  [] of Int64
                when "bytes-callback-counter"
                  confirmed = Atomic(Int32).new(0)
                  done = Channel(Nil).new(1)
                  callback = ->(confirmation : Crabbit::Confirmation) do
                    raise confirmation.error.not_nil! unless confirmation.confirmed
                    done.send(nil) if confirmed.add(1, :relaxed) == count - 1
                    nil
                  end
                  count.times { producer.publish(payload, &callback) }
                  done.receive
                  [] of Int64
                when "bytes-callback-channel"
                  completion_signals = Channel(Nil).new(count)
                  count.times do
                    producer.publish(payload) do |confirmation|
                      raise confirmation.error.not_nil! unless confirmation.confirmed
                      completion_signals.send(nil)
                    end
                  end
                  count.times { completion_signals.receive }
                  [] of Int64
                when "raw-throughput"
                  count.times { producer.publish(raw_message) }
                  producer.wait_for_confirms(60.seconds)
                  [] of Int64
                else
                  raise ArgumentError.new(
                    "unknown CRABBIT_BENCH_VARIANT #{variant.inspect}; expected bytes-callback, " \
                    "bytes-throughput, bytes-callback-counter, bytes-callback-channel, or raw-throughput"
                  )
                end
    elapsed = Time.instant - started
    report("publish-confirm-#{variant}", count, payload_size, elapsed, GC.stats.total_bytes - before, latencies)
    producer.close
    environment.delete_stream(stream)
    environment.close
  end

  count = (ENV["CRABBIT_BENCH_MESSAGES"]? || "100000").to_i
  payload_size = (ENV["CRABBIT_BENCH_PAYLOAD"]? || "1024").to_i
  batch_size = (ENV["CRABBIT_BENCH_BATCH"]? || "100").to_i
  max_unconfirmed = (ENV["CRABBIT_BENCH_MAX_UNCONFIRMED"]? || "10000").to_i
  variant = ENV["CRABBIT_BENCH_VARIANT"]? || "bytes-callback"
  mode = ARGV.first? || "codec"
  case mode
  when "codec"
    codec(count, payload_size)
  when "broker"
    broker(count, payload_size, batch_size, max_unconfirmed, variant)
  else
    STDERR.puts "usage: crabbit-benchmark [codec|broker]"
    exit 2
  end
end
