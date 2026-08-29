package io.crabbit;

import com.rabbitmq.stream.ConfirmationStatus;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Producer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class Bench {
  public static void main(String[] args) throws Exception {
    int count = Integer.parseInt(System.getenv().getOrDefault("CRABBIT_BENCH_MESSAGES", "100000"));
    int payloadSize = Integer.parseInt(System.getenv().getOrDefault("CRABBIT_BENCH_PAYLOAD", "1024"));
    int batchSize = Integer.parseInt(System.getenv().getOrDefault("CRABBIT_BENCH_BATCH", "100"));
    int maxUnconfirmed = Integer.parseInt(
        System.getenv().getOrDefault("CRABBIT_BENCH_MAX_UNCONFIRMED", "10000"));
    String uri = System.getenv().getOrDefault(
        "CRABBIT_BENCH_URI", "rabbitmq-stream://crabbit:crabbit@localhost:5552/%2f");
    String stream = System.getenv().getOrDefault("CRABBIT_BENCH_STREAM", "crabbit-benchmark");

    Environment environment = Environment.builder().uri(uri).build();
    try {
      environment.streamCreator().stream(stream).create();
    } catch (RuntimeException ignored) {
      // The Crystal benchmark normally creates the shared stream first.
    }
    Producer producer = environment.producerBuilder()
        .stream(stream)
        .batchSize(batchSize)
        .maxUnconfirmedMessages(maxUnconfirmed)
        .dynamicBatch(false)
        .build();
    byte[] payload = new byte[payloadSize];
    Arrays.fill(payload, (byte) 'a');
    CountDownLatch confirmations = new CountDownLatch(count);
    long[] latencies = new long[count];
    long started = System.nanoTime();
    for (int i = 0; i < count; i++) {
      final int index = i;
      final long messageStarted = System.nanoTime();
      Message message = producer.messageBuilder().addData(payload).build();
      producer.send(message, status -> {
        if (!status.isConfirmed()) {
          throw new IllegalStateException("publish failed with code " + status.getCode());
        }
        latencies[index] = System.nanoTime() - messageStarted;
        confirmations.countDown();
      });
    }
    if (!confirmations.await(60, TimeUnit.SECONDS)) {
      throw new IllegalStateException("publish confirmations timed out");
    }
    long elapsed = System.nanoTime() - started;
    Arrays.sort(latencies);
    double seconds = elapsed / 1_000_000_000.0;
    System.out.printf(
        "{\"implementation\":\"rabbitmq-stream-java\",\"benchmark\":\"publish-confirm\","
            + "\"messages\":%d,\"payload_bytes\":%d,\"messages_per_second\":%.2f,"
            + "\"mib_per_second\":%.2f,\"elapsed_milliseconds\":%.2f,"
            + "\"latency_p50_nanoseconds\":%d,\"latency_p95_nanoseconds\":%d,"
            + "\"latency_p99_nanoseconds\":%d}%n",
        count,
        payloadSize,
        count / seconds,
        (count * (double) payloadSize / 1_048_576) / seconds,
        elapsed / 1_000_000.0,
        percentile(latencies, 0.50),
        percentile(latencies, 0.95),
        percentile(latencies, 0.99));
    producer.close();
    environment.deleteStream(stream);
    environment.close();
  }

  private static long percentile(long[] values, double percentile) {
    int index = (int) Math.round((values.length - 1) * percentile);
    return values[index];
  }
}
