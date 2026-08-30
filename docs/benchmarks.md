# Benchmarks

Crabbit includes repeatable codec and broker benchmarks. Their results are
orientation points for local comparisons, not throughput guarantees or release
thresholds. Broker scheduling, container limits, CPU power management, GC, TLS,
payload shape, and RabbitMQ topology can all materially change the result.

## Reproducing the benchmarks

Build the benchmark executable in release mode:

```bash
shards build --release crabbit-benchmark
```

Run the in-process AMQP codec benchmark:

```bash
CRABBIT_BENCH_MESSAGES=500000 \
CRABBIT_BENCH_PAYLOAD=1024 \
  bin/crabbit-benchmark codec
```

Run the publish-confirm benchmark against the pinned local broker:

```bash
docker compose up --detach --wait rabbitmq

CRABBIT_BENCH_MESSAGES=500000 \
CRABBIT_BENCH_PAYLOAD=1024 \
CRABBIT_BENCH_BATCH=100 \
CRABBIT_BENCH_MAX_UNCONFIRMED=10000 \
  bin/crabbit-benchmark broker
```

`benchmarks/compare.sh` builds Crabbit and runs the matching RabbitMQ Java
Stream client workload. Every result is one newline-delimited JSON object.

## Reference snapshot

The following ranges are three local runs measured on 2026-08-30 at commit
`7dd21d4`. The machine was an x86-64 WSL2 environment with an Intel Core Ultra
7 255H and 16 visible logical CPUs. Crystal results used Crystal 1.21.0. Broker
results used RabbitMQ 4.3.5 in Docker on the same host without TLS. The Java
comparison used RabbitMQ Stream Java client 1.9.0 on JDK 21 through Docker.

These numbers should be read as approximate ranges. In particular, broker and
JVM results varied substantially between otherwise identical runs.

### AMQP codec

The codec workload processes 500,000 messages with a 1 KiB Data payload. The
sections cases additionally contain a header, properties, and application
properties.

| Operation | Approximate operations/s | Allocated per operation |
| --- | ---: | ---: |
| Encode to a new `Bytes` | 0.50–0.53 M | 2,248 B |
| Encode to a reused `IO` | 14.4–16.7 M | 0 B |
| Encode sections to a reused `IO` | 3.41–3.55 M | 0 B |
| Decode with safe binary copies | 1.83–2.62 M | 1,568 B |
| Decode Data with `zero_copy: true` | 6.94–8.35 M | 224 B |
| Decode sections with `zero_copy: true` | 1.68–2.34 M | about 992 B |

Direct-to-IO encoding reuses one `IO::Memory`; its zero allocation result does
not imply that an arbitrary destination IO or its downstream transport never
allocates. Zero-copy decoding keeps slices into the encoded input alive and is
safe only while that input remains immutable and available.

### Broker publish and confirmation

The broker workload publishes 500,000 messages with a 1 KiB payload, fixed
batches of 100, at most 10,000 unconfirmed messages, and waits for broker
confirmations before stopping the timer.

| Variant | Approximate messages/s | Crystal allocation/message | What it isolates |
| --- | ---: | ---: | --- |
| `bytes-throughput` | 280–286 k | about 1,812 B | Owned `Bytes` Data payload, no user callback |
| `raw-throughput` | 262–333 k | about 468 B | Pre-encoded, non-copying `RawMessage` path |
| `bytes-callback-counter` | 242–297 k | about 1,990 B | Library callback dispatch without latency samples |
| `bytes-callback` | 229–290 k | about 2,038 B | Callback dispatch and per-message latency recording |
| RabbitMQ Java client 1.9.0 | 198–237 k | not measured | Matching full publish-confirm workload |

The Java row is a comparison harness, not an official RabbitMQ performance
claim. Both full callback workloads use a 1 KiB payload, fixed batch size 100,
10,000 outstanding confirmations, and record one latency value per confirmed
message. The Java producer explicitly disables dynamic batching.

## Interpreting results

- Compare release builds on the same machine and broker, and use repeated runs.
- Treat allocation figures as more stable than broker wall-clock throughput.
- `publish(Bytes)` owns its input by copying it before returning; that copy is
  intentionally included in the benchmark.
- `RawMessage(copy: false)` measures a different ownership contract and starts
  with a complete AMQP-encoded message.
- Latency percentiles measure time spent behind earlier queued work as well as
  network and broker latency; they are not single-message round-trip tests.
- Results from TLS, replicas, remote brokers, filtering, compression, or
  sub-entry batching are not represented by this snapshot.

Use `CRABBIT_BENCH_VARIANT`, `CRABBIT_BENCH_MESSAGES`,
`CRABBIT_BENCH_PAYLOAD`, `CRABBIT_BENCH_BATCH`,
`CRABBIT_BENCH_MAX_UNCONFIRMED`, `CRABBIT_BENCH_URI`, and
`CRABBIT_BENCH_STREAM` to define a workload appropriate for an application.
