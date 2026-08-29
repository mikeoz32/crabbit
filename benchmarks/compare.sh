#!/usr/bin/env bash
set -euo pipefail

benchmark_count="${CRABBIT_BENCH_MESSAGES:-500000}"
benchmark_payload="${CRABBIT_BENCH_PAYLOAD:-1024}"
benchmark_batch="${CRABBIT_BENCH_BATCH:-100}"
benchmark_unconfirmed="${CRABBIT_BENCH_MAX_UNCONFIRMED:-10000}"
benchmark_uri="${CRABBIT_BENCH_URI:-rabbitmq-stream://crabbit:crabbit@localhost:5552/%2f}"
run_id="$(date +%s)-$$"

shards build --release crabbit-benchmark
CRABBIT_BENCH_MESSAGES="$benchmark_count" \
CRABBIT_BENCH_PAYLOAD="$benchmark_payload" \
CRABBIT_BENCH_BATCH="$benchmark_batch" \
CRABBIT_BENCH_MAX_UNCONFIRMED="$benchmark_unconfirmed" \
CRABBIT_BENCH_URI="$benchmark_uri" \
CRABBIT_BENCH_STREAM="crabbit-benchmark-$run_id" \
  bin/crabbit-benchmark broker

if command -v mvn >/dev/null 2>&1; then
  CRABBIT_BENCH_MESSAGES="$benchmark_count" \
  CRABBIT_BENCH_PAYLOAD="$benchmark_payload" \
  CRABBIT_BENCH_BATCH="$benchmark_batch" \
  CRABBIT_BENCH_MAX_UNCONFIRMED="$benchmark_unconfirmed" \
  CRABBIT_BENCH_URI="$benchmark_uri" \
  CRABBIT_BENCH_STREAM="java-benchmark-$run_id" \
    mvn --quiet --file benchmarks/java/pom.xml compile exec:java
elif command -v docker >/dev/null 2>&1; then
  repository_dir="$(pwd)"
  java_image="${CRABBIT_JAVA_BENCH_IMAGE:-maven:3.9.11-eclipse-temurin-21-alpine}"
  docker run --rm --network host \
    -e CRABBIT_BENCH_MESSAGES="$benchmark_count" \
    -e CRABBIT_BENCH_PAYLOAD="$benchmark_payload" \
    -e CRABBIT_BENCH_BATCH="$benchmark_batch" \
    -e CRABBIT_BENCH_MAX_UNCONFIRMED="$benchmark_unconfirmed" \
    -e CRABBIT_BENCH_URI="$benchmark_uri" \
    -e CRABBIT_BENCH_STREAM="java-benchmark-$run_id" \
    -v "$repository_dir:/workspace" \
    -w /workspace \
    "$java_image" \
    mvn --quiet --file benchmarks/java/pom.xml compile exec:java
else
  echo "Java comparison requires Maven or Docker" >&2
  exit 2
fi
