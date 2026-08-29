#!/usr/bin/env bash
set -euo pipefail

export CRABBIT_STREAM_URI="${CRABBIT_STREAM_URI:-rabbitmq-stream://crabbit:crabbit@localhost:5552/%2f}"

examples=(
  examples/java_reference/sample_application.cr
  examples/java_reference/environment_usage.cr
  examples/java_reference/producer_usage.cr
  examples/java_reference/consumer_usage.cr
  examples/java_reference/filtering_usage.cr
  examples/java_reference/super_stream_usage.cr
)

for example in "${examples[@]}"; do
  echo "Running ${example}"
  crystal run "${example}"
done
