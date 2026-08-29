#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  docker compose down --volumes --remove-orphans
}
trap cleanup EXIT

docker compose up --detach --wait
CRABBIT_INTEGRATION=1 crystal spec spec/integration
scripts/reference_examples.sh
