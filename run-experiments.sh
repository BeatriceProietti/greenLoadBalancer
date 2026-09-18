#!/bin/bash
# Executes all 4 policies, N times each, with a full stack
# (tearing down and bringing the stack back up for each run—no residual state between runs)
# and a distinct CSV filename for each (policy, run) so that results
# do not overwrite each other.
set -e
cd "$(dirname "$0")"

RUNS="${RUNS:-5}"
POLICIES=(round_robin least_connection green_topk green_lc)

mkdir -p results

# Only one built for all the runs
docker compose -f docker-compose.yaml build

for policy in "${POLICIES[@]}"; do
  for run in $(seq 1 "$RUNS"); do
    echo "=============================================="
    echo " Policy: $policy — run $run/$RUNS"
    echo "=============================================="

    docker compose -f docker-compose.yaml -f "experiments/exp_${policy}.yml" down -v 2>/dev/null || true

    export LOADGEN_OUTPUT="/app/output/results_${policy}_run${run}.csv"

    docker compose -f docker-compose.yaml -f "experiments/exp_${policy}.yml" \
      up --abort-on-container-exit
  done
done

docker compose down -v 2>/dev/null || true
echo "Tutti i run completati. Risultati in ./results/"