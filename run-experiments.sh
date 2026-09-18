#!/bin/bash
# Esegue tutte e 4 le policy, N volte ciascuna, con stack completo
# down+up a ogni run (nessuno stato residuo tra un run e l'altro) e un
# nome di file CSV distinto per ogni (policy, run) cosi' i risultati non
# si sovrascrivono. Vedi PDF "Metodologia esperimenti" per la motivazione
# di N (default 5) e dei parametri del load generator.
set -e
cd "$(dirname "$0")"

RUNS="${RUNS:-5}"
POLICIES=(round_robin least_connection green_topk green_lc)

mkdir -p results

for policy in "${POLICIES[@]}"; do
  for run in $(seq 1 "$RUNS"); do
    echo "=============================================="
    echo " Policy: $policy — run $run/$RUNS"
    echo "=============================================="

    # Stack completo spento e riacceso: niente stato residuo (task attivi,
    # leader eletto, ecc.) tra un run e l'altro — stesse condizioni di
    # partenza per ogni misura, come richiesto.
    docker compose -f docker-compose.yaml -f "experiments/exp_${policy}.yml" down -v 2>/dev/null || true

    export LOADGEN_OUTPUT="/app/output/results_${policy}_run${run}.csv"
    docker compose -f docker-compose.yaml -f "experiments/exp_${policy}.yml" \
      up --build --abort-on-container-exit
  done
done

docker compose down -v 2>/dev/null || true
echo "Tutti i run completati. Risultati in ./results/"
