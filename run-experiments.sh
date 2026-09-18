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

# Valori pensati per un'istanza EC2 con CPU limitata (AWS Academy Learner
# Lab): con i default "manuali" del docker-compose.yaml (3000 richieste,
# concorrenza 30, difficolta' fino a 4M) ogni singolo run causava eviction
# a raffica su tutti i worker (CPU satura dai cicli SHA-256, l'heartbeat
# gRPC non riusciva piu' a rispondere in tempo) e superava i 10-15 minuti
# — inaccettabile per 20 run consecutivi. Qui la concorrenza e' abbassata
# a ~1 richiesta per worker alla volta (nessun worker riceve piu' cicli
# SHA-256 concorrenti di quanti possa gestire senza affamare il proprio
# heartbeat) e la difficolta' massima e' tagliata di un ordine di
# grandezza. Restano personalizzabili da env var per chi lavora in
# locale su una macchina piu' potente. Vedi PDF "ec2_eviction_storm" e
# "ec2_loadgen_lento" per i log che hanno motivato questi valori, e
# calibrare con un run singolo prima di lanciare tutti i 20.
# REQUESTS ricalibrato sul dato reale: la run "standard" originale (3000
# richieste, concorrenza 30, difficolta' 50k-4M) ha impiegato SICURAMENTE
# oltre 40-50 minuti su questa istanza EC2 prima di fermarsi. Da quel dato
# (vedi PDF "settaggio_ec2_20run_v2" per il calcolo) si stima un budget di
# lavoro compatibile con 3-4 minuti a concorrenza 6 / difficolta' 20k-300k
# — con un margine di sicurezza applicato apposta, perche' la stima parte
# da un singolo run degradato dall'eviction, non da una misura pulita.
export LOADGEN_REQUESTS="${LOADGEN_REQUESTS:-1000}"
export LOADGEN_CONCURRENCY="${LOADGEN_CONCURRENCY:-18}"
export LOADGEN_DIFFICULTY_MIN="${LOADGEN_DIFFICULTY_MIN:-20000}"
export LOADGEN_DIFFICULTY_MAX="${LOADGEN_DIFFICULTY_MAX:-500000}"

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