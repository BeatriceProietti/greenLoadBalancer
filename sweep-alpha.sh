#!/bin/bash
# Sweep di GreenLC su due dimensioni: alpha (aggressivita' della penalita')
# e concorrenza (carico offerto) — le due si influenzano a vicenda: lo
# stesso alpha si comporta "piu' verde" a basso carico e "piu' da Least
# Connections" ad alto carico, perche' il numero di richieste attive che
# innesca la penalita' esponenziale dipende dal carico reale, non solo da
# alpha. Vedi PDF "Sweep alpha GreenLC" per la derivazione del crossover
# reqs* = ln(R)/ln(1+alpha) che giustifica la scelta dei valori sotto.
set -e
cd "$(dirname "$0")"

# Valori scelti attorno al crossover analitico (R~35 tra il worker piu'
# verde e il piu' sporco nel vostro mock di CO2): 0.05 quasi non protegge
# mai (reqs*~73), 2.0 protegge quasi subito (reqs*~3).
ALPHAS="${ALPHAS:-0.05 0.15 0.3 0.5 1.0 2.0}"
# Bassa/media/alta concorrenza rispetto ai 6 worker disponibili — valori
# tenuti bassi (max 8, non piu' di ~1-1.5 richieste per worker alla volta)
# per lo stesso motivo di run-experiments.sh: con concorrenza 20-50 su una
# CPU EC2 limitata si rientra nello stesso sovraccarico che ha causato
# l'eviction a raffica (vedi PDF "ec2_eviction_storm" e "settaggio_ec2_20run").
CONCURRENCIES="${CONCURRENCIES:-4 6 8}"
RUNS="${RUNS:-3}"
# Stessa riduzione di difficolta' applicata in run-experiments.sh: senza
# queste due righe, ogni punto dello sweep userebbe i default "pesanti" di
# docker-compose.yaml (fino a 4.000.000), lo stesso volume che ha richiesto
# 40-50+ minuti per una SOLA run in run-experiments.sh.
export LOADGEN_DIFFICULTY_MIN="${LOADGEN_DIFFICULTY_MIN:-20000}"
export LOADGEN_DIFFICULTY_MAX="${LOADGEN_DIFFICULTY_MAX:-300000}"

OUTDIR="results/alpha_sweep"
mkdir -p "$OUTDIR"

N_ALPHAS=$(echo $ALPHAS | wc -w)
N_CONC=$(echo $CONCURRENCIES | wc -w)
TOTAL_RUNS=$((N_ALPHAS * N_CONC * RUNS))
echo "Sweep completo: $N_ALPHAS alpha x $N_CONC concorrenze x $RUNS run = $TOTAL_RUNS run totali."
echo "A ~2 minuti/run, stima grezza: ~$((TOTAL_RUNS * 2)) minuti totali. Riducete ALPHAS/CONCURRENCIES/RUNS se troppo lungo per la sessione."

for alpha in $ALPHAS; do
  for c in $CONCURRENCIES; do
    for run in $(seq 1 "$RUNS"); do
      echo "=============================================="
      echo " alpha=$alpha  concorrenza=$c  run=$run/$RUNS"
      echo "=============================================="

      docker compose -f docker-compose.yaml -f experiments/exp_green_lc.yml down -v 2>/dev/null || true

      export LB_ALPHA="$alpha"
      export LOADGEN_CONCURRENCY="$c"
      # -n fisso a prescindere dalla concorrenza: si vuole lo stesso
      # volume totale di lavoro offerto, distribuito su piu' o meno
      # client paralleli, non un volume diverso per ogni punto dello sweep.
      # 750 (invece di 1500) per restare in una finestra di ~2 minuti a
      # punto: con 6 alpha x 3 concorrenze x RUNS run, il totale è già
      # molte decine di run — vedi il tempo stimato stampato sotto.
      export LOADGEN_REQUESTS="${LOADGEN_REQUESTS:-750}"
      export LOADGEN_OUTPUT="/app/output/alpha_sweep/a${alpha}_c${c}_run${run}.csv"

      docker compose -f docker-compose.yaml -f experiments/exp_green_lc.yml \
        up --build --abort-on-container-exit
    done
  done
done

docker compose down -v 2>/dev/null || true
echo "Sweep completato. Risultati in ./$OUTDIR/"
echo "Analizza con: python analyze_alpha_sweep.py"
