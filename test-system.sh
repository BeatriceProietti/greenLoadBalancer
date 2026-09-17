#!/bin/bash
set -e

cd "$(dirname "$0")"

LOG_DIR="/tmp/green_lb_test_logs"
CSV_OUT="benchmark_results.csv"

# Attivazione del mock per le API di Electricity Maps
export MOCK_ELECTRICITY_API="true"

echo "🧹 1. Pulizia processi attivi e residui..."
fuser -k 8080/tcp 8081/tcp 8082/tcp 8090/tcp 8100/tcp 8110/tcp 8120/tcp \
         9081/tcp 9082/tcp 9091/tcp 9092/tcp 9093/tcp 9094/tcp 9095/tcp > /dev/null 2>&1 || true
rm -rf "$LOG_DIR" "$CSV_OUT"
mkdir -p "$LOG_DIR"
sleep 1

echo "🔨 2. Compilazione dei componenti (Worker, LB, Load Generator)..."
go build -o /tmp/worker_bin ./cmd/worker/*.go
go build -o /tmp/lb_bin ./cmd/lb/*.go
go build -o /tmp/loadgen_bin ./cmd/loadgen/*.go

echo "🚀 3. Avvio di 2 Worker (IT-NO e FR)..."
HTTP_PORT=8081 GRPC_PORT=9081 WORKER_ID=worker-1 REGION=IT-NO /tmp/worker_bin > "$LOG_DIR/worker-1.log" 2>&1 &
HTTP_PORT=8082 GRPC_PORT=9082 WORKER_ID=worker-2 REGION=FR    /tmp/worker_bin > "$LOG_DIR/worker-2.log" 2>&1 &
sleep 1

echo "🌐 4. Avvio del Cluster LB (5 Nodi: lb-node-1 a lb-node-5)..."
HTTP_PORT=8080 LB_ID=lb-node-1 /tmp/lb_bin > "$LOG_DIR/lb-1.log" 2>&1 &
HTTP_PORT=8090 LB_ID=lb-node-2 /tmp/lb_bin > "$LOG_DIR/lb-2.log" 2>&1 &
HTTP_PORT=8100 LB_ID=lb-node-3 /tmp/lb_bin > "$LOG_DIR/lb-3.log" 2>&1 &
HTTP_PORT=8110 LB_ID=lb-node-4 /tmp/lb_bin > "$LOG_DIR/lb-4.log" 2>&1 &
HTTP_PORT=8120 LB_ID=lb-node-5 /tmp/lb_bin > "$LOG_DIR/lb-5.log" 2>&1 &

echo "⏳ 5. In attesa di convergenza Bully e primo SyncState (4s)..."
sleep 4

echo ""
echo "🔍 6. Test Iniezione Header HTTP (Richiesta singola verso lb-node-1)..."
echo "----------------------------------------------------------------------"
curl -s -i "http://localhost:8080/task" | grep -iE "(HTTP/|X-LB-|X-Worker-)"
echo "----------------------------------------------------------------------"
echo ""

echo "⚡ 7. Esecuzione Benchmark con il Load Generator..."
/tmp/loadgen_bin \
  -urls="http://localhost:8080/task,http://localhost:8090/task,http://localhost:8100/task,http://localhost:8110/task,http://localhost:8120/task" \
  -n=1000 \
  -c=100 \
  -o="$CSV_OUT"

echo ""
echo "📊 8. Prime 6 righe del dataset generato ($CSV_OUT):"
echo "----------------------------------------------------------------------"
head -n 6 "$CSV_OUT"
echo "----------------------------------------------------------------------"
echo ""

read -p "Premi [INVIO] per arrestare il cluster e terminare il test."

echo "🛑 Spegnimento del cluster..."
fuser -k 8080/tcp 8081/tcp 8082/tcp 8090/tcp 8100/tcp 8110/tcp 8120/tcp \
         9081/tcp 9082/tcp 9091/tcp 9092/tcp 9093/tcp 9094/tcp 9095/tcp > /dev/null 2>&1 || true
rm -f /tmp/worker_bin /tmp/lb_bin /tmp/loadgen_bin
echo "Test completato con successo."