#!/bin/bash
cd "$(dirname "$0")"

echo "🧹 Pulizia preliminare delle porte..."
fuser -k 8080/tcp 8081/tcp 8082/tcp 8090/tcp 8100/tcp 8110/tcp 8120/tcp 9081/tcp 9082/tcp > /dev/null 2>&1
sleep 1

# ==========================================
# 1. AVVIO DEI WORKER
# ==========================================
echo "🚀 Avvio Worker 1 (HTTP: 8081, gRPC: 9081)..."
HTTP_PORT=8081 GRPC_PORT=9081 WORKER_ID=worker-1 REGION=eu-central-1 go run ./cmd/worker/main.go &
W1_PID=$!

echo "🚀 Avvio Worker 2 (HTTP: 8082, gRPC: 9082)..."
HTTP_PORT=8082 GRPC_PORT=9082 WORKER_ID=worker-2 REGION=eu-north-1 go run ./cmd/worker/main.go &
W2_PID=$!

sleep 2

# ==========================================
# 2. AVVIO CLUSTER LOAD BALANCER (5 NODI)
# ==========================================
echo "🌐 Avvio Cluster Load Balancer (5 Nodi)..."

HTTP_PORT=8080 LB_ID=lb-node-1 go run ./cmd/lb/main.go ./cmd/lb/electricity-telemetry.go ./cmd/lb/policy.go &
LB1_PID=$!

HTTP_PORT=8090 LB_ID=lb-node-2 go run ./cmd/lb/main.go ./cmd/lb/electricity-telemetry.go ./cmd/lb/policy.go &
LB2_PID=$!

HTTP_PORT=8100 LB_ID=lb-node-3 go run ./cmd/lb/main.go ./cmd/lb/electricity-telemetry.go ./cmd/lb/policy.go &
LB3_PID=$!

HTTP_PORT=8110 LB_ID=lb-node-4 go run ./cmd/lb/main.go ./cmd/lb/electricity-telemetry.go ./cmd/lb/policy.go &
LB4_PID=$!

HTTP_PORT=8120 LB_ID=lb-node-5 go run ./cmd/lb/main.go ./cmd/lb/electricity-telemetry.go ./cmd/lb/policy.go &
LB5_PID=$!

echo "======================================================"
echo "✅ CLUSTER AVVIATO CON SUCCESSO!"
echo "======================================================"
echo "Apri un ALTRO TERMINALE e usa questo comando per generare traffico continuo:"
echo "while true; do curl -s \"http://localhost:8080/task?difficulty=500000\"; echo \"\"; sleep 0.5; done"
echo "======================================================"

# ==========================================
# 3. SIMULAZIONE CRASH E RECOVERY
# ==========================================
echo "Premi INVIO quando sei pronto a SIMULARE IL CRASH del Worker 1 per 10 secondi..."
read -r

echo "💥 Uccisione del Worker 1 in corso..."
kill -9 $W1_PID > /dev/null 2>&1
fuser -k 8081/tcp 9081/tcp > /dev/null 2>&1

echo "⏳ Attendi 20 secondi. Guarda l'altro terminale: il traffico si sposterà tutto sul Worker 2!"
sleep 20

echo "🔄 Riavvio del Worker 1..."
HTTP_PORT=8081 GRPC_PORT=9081 WORKER_ID=worker-1 REGION=eu-central-1 go run ./cmd/worker/main.go &
W1_PID=$!

echo "✅ Worker 1 tornato online. Il traffico dovrebbe riequilibrarsi a breve."
echo "Premi INVIO per SPEGNERE l'intero cluster."
read -r

# ==========================================
# 4. SHUTDOWN
# ==========================================
echo "Spegnimento in corso..."
fuser -k 8080/tcp 8081/tcp 8082/tcp 8090/tcp 8100/tcp 8110/tcp 8120/tcp 9081/tcp 9082/tcp > /dev/null 2>&1
echo "Tutti i processi arrestati."
