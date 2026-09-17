#!/bin/bash
cd "$(dirname "$0")"

echo "🧹 Pulizia porte..."
fuser -k 8080/tcp 8081/tcp 8082/tcp 8090/tcp 8100/tcp 9081/tcp 9082/tcp 9091/tcp 9092/tcp 9093/tcp > /dev/null 2>&1
sleep 1

# 1. AVVIO WORKER
echo "🚀 Avvio Worker 1 e 2..."
HTTP_PORT=8081 GRPC_PORT=9081 WORKER_ID=worker-1 REGION=eu-central go run ./cmd/worker/*.go &
HTTP_PORT=8082 GRPC_PORT=9082 WORKER_ID=worker-2 REGION=eu-north go run ./cmd/worker/*.go &
sleep 2

# 2. AVVIO CLUSTER LB
echo "🌐 Avvio Cluster Load Balancer (3 Nodi)..."

# Nodo 1 (Priorità 1)
HTTP_PORT=8080 LB_ID=lb-node-1 go run ./cmd/lb/*.go &

# Nodo 2 (Priorità 2)
HTTP_PORT=8090 LB_ID=lb-node-2 go run ./cmd/lb/*.go &

# Nodo 3 (Priorità 3 - Destinato a vincere)
HTTP_PORT=8100 LB_ID=lb-node-3 go run ./cmd/lb/*.go &
LB3_PID=$!

echo "======================================================"
echo "✅ Osserva i log: lb-node-3 dovrebbe dichiararsi LEADER."
echo "======================================================"

read -p "Premi INVIO per UCCIDERE il Leader (lb-node-3) e scatenare il Bully..."

echo "💥 Assassinio di lb-node-3 in corso..."
# Uccide sia il processo associato al PID che i processi in ascolto sulle sue porte
kill -9 $LB3_PID > /dev/null 2>&1
fuser -k 8100/tcp 9093/tcp > /dev/null 2>&1

echo "⏳ Attendi 6 secondi (3 missed heartbeats). lb-node-2 dovrebbe indire un'elezione e vincere!"
read -p "Premi INVIO per spegnere tutto."

fuser -k 8080/tcp 8081/tcp 8082/tcp 8090/tcp 8100/tcp > /dev/null 2>&1
echo "Tutti i processi arrestati."
