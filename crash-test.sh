#!/bin/bash
cd "$(dirname "$0")"

# Pulizia preliminare
fuser -k 8080/tcp 8081/tcp 8082/tcp 9081/tcp 9082/tcp > /dev/null 2>&1
sleep 1

echo "Avvio Worker 1 (HTTP: 8081, gRPC: 9081)..."
HTTP_PORT=8081 GRPC_PORT=9081 WORKER_ID=worker-1 REGION=eu-central-1 go run ./cmd/worker/main.go &

echo "Avvio Worker 2 (HTTP: 8082, gRPC: 9082)..."
HTTP_PORT=8082 GRPC_PORT=9082 WORKER_ID=worker-2 REGION=eu-north-1 go run ./cmd/worker/main.go &

sleep 2

echo "Avvio Load Balancer (HTTP: 8080)..."
go run ./cmd/lb &

echo "Ambiente avviato. Premi INVIO per testare il crash del Worker 1, oppure CTRL+C per terminare."
read -r

echo "💥 Uccisione del Worker 1..."
fuser -k 8081/tcp 9081/tcp > /dev/null 2>&1

echo "Attendi circa 16 secondi (3 heartbeat falliti) per osservare l'eviction nei log..."
sleep 16

echo "🔄 Riavvio del Worker 1..."
HTTP_PORT=8081 GRPC_PORT=9081 WORKER_ID=worker-1 REGION=eu-central-1 go run ./cmd/worker/main.go &

echo "Premi INVIO per arrestare l'intero ambiente."
read -r

fuser -k 8080/tcp 8081/tcp 8082/tcp 9081/tcp 9082/tcp > /dev/null 2>&1
echo "Tutti i processi arrestati."
