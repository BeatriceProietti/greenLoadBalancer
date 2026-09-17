#!/bin/bash
set -e

# Posizionati nella directory dello script (root del progetto)
cd "$(dirname "$0")"

echo "🧹 1. Pulizia porte e processi residui..."
fuser -k 8080/tcp 8081/tcp 8082/tcp 8090/tcp 8100/tcp 8110/tcp 8120/tcp \
         9081/tcp 9082/tcp 9091/tcp 9092/tcp 9093/tcp 9094/tcp 9095/tcp > /dev/null 2>&1 || true
sleep 1

echo "🔨 2. Compilazione binari (prevenzione lag al boot)..."
go build -o /tmp/worker_node ./cmd/worker/*.go
go build -o /tmp/lb_node ./cmd/lb/*.go

echo "🚀 3. Avvio Worker 1 e 2..."
HTTP_PORT=8081 GRPC_PORT=9081 WORKER_ID=worker-1 REGION=eu-central /tmp/worker_node &
HTTP_PORT=8082 GRPC_PORT=9082 WORKER_ID=worker-2 REGION=eu-north /tmp/worker_node &
sleep 2

echo "🌐 4. Avvio Cluster Load Balancer a 5 nodi..."
HTTP_PORT=8080 LB_ID=lb-node-1 /tmp/lb_node &
HTTP_PORT=8090 LB_ID=lb-node-2 /tmp/lb_node &
HTTP_PORT=8100 LB_ID=lb-node-3 /tmp/lb_node &
HTTP_PORT=8110 LB_ID=lb-node-4 /tmp/lb_node &
HTTP_PORT=8120 LB_ID=lb-node-5 /tmp/lb_node &
LB5_PID=$!

echo ""
echo "================================================================"
echo "✅ BOOT COMPLETATO:"
echo "   Attendi qualche istante: lb-node-5 (Priority=5) deve vincere"
echo "   e diventare il LEADER unico, propagando lo stato agli altri 4."
echo "================================================================"
echo ""

read -p "Premi [INVIO] per UCCIDERE il Leader (lb-node-5)..."

echo ""
echo "💥 Uccisione brutale di lb-node-5..."
kill -9 $LB5_PID > /dev/null 2>&1 || true
fuser -k 8120/tcp 9095/tcp > /dev/null 2>&1 || true

echo "⏳ Leader abbattuto. Attendi circa 6-7 secondi per il timeout del watchdog..."
echo "   lb-node-4 (Priority=4) deve rilevare il silenzio e autoproclamarsi LEADER!"
echo ""

read -p "Premi [INVIO] quando sei pronto a RESUSCITARE lb-node-5..."

echo ""
echo "🔄 Resurrezione di lb-node-5 in corso..."
HTTP_PORT=8120 LB_ID=lb-node-5 /tmp/lb_node &

echo "================================================================"
echo "👑 OSSERVA I LOG:"
echo "   1. lb-node-5 si riavvia e indice una nuova elezione."
echo "   2. lb-node-4 e gli altri riconoscono la priorità superiore."
echo "   3. lb-node-4 cede il comando (StopLeaderDuties)."
echo "   4. lb-node-5 torna LEADER."
echo "================================================================"
echo ""

read -p "Premi [INVIO] per terminare il cluster e pulire le porte."

echo "🛑 Spegnimento cluster in corso..."
fuser -k 8080/tcp 8081/tcp 8082/tcp 8090/tcp 8100/tcp 8110/tcp 8120/tcp \
         9081/tcp 9082/tcp 9091/tcp 9092/tcp 9093/tcp 9094/tcp 9095/tcp > /dev/null 2>&1 || true
rm -f /tmp/worker_node /tmp/lb_node
echo " Cluster arrestato e risorse liberate."