#!/bin/bash
set -e

cd "$(dirname "$0")"

# Flag per il mock: default a "true" per proteggere la quota API.
# Per usare l'API reale, avvia lo script con: USE_MOCK=false ./test_script.sh
export MOCK_ELECTRICITY_API="${USE_MOCK:-true}"

# Token per API reali (utilizzato solo se MOCK_ELECTRICITY_API=false)
export ELECTRICITY_MAPS_TOKEN="${ELECTRICITY_MAPS_TOKEN:-INSERISCI_QUI_IL_TUO_TOKEN}"

LOG_DIR="/tmp/lb_green_logs"
mkdir -p "$LOG_DIR"

echo "🧹 1. Pulizia processi e log precedenti..."
fuser -k 8080/tcp 8081/tcp 8082/tcp 8090/tcp 8100/tcp 9081/tcp 9082/tcp 9091/tcp 9092/tcp 9093/tcp > /dev/null 2>&1 || true
rm -f "$LOG_DIR"/*.log
sleep 1

echo "🔨 2. Compilazione..."
go build -o /tmp/worker_node ./cmd/worker/*.go
go build -o /tmp/lb_node ./cmd/lb/*.go

echo "⚙️ Configurazione API: MOCK_ELECTRICITY_API=$MOCK_ELECTRICITY_API"

echo "🚀 3. Avvio 2 Worker..."
HTTP_PORT=8081 GRPC_PORT=9081 WORKER_ID=worker-1 REGION=IT-NO /tmp/worker_node > "$LOG_DIR/worker-1.log" 2>&1 &
HTTP_PORT=8082 GRPC_PORT=9082 WORKER_ID=worker-2 REGION=FR    /tmp/worker_node > "$LOG_DIR/worker-2.log" 2>&1 &
sleep 2

echo "🌐 4. Avvio Load Balancer Cluster (3 Nodi)..."
# lb-node-3 ha priorità 3 (sarà il leader del cluster a 3 nodi)
HTTP_PORT=8080 LB_ID=lb-node-1 /tmp/lb_node > "$LOG_DIR/lb-1.log" 2>&1 &
HTTP_PORT=8090 LB_ID=lb-node-2 /tmp/lb_node > "$LOG_DIR/lb-2.log" 2>&1 &
HTTP_PORT=8100 LB_ID=lb-node-3 /tmp/lb_node > "$LOG_DIR/lb-3.log" 2>&1 &

echo "⏳ In attesa di elezione Bully e primo ciclo SyncState (5s)..."
sleep 5

echo ""
echo "📊 --- VERIFICA STATO CLUSTER ---"
echo ">> Leader Duties (Fetch Carbon Intensity):"
grep -h "Carbon Intensity aggiornata" "$LOG_DIR"/lb-*.log || echo "Nessun fetch rilevato ancora."

echo ""
echo ">> Follower Update (Eventual Consistency su lb-node-1 e lb-node-2):"
grep -h "🔄 Worker" "$LOG_DIR"/lb-1.log "$LOG_DIR"/lb-2.log | tail -n 6 || echo "Nessun aggiornamento registrato sui follower."
echo "---------------------------------"
echo ""

echo "I log completi si trovano in: $LOG_DIR"
read -p "Premi [INVIO] per terminare il test e arrestare i processi."

echo "🛑 Spegnimento cluster in corso..."
fuser -k 8080/tcp 8081/tcp 8082/tcp 8090/tcp 8100/tcp 9081/tcp 9082/tcp 9091/tcp 9092/tcp 9093/tcp > /dev/null 2>&1 || true
rm -f /tmp/worker_node /tmp/lb_node
echo "Test concluso."