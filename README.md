Green Load Balancer — Load Balancing Carbon-Aware con Elezione del Leader
==========================================================================
Questo progetto è un cluster di **Load Balancer distribuiti e carbon-aware** per il corso di **Sistemi Distribuiti e Cloud Computing**. I nodi LB si organizzano tramite l'algoritmo di elezione **Bully** (via gRPC), instradano il traffico verso 6 worker simulati in altrettante regioni con politiche sensibili al consumo energetico e all'intensità di carbonio, e tollerano guasti di nodi LB e worker tramite heartbeat periodici. Il tutto è containerizzato con **Docker** e orchestrato con **Docker Compose**.

L'implementazione è realizzata in **Go (Golang)** ed è orchestrata tramite **Docker** e **Docker Compose** per una facile gestione e deployment.

Caratteristiche Principali
---------------------------
- **Elezione del leader Bully**: 5 nodi LB con priorità distinte, elezione via gRPC (`StartElection`/`AnnounceCoordinator`/`SyncState`), rielezione automatica alla morte del leader, riconoscimento di un leader a priorità superiore al suo rientro.
- **Failure detection con heartbeat**: ogni nodo LB monitora i 6 worker con heartbeat periodici; un worker che manca N heartbeat consecutivi viene marcato inattivo (eviction) ed escluso dal routing, per poi rientrare al primo heartbeat andato a buon fine.
- **4 politiche di bilanciamento**: Round Robin, Least Connections, Green Top-K e Green Least Connections (con iperparametro α che pesa consumo energetico/CO2 contro numero di richieste attive).
- **Telemetria energetica con fallback intelligente**: lettura di Scaphandre/RAPL quando disponibile, con **mock reattivo al carico** (`idleWatts + dynamicWatts × utilizzo`) come fallback automatico e silenzioso — verificato necessario sotto qualunque virtualizzazione (Docker Desktop, EC2 standard).
- **Intensità di carbonio per regione**: 6 worker in altrettante zone (SE/FR/GB/IT-NO/DE/PL), con integrazione Electricity Maps e fallback a valori mock plausibili.
- **Load generator parametrico**: richieste a difficoltà variabile (carico SHA-256) con seed deterministico, per confronti riproducibili tra policy con lo stesso identico carico.
- **Esperimenti e analisi riproducibili**: script per N run ripetute per policy, sweep sull'iperparametro α, e script Python di analisi/visualizzazione (quota per worker, CDF di latenza, frontiera di Pareto ambiente/latenza).

Prerequisiti
------------
Per eseguire il progetto servono:
- **Git** per clonare il repository.
- **Docker** (consigliato ≥ 24) e **Docker Compose v2** (`docker compose ...`).
- **Go** (≥ 1.21), solo se si vuole compilare/eseguire i componenti fuori da Docker.
- **Python 3** con `matplotlib` (opzionale), solo per gli script di analisi dei risultati.

Come Avviare il Progetto (con Docker Compose)
----------------------------------------------

1. Clona il repository ed entra nella cartella:

   ```bash
   git clone <URL-DEL-REPOSITORY>
   cd greenLoadBalancer
   ```

2. Copia il file d'ambiente (per il token Electricity Maps, opzionale — senza token si usa il fallback mock):

   ```bash
   cp .env.example .env
   ```

3. Build & avvio di tutto lo stack (5 nodi LB, 6 worker):

   ```bash
   docker compose up --build -d
   ```

4. Verifica che tutti i container siano su:

   ```bash
   docker compose ps
   ```

5. Stop e cleanup:

   ```bash
   docker compose down
   ```

------------
Osservare il comportamento del cluster
---------------------------------------

Le sezioni seguenti mostrano, passo per passo, come osservare dal vivo le caratteristiche distintive del progetto. Le priorità Bully sono `lb-node-1=1 ... lb-node-5=5`: il leader iniziale è **lb-node-5** (priorità più alta).

1. Log di un nodo (comportamento generale):
   ```bash
   docker compose logs -f lb-node-1
   ```

2. Chi è il leader in questo momento:
   ```bash
   docker compose logs | grep "LEADER"
   ```

3. Intensità di carbonio vista dal leader (aggiornata periodicamente per zona):
   ```bash
   docker compose logs | grep "Carbon"
   ```

4. Watt reattivi al carico (mentre gira il load generator, punto 12): tenete sotto controllo un follower qualsiasi:
   ```bash
   docker compose logs -f lb-node-1 | grep "Power"
   ```

5. Uccisione del leader — i follower se ne accorgono e c'è una nuova elezione (vince lb-node-4, la priorità più alta tra i superstiti):
   ```bash
   docker kill lb-node-5
   docker compose logs -f lb-node-4 lb-node-3 lb-node-2 lb-node-1 | grep -E "elezione|LEADER"
   ```

6. Resume del vecchio leader — priorità più alta di tutti, forza una nuova elezione e riprende il comando; lb-node-4 lo riconosce e cede:
   ```bash
   docker start lb-node-5
   docker compose logs -f lb-node-4 | grep -E "Nuovo leader riconosciuto"
   ```

7. Uccisione di un follower — nessuna conseguenza sul cluster, il leader resta lo stesso (contrasto diretto col punto 5):
   ```bash
   docker kill lb-node-2
   docker compose logs -f lb-node-4 | grep -i leader
   ```

8. Resume del follower:
   ```bash
   docker start lb-node-2
   docker compose logs -f lb-node-2 | grep "Nuovo leader riconosciuto"
   ```

9. Rimozione di un worker — dopo N heartbeat mancati consecutivi il leader lo marca inattivo (eviction) ed esclude il worker dal routing:
   ```bash
   docker kill worker-3
   docker compose logs -f lb-node-4 | grep -E "Eviction|ONLINE"
   ```

10. Resume del worker — il leader lo riconosce ONLINE al primo heartbeat andato a buon fine:
    ```bash
    docker start worker-3
    docker compose logs -f lb-node-4 | grep "ONLINE"
    ```

11. Cambiare la politica di bilanciamento attiva (round_robin, least_connections, green_top_k, green_lc):
    ```bash
    docker compose -f docker-compose.yaml -f experiments/exp_green_lc.yml up --build -d
    ```

12. Attivare il load generator (per vedere i Watt reagire al carico, punto 4, e la politica instradare il traffico):
    ```bash
    docker compose up --build loadgen
    ```

---------
Esperimenti e analisi
-----------------------

1. Confronto delle 4 policy, N run ciascuna (default 5), con stack riavviato tra un run e l'altro:
   ```bash
   ./run-experiments.sh
   ```

2. Generare tutti i grafici di analisi dai CSV prodotti sopra:
   ```bash
   pip install matplotlib
   python generate_all_graphs.py
   ```

---------
Parametri configurabili
-------------------------

Policy e circuit breaker (`configs/config.docker.yaml`)

| Variabile / campo         | Valore | Descrizione                                                                 |
| -------------------------- | ------ | ---------------------------------------------------------------------------- |
| `strategy`                 | `round_robin` | Politica attiva: `round_robin`, `least_connections`, `green_top_k`, `green_lc`. |
| `alpha`                    | `0.15` | Peso della penalità per richieste attive in Green Least Connections.        |
| `top_k_percent`            | `0.30` | Frazione dei worker più "verdi" tra cui scegliere in Green Top-K.           |
| `max_active_requests`      | `100`  | Soglia del circuit breaker: oltre questo numero di richieste attive, un worker non è più eleggibile. |
| `max_missed_heartbeats`    | `3`    | Heartbeat mancati consecutivi prima di marcare un worker inattivo (eviction). |
| `heartbeat_interval_ms`    | `2000` | Intervallo tra un ciclo di heartbeat/telemetria e il successivo.            |
| `election_timeout_ms`      | `3000` | Timeout di attesa dell'annuncio del coordinatore prima di rieleggere.       |
| `rpc_timeout_ms`           | `800`  | Timeout delle chiamate gRPC (heartbeat, elezione).                          |
| `carbon_query_interval_s`  | `300`  | Intervallo di aggiornamento dell'intensità di carbonio (Electricity Maps o mock). |

Load generator (`docker-compose.yaml`, servizio `loadgen`)

| Variabile                  | Valore default | Descrizione                                                      |
| --------------------------- | --------------- | ------------------------------------------------------------------ |
| `LOADGEN_REQUESTS`          | `3000`          | Numero totale di richieste per run.                               |
| `LOADGEN_CONCURRENCY`       | `30`            | Numero di client concorrenti.                                     |
| `LOADGEN_DIFFICULTY_MIN/MAX`| `50000` / `4000000` | Range di iterazioni SHA-256 per richiesta (variabilità del carico). |
| `LOADGEN_SEED`              | `42`            | Seed per una sequenza di difficoltà deterministica e riproducibile tra policy diverse. |
| `LOADGEN_OUTPUT`            | `/app/output/benchmark_results.csv` | Percorso del CSV con i risultati, montato su `./results/`. |

Mock di potenza (`docker-compose.yaml`, servizi `worker-*`)

| Variabile                   | Valore default | Descrizione                                                        |
| ---------------------------- | --------------- | --------------------------------------------------------------------- |
| `DEFAULT_POWER_WATTS`        | `2.50`          | Potenza a riposo (idle) usata dal mock quando Scaphandre non è disponibile. |
| `MOCK_DYNAMIC_POWER_WATTS`   | `12.0`          | Potenza dinamica massima aggiuntiva sotto carico saturo.            |

