package main

import (
	"fmt"
	"greenLoadBalancer/configs"
	pb "greenLoadBalancer/internal/pb"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// WorkerNode represents a single worker
type WorkerNode struct {
	ID             string
	URL            *url.URL
	Proxy          *httputil.ReverseProxy
	ActiveRequests int

	// gRPC, zone and telemetry
	GRPCAddress      string
	GRPCClient       pb.WorkerServiceClient
	IsActive         bool
	PowerWatts       float64
	CarbonInt        float64
	Zone             string
	missedHeartbeats int

	mu sync.RWMutex
}

type LoadBalancer struct {
	ID              string
	Priority        int32  // priority used in the bully algorithm
	SelfGRPCAddress string // indirizzo su cui QUESTO nodo espone ClusterService

	Workers []*WorkerNode
	Peers   []*PeerNode // other LB nodes in the cluster

	Policy   BalancingPolicy
	Config   *configs.Config
	shutdown chan struct{} // clean shutdown

	election ElectionState // state of the election algorithm
}

func main() {
	cfg, err := configs.LoadConfig("configs/config.yaml")
	if err != nil {
		log.Fatalf("Errore caricamento config: %v", err)
	}

	// Override da ENV se specificati (permette gli esperimenti via Docker Compose)
	if envStrat := os.Getenv("LB_STRATEGY"); envStrat != "" {
		cfg.LB.Strategy = envStrat
	}
	if envAlpha := os.Getenv("LB_ALPHA"); envAlpha != "" {
		if val, err := strconv.ParseFloat(envAlpha, 64); err == nil {
			cfg.LB.Alpha = val
		}
	}

	if envTopK := os.Getenv("LB_TOP_K_PERCENT"); envTopK != "" {
		if val, err := strconv.ParseFloat(envTopK, 64); err == nil {
			cfg.LB.TopKPercent = val
		}
	}
	if envMaxReq := os.Getenv("LB_MAX_ACTIVE_REQUESTS"); envMaxReq != "" {
		if val, err := strconv.Atoi(envMaxReq); err == nil {
			cfg.LB.MaxActiveRequests = val
		}
	}

	lbID := os.Getenv("LB_ID")
	if lbID == "" {
		lbID = "lb-node-1"
	}

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = cfg.LB.Port
	}

	lb := &LoadBalancer{
		ID:       lbID,
		Config:   cfg,
		Policy:   PolicyFactory(cfg.LB.Strategy, cfg.LB.Alpha, cfg.LB.TopKPercent, cfg.LB.MaxActiveRequests),
		shutdown: make(chan struct{}),
	}

	// Trova il proprio indirizzo gRPC nella lista condivisa lb.nodes (stesso
	// config.docker.yaml montato su ogni container, ci si riconosce tramite LB_ID).
	for _, n := range cfg.LB.Nodes {
		if n.ID == lbID {
			lb.SelfGRPCAddress = n.GRPCAddress
			lb.Priority = n.Priority
			break
		}
	}
	if lb.SelfGRPCAddress == "" {
		log.Fatalf("Nodo %s non trovato in lb.nodes: controlla config.docker.yaml", lbID)
	}

	// Worker initialization
	for _, wCfg := range cfg.Workers {
		parsedURL, err := url.Parse(wCfg.URL)
		if err != nil {
			log.Fatalf("URL non valido: %v", err)
		}

		proxy := httputil.NewSingleHostReverseProxy(parsedURL)

		nodeID := wCfg.ID
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte(fmt.Sprintf("⚠️ Il nodo %s è attualmente irraggiungibile.\n", nodeID)))
		}

		lb.Workers = append(lb.Workers, &WorkerNode{
			ID:          wCfg.ID,
			URL:         parsedURL,
			Proxy:       proxy,
			GRPCAddress: wCfg.GRPCAddress,
			Zone:        wCfg.Zone,
		})
	}

	// Client gRPC verso worker e verso peer LB (nessuna connessione fisica
	// aperta subito: grpc.NewClient e' lazy, si connette al primo RPC reale).
	lb.ConnectToWorkers()
	lb.ConnectToPeers()

	// Server gRPC per ClusterService: gli altri nodi devono poter chiamare
	// StartElection / AnnounceCoordinator / SyncState su di noi.
	// NOTA: si ascolta sulla porta del PROPRIO grpc_address (letto sopra da
	// lb.nodes), non su un valore condiviso: ogni nodo ha la sua.
	_, selfPort, err := net.SplitHostPort(lb.SelfGRPCAddress)
	if err != nil {
		log.Fatalf("grpc_address non valido per %s (%q): %v", lbID, lb.SelfGRPCAddress, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterClusterServiceServer(grpcServer, lb.NewClusterServer())
	lis, err := net.Listen("tcp", ":"+selfPort)
	if err != nil {
		log.Fatalf("Errore apertura listener gRPC: %v", err)
	}
	go func() {
		log.Printf("[%s] ClusterService in ascolto su :%s", lb.ID, selfPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Errore gRPC server: %v", err)
		}
	}()

	// Al boot nessun nodo conosce ancora il leader: si parte tutti da
	// un'elezione, invece di aspettare che scada il primo watchdog. Ma se
	// durante questa breve attesa e' GIA' arrivato un AnnounceCoordinator
	// legittimo (un peer con priorita' piu' alta puo' essersi eletto ed
	// essersi annunciato prima che i 200ms scadano), non ha senso aprire
	// comunque un'elezione ridondante: porterebbe a un ciclo di rielezioni
	// a vuoto (bug osservato: "Nessun annuncio di coordinatore ricevuto in
	// tempo: rieleggo." ripetuto all'infinito nonostante il leader vero
	// fosse vivo e raggiungibile).
	time.Sleep(200 * time.Millisecond)
	lb.election.mu.RLock()
	alreadyKnowsLeader := lb.election.leaderID != 0
	lb.election.mu.RUnlock()
	if !alreadyKnowsLeader {
		lb.StartElection()
	}

	http.HandleFunc("/", lb.handleRequest)
	log.Printf("LB %s avviato su porta %s con strategia: %s", lb.ID, httpPort, cfg.LB.Strategy)
	if err := http.ListenAndServe(":"+httpPort, nil); err != nil {
		log.Fatalf("Errore avvio LB %s: %v", lb.ID, err)
	}
}

func (lb *LoadBalancer) handleRequest(w http.ResponseWriter, r *http.Request) {
	// Pluggable policy
	worker := lb.Policy.NextWorker(lb.Workers)

	if worker == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("❌ Errore 503: Nessun Worker attivo nel cluster.\n"))
		return
	}

	// Header injection
	lb.election.mu.RLock()
	currentRole := lb.election.state.String()
	lb.election.mu.RUnlock()

	worker.mu.RLock()
	workerCO2 := worker.CarbonInt
	worker.mu.RUnlock()

	w.Header().Set("X-LB-Node", lb.ID)
	w.Header().Set("X-LB-Role", currentRole)
	w.Header().Set("X-LB-Strategy", lb.Config.LB.Strategy)
	w.Header().Set("X-Worker-Carbon-Intensity", fmt.Sprintf("%.2f", workerCO2))

	worker.mu.Lock()
	worker.ActiveRequests++
	worker.mu.Unlock()

	defer func() {
		worker.mu.Lock()
		worker.ActiveRequests--
		worker.mu.Unlock()
	}()

	worker.Proxy.ServeHTTP(w, r)
}
