package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "greenLoadBalancer/internal/pb"
)

// ElectricityMapResponse maps the JSON payload retrieved with APIs
type ElectricityMapResponse struct {
	Zone            string  `json:"zone"`
	CarbonIntensity float64 `json:"carbonIntensity"`
	Datetime        string  `json:"datetime"`
	UpdatedAt       string  `json:"updatedAt"`
}

// ---------------------------------------------------------------------
// Setup client gRPC to the worker (Scaphandre via WorkerService.Heartbeat)
// ---------------------------------------------------------------------

func (lb *LoadBalancer) ConnectToWorkers() {
	for _, w := range lb.Workers {
		conn, err := grpc.NewClient(w.GRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[%s] Impossibile creare client gRPC per worker %s: %v", lb.ID, w.ID, err)
			continue
		}
		w.GRPCClient = pb.NewWorkerServiceClient(conn)
		// IsActive = false as a default value until the veery first heartbeat arrives
	}
}

// ---------------------------------------------------------------------
// Leader duties: polling worker (Scaphandre) + broadcast SyncState ai peer
// ---------------------------------------------------------------------

// StartLeaderDuties viene chiamata solo da becomeLeader(). E' l'unico punto
// del sistema in cui si fa polling energetico reale: i follower non
// interrogano mai direttamente i worker, ricevono lo stato via SyncState.
func (lb *LoadBalancer) StartLeaderDuties() {
	lb.election.mu.Lock()

	// Se nel frattempo siamo stati retrocessi a follower, abortisci subito
	if lb.election.state != StateLeader {
		lb.election.mu.Unlock()
		return
	}

	if lb.election.leaderDutiesStop != nil {
		lb.election.mu.Unlock()
		return // gia' in esecuzione (non dovrebbe succedere, difensivo)
	}
	stop := make(chan struct{})
	lb.election.leaderDutiesStop = stop
	lb.election.mu.Unlock()

	lb.pingAllWorkers(true)    // first synchronous polling
	lb.updateCarbonIntensity() // first carbon intensity fetch
	lb.broadcastState()        // immediate first update to followers

	// heartbeat loop and watchdog refresh
	heartbeatTicker := time.NewTicker(lb.Config.LB.HeartbeatInterval())
	go func() {
		defer heartbeatTicker.Stop()
		for {
			select {
			case <-heartbeatTicker.C:
				lb.pingAllWorkers(false)
				lb.broadcastState()
			case <-stop:
				return
			case <-lb.shutdown:
				return
			}
		}
	}()

	// Electricity Maps loop (5-15 min) in a separate goroutine
	carbonTicker := time.NewTicker(lb.Config.LB.CarbonQueryInterval())
	go func() {
		defer carbonTicker.Stop()
		for {
			select {
			case <-carbonTicker.C:
				lb.updateCarbonIntensity()
			case <-stop:
				return
			case <-lb.shutdown:
				return
			}
		}
	}()
}

// StopLeaderDuties ferma il polling/broadcast. Chiamata quando questo nodo
// scopre di essere stato "sorpassato" da un leader con priorita' maggiore.
func (lb *LoadBalancer) StopLeaderDuties() {
	lb.election.mu.Lock()
	defer lb.election.mu.Unlock()
	if lb.election.leaderDutiesStop != nil {
		close(lb.election.leaderDutiesStop)
		lb.election.leaderDutiesStop = nil
	}
}

// pingWorker esegue l'heartbeat verso un singolo worker e ne aggiorna lo stato,
// con eviction a soglia (N fallimenti consecutivi) invece che a singolo timeout.
func (lb *LoadBalancer) pingWorker(worker *WorkerNode) {
	if worker.GRPCClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), lb.Config.LB.RPCTimeout())
	defer cancel()

	resp, err := worker.GRPCClient.Heartbeat(ctx, &pb.PingRequest{LeaderId: lb.ID})

	worker.mu.Lock()
	defer worker.mu.Unlock()

	if err != nil || !resp.GetIsHealthy() {
		worker.missedHeartbeats++
		if worker.missedHeartbeats >= lb.Config.LB.MaxMissedHeartbeats {
			if worker.IsActive {
				log.Printf("[%s] ⚠️ Worker %s: %d heartbeat mancati consecutivi. Eviction.",
					lb.ID, worker.ID, worker.missedHeartbeats)
				worker.IsActive = false
			}
		}
		return
	}

	if !worker.IsActive {
		log.Printf("[%s] ✅ Worker %s è ONLINE.", lb.ID, worker.ID)
	}
	worker.missedHeartbeats = 0
	worker.IsActive = true
	worker.PowerWatts = float64(resp.GetCurrentPowerWatts())
}

// pingAllWorkers esegue un fan-out concorrente dell'heartbeat verso tutti i worker.
// Se wait e' true, blocca fino al completamento dell'intero giro (usato solo al boot,
// cosi' che il primo routing HTTP non parta con stato ancora vuoto).
func (lb *LoadBalancer) pingAllWorkers(wait bool) {
	var wg sync.WaitGroup
	for _, w := range lb.Workers {
		wg.Add(1)
		go func(worker *WorkerNode) {
			defer wg.Done()
			lb.pingWorker(worker)
		}(w)
	}
	if wait {
		wg.Wait()
	}
}

// buildStateSnapshot legge lo stato corrente di tutti i worker (protetto da
// RLock) e lo trasforma nel formato del proto, pronto per il broadcast.
func (lb *LoadBalancer) buildStateSnapshot() map[string]*pb.WorkerData {
	out := make(map[string]*pb.WorkerData, len(lb.Workers))
	for _, w := range lb.Workers {
		w.mu.RLock()
		out[w.ID] = &pb.WorkerData{
			Address:         w.URL.String(),
			PowerWatts:      float32(w.PowerWatts),
			CarbonIntensity: float32(w.CarbonInt),
			IsActive:        w.IsActive,
		}
		w.mu.RUnlock()
	}
	return out
}

// broadcastState invia lo stato corrente a tutti i peer. Viene chiamata
// incondizionatamente a ogni ciclo (non solo quando qualcosa cambia): questa
// stessa chiamata e' anche l'heartbeat che tiene vivo il watchdog dei follower
// (vedi resetWatchdog in election.go, e PDF Q1).
func (lb *LoadBalancer) broadcastState() {
	snapshot := lb.buildStateSnapshot()
	for _, p := range lb.Peers {
		go func(peer *PeerNode) {
			ctx, cancel := context.WithTimeout(context.Background(), lb.Config.LB.RPCTimeout())
			defer cancel()
			_, err := peer.Client.SyncState(ctx, &pb.StateUpdateRequest{
				LeaderId: lb.Priority,
				Workers:  snapshot,
			})
			if err != nil {
				log.Printf("[%s] ⚠️ Peer %s offline (SyncState non recapitato)", lb.ID, peer.ID)
			}
		}(p)
	}
}

// HandleSyncState (RPC server-side, lato follower): applica lo stato ricevuto
// dal leader e riarma il watchdog.
//
// Gestione dei conflitti sul leader_id ricevuto rispetto a quello gia'
// riconosciuto (lb.election.leaderID):
//   - se e' PIU' DEBOLE (priorita' minore) del leader gia' riconosciuto,
//     viene scartato: e' un residuo di una race gia' risolta altrove, o un
//     leader decaduto che non si e' ancora accorto di esserlo.
//   - se e' PIU' FORTE (priorita' maggiore), lo stato locale viene
//     AGGIORNATO invece di scartarlo. Prima questo ramo scartava sempre
//     qualsiasi mismatch: se l'AnnounceCoordinator del vero leader non
//     riusciva a essere recapitato (es. il peer non era ancora raggiungibile
//     al momento dell'elezione, tipico con avvii non sincronizzati / go run
//     lento a compilare), il nodo restava bloccato a credersi leader per
//     sempre, anche quando il SyncState del leader vero ricominciava ad
//     arrivare regolarmente — SyncState e' infatti riprovato a ogni ciclo di
//     heartbeat, mentre AnnounceCoordinator viene tentato solo poche volte
//     al momento dell'elezione e poi mai piu'. Nel Bully la priorita' piu'
//     alta vince sempre: non c'e' nulla di ambiguo nell'accettare l'upgrade.
func (lb *LoadBalancer) HandleSyncState(ctx context.Context, req *pb.StateUpdateRequest) (*pb.Ack, error) {
	log.Printf("[%s] 💓 Ricevuto SyncState da Leader ID=%d", lb.ID, req.GetLeaderId())

	lb.election.mu.Lock()
	recognizedLeader := lb.election.leaderID
	currentState := lb.election.state

	// 1. SCARTO: Ignoro stati provenienti da "ex-leader" o nodi con priorità inferiore
	// al leader legittimo che già riconosco.
	if recognizedLeader != 0 && req.GetLeaderId() < recognizedLeader {
		lb.election.mu.Unlock()
		log.Printf("[%s] SyncState ignorato: arrivato da leader_id=%d, riconosco leader_id=%d",
			lb.ID, req.GetLeaderId(), recognizedLeader)
		return &pb.Ack{Success: false}, nil
	}

	// 2. ACCETTAZIONE: Il leader è valido. Aggiorno lo stato a prescindere che
	// recognizedLeader sia 0 (boot) o diverso.
	isNewSuperiorLeader := req.GetLeaderId() > lb.Priority && currentState == StateLeader

	if req.GetLeaderId() != recognizedLeader || currentState == StateCandidate {
		log.Printf("[%s] Aggiorno leader riconosciuto: %d -> %d (via SyncState)",
			lb.ID, recognizedLeader, req.GetLeaderId())
	}

	lb.election.state = StateFollower
	lb.election.leaderID = req.GetLeaderId()
	lb.election.electionInProgress = false
	lb.closeElectionDoneLocked() // Sblocca immediatamente i nodi in attesa!

	// Rilascio il lock PRIMA di chiamare StopLeaderDuties per evitare Deadlock
	lb.election.mu.Unlock()

	// 3. AZIONI ESTERNE AL LOCK
	lb.resetWatchdog()

	if isNewSuperiorLeader {
		log.Printf("[%s] 🙇 Riconosciuto leader superiore (ID=%d): cedo il comando.", lb.ID, req.GetLeaderId())
		lb.StopLeaderDuties()
	}

	lb.applyStateUpdate(req.GetWorkers())
	return &pb.Ack{Success: true}, nil
}

// applyStateUpdate aggiorna la copia locale (RAM) dei worker con i dati
// ricevuti dal leader: e' cosi' che i follower instradano il traffico HTTP
// con eventual consistency, senza mai interrogare Scaphandre direttamente.
func (lb *LoadBalancer) applyStateUpdate(data map[string]*pb.WorkerData) {
	for _, w := range lb.Workers {
		wd, ok := data[w.ID]
		if !ok {
			continue
		}
		w.mu.Lock()
		w.PowerWatts = float64(wd.GetPowerWatts())
		w.CarbonInt = float64(wd.GetCarbonIntensity())
		w.IsActive = wd.GetIsActive()
		w.mu.Unlock()

		log.Printf("[%s] 🔄 Worker %s aggiornato -> Power: %.2fW | CO2: %.2f gCO2eq/kWh",
			lb.ID, w.ID, w.PowerWatts, w.CarbonInt)
	}
}

// ---------------------------------------------------------------------
// Carbon intensity-related functions
// ---------------------------------------------------------------------

// updateCarbonIntensity queries Electricity Maps and updates the worker
func (lb *LoadBalancer) updateCarbonIntensity() {
	isMock := os.Getenv("MOCK_ELECTRICITY_API") == "true"

	token := os.Getenv("ELECTRICITY_MAPS_TOKEN")
	if token == "" {
		token = lb.Config.LB.ElectricityMapsToken
	}

	// Se il mock non è attivo e non c'è un token valido, salta la chiamata
	if !isMock && (token == "" || token == "YOUR_TOKEN_HERE") {
		log.Printf("[%s] ⚠️ Nessun token Electricity Maps configurato. Query saltata.", lb.ID)
		return
	}

	// Deduplicates the zones to save API calls if there are multiple workers for one zone
	zoneMap := make(map[string]float64)
	client := &http.Client{Timeout: 8 * time.Second}

	for _, w := range lb.Workers {
		if w.Zone == "" {
			continue
		}
		if _, evaluated := zoneMap[w.Zone]; evaluated {
			continue
		}

		intensity, err := lb.fetchZoneCarbonIntensity(client, w.Zone, token)
		if err != nil {
			log.Printf("[%s] ⚠️ Errore fetch Electricity Maps per zona %s: %v", lb.ID, w.Zone, err)
			continue
		}
		zoneMap[w.Zone] = intensity
		log.Printf("[%s] 🌍 Carbon Intensity aggiornata [%s]: %.2f gCO2eq/kWh", lb.ID, w.Zone, intensity)
	}

	// Values assigned concurrently to every worker
	for _, w := range lb.Workers {
		if val, exists := zoneMap[w.Zone]; exists {
			w.mu.Lock()
			w.CarbonInt = val
			w.mu.Unlock()
		}
	}
}

func (lb *LoadBalancer) fetchZoneCarbonIntensity(client *http.Client, zone, token string) (float64, error) {
	// Carbon intensity mock to save API calls
	if os.Getenv("MOCK_ELECTRICITY_API") == "true" {
		return mockCarbonIntensity(zone), nil
	}
	apiURL := fmt.Sprintf("https://api.electricitymap.org/v3/carbon-intensity/latest?zone=%s", zone)

	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("auth-token", token)

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("API response status code %d", resp.StatusCode)
	}

	var data ElectricityMapResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0, err
	}

	return data.CarbonIntensity, nil
}

// mockCarbonIntensity generates mocked values
func mockCarbonIntensity(zone string) float64 {
	var baseIntensity float64

	switch zone {
	case "SE":
		baseIntensity = 20.0 // Sweden
	case "FR":
		baseIntensity = 60.0 // France
	case "GB":
		baseIntensity = 180.0 // UK
	case "IT-NO":
		baseIntensity = 320.0 // Italy
	case "DE":
		baseIntensity = 400.0 // Germany
	case "PL":
		baseIntensity = 700.0 // Polony
	default:
		baseIntensity = 250.0 // Fallback
	}

	// Casual fluctuation of 5% for realism
	fluctuation := baseIntensity * 0.05
	randomOffset := (rand.Float64() * fluctuation * 2) - fluctuation

	return baseIntensity + randomOffset
}

// ---------------------------------------------------------------------
// Adapter verso l'interfaccia generata pb.ClusterServiceServer
// ---------------------------------------------------------------------

// clusterServer inoltra le RPC del ClusterService ai metodi Handle* di
// LoadBalancer. Serve solo perche' LoadBalancer.StartElection() (avvio di
// un'elezione, zero argomenti) e la RPC StartElection(ctx, req) del proto
// condividono lo stesso nome: separarli evita l'ambiguita'.
type clusterServer struct {
	pb.UnimplementedClusterServiceServer
	lb *LoadBalancer
}

func (s *clusterServer) StartElection(ctx context.Context, req *pb.ElectionRequest) (*pb.ElectionResponse, error) {
	return s.lb.HandleStartElection(ctx, req)
}

func (s *clusterServer) AnnounceCoordinator(ctx context.Context, req *pb.CoordinatorAnnouncement) (*pb.Ack, error) {
	return s.lb.HandleAnnounceCoordinator(ctx, req)
}

func (s *clusterServer) SyncState(ctx context.Context, req *pb.StateUpdateRequest) (*pb.Ack, error) {
	return s.lb.HandleSyncState(ctx, req)
}

func (lb *LoadBalancer) NewClusterServer() pb.ClusterServiceServer {
	return &clusterServer{lb: lb}
}
