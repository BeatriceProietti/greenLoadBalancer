package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "greenLoadBalancer/internal/pb"

	"google.golang.org/grpc"
)

var (
	region        = os.Getenv("REGION")
	workerID      = os.Getenv("WORKER_ID")
	httpPort      = os.Getenv("HTTP_PORT")
	grpcPort      = os.Getenv("GRPC_PORT")
	latestWattsMu sync.RWMutex
	latestWatts   float32
)

type workerServer struct {
	pb.UnimplementedWorkerServiceServer
	currentWatts float32
}

// Scaphandre sampled in background every 2 s
func startPowerMonitor() {
	for {
		scaphURL := os.Getenv("SCAPHANDRE_URL")
		if scaphURL == "" {
			scaphURL = "http://localhost:8080/metrics"
		}

		watts := fetchScaphandreWatts(scaphURL)

		// Aggiorniamo il valore solo se valido, altrimenti teniamo l'ultimo noto
		if watts > 0 {
			latestWattsMu.Lock()
			latestWatts = watts
			latestWattsMu.Unlock()
		}

		time.Sleep(2 * time.Second)
	}
}

func (s *workerServer) Heartbeat(ctx context.Context, req *pb.PingRequest) (*pb.TelemetryResponse, error) {
	latestWattsMu.RLock()
	watts := latestWatts
	latestWattsMu.RUnlock()

	s.currentWatts = watts

	return &pb.TelemetryResponse{
		WorkerId:          workerID,
		CurrentPowerWatts: s.currentWatts,
		IsHealthy:         true,
	}, nil
}

func main() {
	if region == "" {
		region = "eu-local"
	}
	if workerID == "" {
		workerID = "worker-01"
	}
	if httpPort == "" {
		httpPort = os.Getenv("PORT")
	}
	if httpPort == "" {
		httpPort = "8081"
	}
	if grpcPort == "" {
		grpcPort = "9081"
	}

	// Energy monitoring is started in background
	go startPowerMonitor()

	go startGRPCServer()

	http.HandleFunc("/task", handleTask)
	log.Printf("Worker [%s] (Regione: %s) avviato. HTTP su :%s, gRPC su :%s", workerID, region, httpPort, grpcPort)
	if err := http.ListenAndServe(":"+httpPort, nil); err != nil {
		log.Fatalf("Errore avvio server HTTP: %v", err)
	}
}

func startGRPCServer() {
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("Errore di ascolto gRPC sulla porta %s: %v", grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServiceServer(grpcServer, &workerServer{})

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Errore avvio server gRPC: %v", err)
	}
}

// fetchScaphandreWatts queries Prometheus
func fetchScaphandreWatts(url string) float32 {
	defaultWatts := float32(2.50) // Fallback
	if val := os.Getenv("DEFAULT_POWER_WATTS"); val != "" {
		if parsed, err := strconv.ParseFloat(val, 32); err == nil {
			defaultWatts = float32(parsed)
		}
	}

	// 1. Timeout rigoroso per non far fallire l'Heartbeat gRPC
	client := http.Client{
		Timeout: 800 * time.Millisecond,
	}

	resp, err := client.Get(url)
	if err != nil {
		// 2. Log aggiornato, evidente, e senza virgolette errate alla fine
		log.Printf("[WORKER] ⚠️ Impossibile contattare Scaphandre su %s: %v. Uso fallback: %.2fW", url, err, defaultWatts)
		return defaultWatts
	}
	defer resp.Body.Close()

	// 3. Espansione del buffer per gestire righe (cmdline) lunghissime di Scaphandre
	scanner := bufio.NewScanner(resp.Body)
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 512*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "scaph_host_power_microwatts") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				microWatts, err := strconv.ParseFloat(fields[1], 64)
				if err == nil {
					watts := float32(microWatts / 1_000_000.0)
					// 4. Log di successo aggiunto
					log.Printf("[WORKER] ⚡ Scaphandre letto con successo: %.2f W", watts)
					return watts
				}
			}
		}
	}

	// 5. Intercettazione dell'errore di buffer (se Scaphandre viene troncato)
	if err := scanner.Err(); err != nil {
		log.Printf("[WORKER] ⚠️ Errore buffer su Scaphandre: %v", err)
	} else {
		log.Printf("[WORKER] ⚠️ Metrica scaph_host_power non trovata nel testo.")
	}

	return defaultWatts
}

// handleTask simulates the working load
func handleTask(w http.ResponseWriter, r *http.Request) {
	diffStr := r.URL.Query().Get("difficulty")
	difficulty, err := strconv.Atoi(diffStr)
	if err != nil || difficulty <= 0 {
		difficulty = 10000
	}

	for i := 0; i < difficulty; i++ {
		hash := sha256.New()
		hash.Write([]byte(fmt.Sprintf("calcolo_fittizio_%d_%s", i, workerID)))
		_ = hash.Sum(nil)
	}

	latestWattsMu.RLock()
	currentW := float64(latestWatts)
	latestWattsMu.RUnlock()

	if currentW == 0 {
		currentW = 15.0
	}

	w.Header().Set("X-Worker-ID", workerID)
	w.Header().Set("X-Worker-Zone", region)
	w.Header().Set("X-Worker-Power-Watts", fmt.Sprintf("%.2f", currentW))
	w.WriteHeader(http.StatusOK)
}
