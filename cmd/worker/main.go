package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

	activeTasks int64
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
			scaphURL = "http://scaphandre:8080/metrics"
		}

		watts := fetchScaphandreWatts(scaphURL)

		// Update only if the value is valid
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
	// 1. Timeout rigoroso per non far fallire l'Heartbeat gRPC
	client := http.Client{
		Timeout: 800 * time.Millisecond,
	}

	resp, err := client.Get(url)
	if err != nil {
		return mockPowerWatts()
	}
	defer resp.Body.Close()

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
					log.Printf("[WORKER] ⚡ Scaphandre letto con successo: %.2f W", watts)
					return watts
				}
			}
		}
	}

	// In case the buffer is truncated
	if err := scanner.Err(); err != nil {
		log.Printf("[WORKER] ⚠️ Errore buffer su Scaphandre: %v", err)
	} else {
		log.Printf("[WORKER] ⚠️ Metrica scaph_host_power non trovata nel testo.")
	}

	return mockPowerWatts()
}

// mockPowerWatts estimates instantaneous power when real hardware
// (Scaphandre/RAPL) is unavailable.
// Instead of a fixed value, the model reproduces the typical power profile
// of a real CPU package, ensuring the reported value genuinely responds
// to the number of concurrent requests in progress.
func mockPowerWatts() float32 {
	idleWatts := float64(2.50)
	if val := os.Getenv("DEFAULT_POWER_WATTS"); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			idleWatts = parsed
		}
	}
	dynamicWatts := float64(12.0)
	if val := os.Getenv("MOCK_DYNAMIC_POWER_WATTS"); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			dynamicWatts = parsed
		}
	}

	saturationTasks := float64(8.0)
	if val := os.Getenv("SATURATION_TASKS"); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			saturationTasks = parsed
		}
	}

	tasks := float64(atomic.LoadInt64(&activeTasks))
	utilization := 1 - math.Exp(-tasks/saturationTasks)

	base := idleWatts + dynamicWatts*utilization

	// Small fluctuation
	fluctuation := base * 0.04
	offset := (rand.Float64() * fluctuation * 2) - fluctuation

	return float32(base + offset)
}

// handleTask simulates the working load
func handleTask(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&activeTasks, 1)
	defer atomic.AddInt64(&activeTasks, -1)

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
