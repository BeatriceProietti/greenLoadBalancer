package main

import (
	"encoding/csv"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ResultRecord struct {
	Timestamp       string
	LatencyMs       int64
	StatusCode      string
	Difficulty      int
	LBNode          string
	LBRole          string
	LBStrategy      string
	WorkerID        string
	WorkerZone      string
	WorkerWatts     string
	WorkerCarbonInt string
}

// getEnv legge una variabile d'ambiente con un default, cosi' il comando
// eseguito dal container (docker-compose.yaml) puo' restare identico per
// tutti gli esperimenti: cambia solo l'environment, non l'entrypoint. E'
// lo stesso pattern gia' usato in cmd/lb/main.go per LB_STRATEGY/LB_ALPHA.
func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return def
}

func getEnvInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed
		}
	}
	return def
}

func main() {
	urlsFlag := flag.String("urls", getEnv("LOADGEN_URLS",
		"http://localhost:8080/task,http://localhost:8090/task,http://localhost:8100/task,http://localhost:8110/task,http://localhost:8120/task"),
		"Lista separata da virgole degli URL del cluster LB")
	concurrency := flag.Int("c", getEnvInt("LOADGEN_CONCURRENCY", 10), "Numero di worker concorrenti (client)")
	totalRequests := flag.Int("n", getEnvInt("LOADGEN_REQUESTS", 200), "Numero totale di richieste da inviare")
	outputCSV := flag.String("o", getEnv("LOADGEN_OUTPUT", "benchmark_results.csv"), "Nome del file CSV di output")

	// Difficolta' variabile del task: senza questo, ogni richiesta usava il
	// default del worker (difficulty=10000) sempre identico — nessun mix di
	// carichi leggeri/pesanti, quindi GreenLC (che pesa il costo anche sulle
	// richieste attive) e GreenTopK non avevano mai davvero occasione di
	// differenziarsi dal comportamento della baseline. Il seed e' FISSO di
	// default (non un timestamp) apposta: con lo stesso seed e lo stesso -n,
	// la sequenza di difficolta' e' IDENTICA a ogni run — condizione
	// necessaria per confrontare le policy a parita' di carico offerto.
	difficultyMin := flag.Int("difficulty-min", getEnvInt("LOADGEN_DIFFICULTY_MIN", 50000), "Difficolta' minima (hash SHA-256) per richiesta")
	difficultyMax := flag.Int("difficulty-max", getEnvInt("LOADGEN_DIFFICULTY_MAX", 4000000), "Difficolta' massima (hash SHA-256) per richiesta")
	seed := flag.Int64("seed", getEnvInt64("LOADGEN_SEED", 42), "Seed del generatore di difficolta' (stesso seed = stessa sequenza di richieste)")
	flag.Parse()

	targetURLs := strings.Split(*urlsFlag, ",")
	if len(targetURLs) == 0 || targetURLs[0] == "" {
		log.Fatal("Nessun URL target specificato.")
	}
	if *difficultyMax < *difficultyMin {
		log.Fatalf("difficulty-max (%d) non puo' essere minore di difficulty-min (%d)", *difficultyMax, *difficultyMin)
	}

	log.Printf("Avvio benchmark: %d richieste (concorrenza: %d) distribuite su %d nodi LB", *totalRequests, *concurrency, len(targetURLs))
	for i, u := range targetURLs {
		log.Printf("  target[%d] = %s", i, strings.TrimSpace(u))
	}
	log.Printf("Difficolta': [%d, %d], seed=%d (sequenza deterministica e riproducibile tra run/policy diverse)",
		*difficultyMin, *difficultyMax, *seed)

	// CSV initialization
	file, err := os.Create(*outputCSV)
	if err != nil {
		log.Fatalf("Errore creazione file CSV: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// File header
	writer.Write([]string{
		"Timestamp",
		"Latency_ms",
		"Status",
		"Difficulty",
		"LB_Node",
		"LB_Role",
		"LB_Strategy",
		"Worker_ID",
		"Worker_Zone",
		"Worker_Power_Watts",
		"Worker_Carbon_Intensity",
	})

	// Sequenza di difficolta' pre-generata (una per ogni richiesta, nell'ordine
	// in cui i job vengono creati) da un RNG seedato: stesso -n e stesso -seed
	// producono sempre la stessa sequenza, indipendentemente dalla policy
	// del LB in prova in quel momento.
	rng := rand.New(rand.NewSource(*seed))
	difficulties := make([]int, *totalRequests)
	spread := *difficultyMax - *difficultyMin
	for i := range difficulties {
		if spread > 0 {
			difficulties[i] = *difficultyMin + rng.Intn(spread)
		} else {
			difficulties[i] = *difficultyMin
		}
	}

	jobs := make(chan int, *totalRequests)
	results := make(chan ResultRecord, *totalRequests)

	// Client HTTP
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	var reqCounter uint64
	var wg sync.WaitGroup

	// Concurrent worker pool start
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for jobIdx := range jobs {
				// Client-Side Round Robin
				idx := atomic.AddUint64(&reqCounter, 1) % uint64(len(targetURLs))
				selectedURL := strings.TrimSpace(targetURLs[idx])
				difficulty := difficulties[jobIdx]
				requestURL := selectedURL + "?difficulty=" + strconv.Itoa(difficulty)

				start := time.Now()
				resp, err := httpClient.Get(requestURL)
				latency := time.Since(start).Milliseconds()
				timestamp := time.Now().Format(time.RFC3339)

				if err != nil {
					results <- ResultRecord{
						Timestamp:       timestamp,
						LatencyMs:       latency,
						StatusCode:      "ERROR",
						Difficulty:      difficulty,
						LBNode:          "N/A",
						LBRole:          "N/A",
						LBStrategy:      "N/A",
						WorkerID:        "N/A",
						WorkerZone:      "N/A",
						WorkerWatts:     "N/A",
						WorkerCarbonInt: "N/A",
					}
					continue
				}

				// Header HTTP extraction
				rec := ResultRecord{
					Timestamp:       timestamp,
					LatencyMs:       latency,
					StatusCode:      strconv.Itoa(resp.StatusCode),
					Difficulty:      difficulty,
					LBNode:          resp.Header.Get("X-LB-Node"),
					LBRole:          resp.Header.Get("X-LB-Role"),
					LBStrategy:      resp.Header.Get("X-LB-Strategy"),
					WorkerID:        resp.Header.Get("X-Worker-ID"),
					WorkerZone:      resp.Header.Get("X-Worker-Zone"),
					WorkerWatts:     resp.Header.Get("X-Worker-Power-Watts"),
					WorkerCarbonInt: resp.Header.Get("X-Worker-Carbon-Intensity"),
				}

				resp.Body.Close()
				results <- rec
			}
		}()
	}

	// Job send
	for i := 0; i < *totalRequests; i++ {
		jobs <- i
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	// Synchronous writing of the results
	successCount := 0
	var totalLatency int64 = 0

	for r := range results {
		writer.Write([]string{
			r.Timestamp,
			strconv.FormatInt(r.LatencyMs, 10),
			r.StatusCode,
			strconv.Itoa(r.Difficulty),
			r.LBNode,
			r.LBRole,
			r.LBStrategy,
			r.WorkerID,
			r.WorkerZone,
			r.WorkerWatts,
			r.WorkerCarbonInt,
		})

		if r.StatusCode == "200" {
			successCount++
			totalLatency += r.LatencyMs
		}
	}

	avgLatency := float64(0)
	if successCount > 0 {
		avgLatency = float64(totalLatency) / float64(successCount)
	}

	log.Printf("Benchmark completato: %d/%d richieste OK. Latenza media: %.2f ms. Dati salvati in: %s",
		successCount, *totalRequests, avgLatency, *outputCSV)
}
