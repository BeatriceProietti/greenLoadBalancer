package main

import (
	"encoding/csv"
	"flag"
	// "fmt"
	"log"
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
	LBNode          string
	LBRole          string
	LBStrategy      string
	WorkerID        string
	WorkerZone      string
	WorkerWatts     string
	WorkerCarbonInt string
}

func main() {
	urlsFlag := flag.String("urls", "http://localhost:8080/task,http://localhost:8090/task,http://localhost:8100/task", "Lista separata da virgole degli URL del cluster LB")
	concurrency := flag.Int("c", 10, "Numero di worker concorrenti (client)")
	totalRequests := flag.Int("n", 200, "Numero totale di richieste da inviare")
	outputCSV := flag.String("o", "benchmark_results.csv", "Nome del file CSV di output")
	flag.Parse()

	targetURLs := strings.Split(*urlsFlag, ",")
	if len(targetURLs) == 0 || targetURLs[0] == "" {
		log.Fatal("Nessun URL target specificato.")
	}

	log.Printf("Avvio benchmark: %d richieste (concorrenza: %d) distribuite su %d nodi LB", *totalRequests, *concurrency, len(targetURLs))

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
		"LB_Node",
		"LB_Role",
		"LB_Strategy",
		"Worker_ID",
		"Worker_Zone",
		"Worker_Power_Watts",
		"Worker_Carbon_Intensity",
	})

	jobs := make(chan int, *totalRequests)
	results := make(chan ResultRecord, *totalRequests)

	// Client HTTP
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
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
			for range jobs {
				// Client-Side Round Robin
				idx := atomic.AddUint64(&reqCounter, 1) % uint64(len(targetURLs))
				selectedURL := strings.TrimSpace(targetURLs[idx])

				start := time.Now()
				resp, err := httpClient.Get(selectedURL)
				latency := time.Since(start).Milliseconds()
				timestamp := time.Now().Format(time.RFC3339)

				if err != nil {
					results <- ResultRecord{
						Timestamp:       timestamp,
						LatencyMs:       latency,
						StatusCode:      "ERROR",
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