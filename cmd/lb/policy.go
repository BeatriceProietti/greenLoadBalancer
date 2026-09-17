package main

import (
	"math"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// BalancingPolicy interface
type BalancingPolicy interface {
	NextWorker(workers []*WorkerNode) *WorkerNode
}

// getActiveWorkers filters and returns only the active workers that respect the circuit breaker limit.
func getActiveWorkers(workers []*WorkerNode, maxReqs int) []*WorkerNode {
	var active []*WorkerNode

	// Fallback
	if maxReqs <= 0 {
		maxReqs = 100
	}

	for _, w := range workers {
		w.mu.RLock()
		isActive := w.IsActive
		activeReqs := w.ActiveRequests
		w.mu.RUnlock()

		if isActive && activeReqs < maxReqs {
			active = append(active, w)
		}
	}
	return active
}

// 1. ROUND ROBIN
type RoundRobin struct {
	counter uint64
	MaxReqs int
}

func (s *RoundRobin) NextWorker(workers []*WorkerNode) *WorkerNode {
	active := getActiveWorkers(workers, s.MaxReqs)
	if len(active) == 0 {
		return nil
	}
	idx := atomic.AddUint64(&s.counter, 1) % uint64(len(active))
	return active[idx]
}

// 2. STANDARD LEAST CONNECTIONS
type LeastConnections struct {
	MaxReqs int
}

func (s *LeastConnections) NextWorker(workers []*WorkerNode) *WorkerNode {
	active := getActiveWorkers(workers, s.MaxReqs)
	if len(active) == 0 {
		return nil
	}

	var best *WorkerNode
	minReqs := math.MaxInt32

	for _, w := range active {
		w.mu.RLock()
		reqs := w.ActiveRequests
		w.mu.RUnlock()

		if reqs < minReqs {
			minReqs = reqs
			best = w
		}
	}
	return best
}

// 3. GREEN RANDOM TOP K%
type GreenTopK struct {
	TopKPercent float64
	MaxReqs     int
}

func (s *GreenTopK) NextWorker(workers []*WorkerNode) *WorkerNode {
	active := getActiveWorkers(workers, s.MaxReqs)
	if len(active) == 0 {
		return nil
	}

	type candidate struct {
		node  *WorkerNode
		score float64
	}

	candidates := make([]candidate, len(active))
	for i, w := range active {
		w.mu.RLock()
		// Green score
		score := w.PowerWatts * w.CarbonInt
		w.mu.RUnlock()
		candidates[i] = candidate{node: w, score: score}
	}

	// Ascending sort based on environmental impact
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score < candidates[j].score
	})

	// Computation of the top-k group's size
	k := int(math.Ceil(float64(len(candidates)) * s.TopKPercent))
	if k < 1 {
		k = 1
	}
	if k > len(candidates) {
		k = len(candidates)
	}

	// Thundering herd limitation
	selectedIdx := rand.Intn(k)
	return candidates[selectedIdx].node
}

// 4. GREEN LEAST CONNECTIONS (exponential model)
type GreenLC struct {
	Alpha   float64
	MaxReqs int
}

func (s *GreenLC) NextWorker(workers []*WorkerNode) *WorkerNode {
	active := getActiveWorkers(workers, s.MaxReqs)
	if len(active) == 0 {
		return nil
	}

	var best *WorkerNode
	lowestCost := math.MaxFloat64

	for _, w := range active {
		w.mu.RLock()
		watts := w.PowerWatts
		co2 := w.CarbonInt
		reqs := float64(w.ActiveRequests)
		w.mu.RUnlock()

		// Fallback
		if watts <= 0 {
			watts = 1.0
		}
		if co2 <= 0 {
			co2 = 50.0
		}

		baseImpact := watts * co2

		// Exponential penalty: cost = BaseImpact * (1 + alpha)^ActiveRequests
		totalCost := baseImpact * math.Pow(1.0+s.Alpha, reqs)

		if totalCost < lowestCost {
			lowestCost = totalCost
			best = w
		}
	}
	return best
}

func PolicyFactory(strategyName string, alpha float64, topK float64, maxReqs int) BalancingPolicy {
	switch strategyName {
	case "round_robin":
		return &RoundRobin{MaxReqs: maxReqs}
	case "least_connections":
		return &LeastConnections{MaxReqs: maxReqs}
	case "green_top_k":
		return &GreenTopK{TopKPercent: topK, MaxReqs: maxReqs}
	case "green_lc":
		return &GreenLC{Alpha: alpha, MaxReqs: maxReqs}
	default:
		return &RoundRobin{MaxReqs: maxReqs}
	}
}
