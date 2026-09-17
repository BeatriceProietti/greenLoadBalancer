package configs

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type WorkerConfig struct {
	ID          string `yaml:"id"`
	URL         string `yaml:"url"`
	GRPCAddress string `yaml:"grpc_address"`
	Zone        string `yaml:"zone"`
}

// LBNodeConfig descrive UN nodo del cluster LB (se stesso incluso).
// La stessa lista, con lo stesso config.docker.yaml montato su ogni container,
// viene letta da tutti i nodi: ciascuno si riconosce tramite LB_ID (env var)
// e tratta gli altri elementi della lista come peer per il Bully.
type LBNodeConfig struct {
	ID          string `yaml:"id"`
	Priority    int32  `yaml:"priority"`     // ID numerico usato dal Bully per il confronto
	GRPCAddress string `yaml:"grpc_address"` // indirizzo su cui il nodo espone ClusterService
}

type LBConfig struct {
	Port                string  `yaml:"port"`
	Strategy            string  `yaml:"strategy"`
	Alpha               float64 `yaml:"alpha"`
	TopKPercent         float64 `yaml:"top_k_percent"`
	MaxActiveRequests   int     `yaml:"max_active_requests"`
	MaxMissedHeartbeats int     `yaml:"max_missed_heartbeats"`

	// Tempi espressi in millisecondi nello YAML (niente tipo Duration nativo
	// in YAML): li convertiamo con i metodi sotto, invece di fare a*time.Millisecond
	// sparso per il codice.
	HeartbeatIntervalMs      int `yaml:"heartbeat_interval_ms"`
	ElectionTimeoutMs        int `yaml:"election_timeout_ms"`
	RPCTimeoutMs             int `yaml:"rpc_timeout_ms"`
	CoordinatorWaitTimeoutMs int `yaml:"coordinator_wait_timeout_ms"`

	// data reletated to CO2 measurement
	CarbonQueryIntervalSec int    `yaml:"carbon_query_interval_s"`
	ElectricityMapsToken   string `yaml:"electricity_maps_token"`

	Nodes []LBNodeConfig `yaml:"nodes"`
}

func (c LBConfig) HeartbeatInterval() time.Duration {
	return time.Duration(c.HeartbeatIntervalMs) * time.Millisecond
}

func (c LBConfig) ElectionTimeout() time.Duration {
	return time.Duration(c.ElectionTimeoutMs) * time.Millisecond
}

func (c LBConfig) RPCTimeout() time.Duration {
	return time.Duration(c.RPCTimeoutMs) * time.Millisecond
}

func (c LBConfig) CoordinatorWaitTimeout() time.Duration {
	return time.Duration(c.CoordinatorWaitTimeoutMs) * time.Millisecond
}

// LeaderSuspicionTimeout e' la durata di silenzio dal leader dopo la quale
// un follower lo dichiara sospetto morto: N heartbeat mancati consecutivi,
// non un singolo timeout secco (vedi PDF, Q1/Q2).
func (c LBConfig) LeaderSuspicionTimeout() time.Duration {
	return time.Duration(c.MaxMissedHeartbeats) * c.HeartbeatInterval()
}

type Config struct {
	LB      LBConfig       `yaml:"lb"`
	Workers []WorkerConfig `yaml:"workers"`
}

// LoadConfig legge il file yaml e restituisce la struct
func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c LBConfig) CarbonQueryInterval() time.Duration {
	if c.CarbonQueryIntervalSec <= 0 {
		return 5 * time.Minute // default
	}
	return time.Duration(c.CarbonQueryIntervalSec) * time.Second
}
