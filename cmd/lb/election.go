package main

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "greenLoadBalancer/internal/pb"
)

// NodeState represents the current role of the node in the LB cluster
type NodeState int32

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func (s NodeState) String() string {
	switch s {
	case StateFollower:
		return "FOLLOWER"
	case StateCandidate:
		return "CANDIDATE"
	case StateLeader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// PeerNode is another node in the LB cluster, which can be reached via ClusterService
type PeerNode struct {
	ID       string
	Priority int32
	Address  string
	Client   pb.ClusterServiceClient
}

// ElectionState groups together all the mutable state of the election algorithm
type ElectionState struct {
	mu sync.RWMutex

	state      NodeState
	leaderID   int32
	leaderAddr string

	electionInProgress bool
	electionDone       chan struct{} // closed when the current election is concluded

	watchdog *time.Timer // triggers a new election if the leader remains silent for too long

	leaderDutiesStop chan struct{} // non-nil if this node is currently performing "leader duties"
}

// ConnectToPeers creates gRPC clients to all other cluster nodes
// listed in config.docker.yaml (lb.nodes), excluding itself (ID and Priority of
// this node have already been resolved in main() prior to this call)
func (lb *LoadBalancer) ConnectToPeers() {
	for _, n := range lb.Config.LB.Nodes {
		if n.ID == lb.ID {
			continue
		}
		conn, err := grpc.NewClient(n.GRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[%s] Impossibile creare client verso peer %s: %v", lb.ID, n.ID, err)
			continue
		}
		lb.Peers = append(lb.Peers, &PeerNode{
			ID:       n.ID,
			Priority: n.Priority,
			Address:  n.GRPCAddress,
			Client:   pb.NewClusterServiceClient(conn),
		})
	}
}

// ---------------------------------------------------------------------
// Start election (candidate side)
// ---------------------------------------------------------------------

// StartElection initiates a Bully election attempt. It can be called:
// - at boot, explicitly from main(), because no node knows the leader yet;
// - by the watchdog, when a follower suspects the leader is dead (N missed heartbeats);
// - by a candidate whose wait for the Coordinator times out (retry).
func (lb *LoadBalancer) StartElection() {
	lb.election.mu.Lock()
	if lb.election.electionInProgress {
		lb.election.mu.Unlock()
		return // an election is already occurring
	}
	lb.election.electionInProgress = true
	lb.election.state = StateCandidate
	done := make(chan struct{})
	lb.election.electionDone = done
	lb.election.mu.Unlock()

	log.Printf("[%s] 🗳️  Avvio elezione Bully (priority=%d)", lb.ID, lb.Priority)

	higherPeers := lb.peersWithHigherPriority()

	if len(higherPeers) == 0 {
		// From my pov there is no node with higher priority: I elect myself
		lb.becomeLeader()
		return
	}

	stepDownReceived := make(chan struct{}, len(higherPeers))
	var wg sync.WaitGroup
	for _, p := range higherPeers {
		wg.Add(1)
		go func(peer *PeerNode) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), lb.Config.LB.ElectionTimeout())
			defer cancel()
			resp, err := peer.Client.StartElection(ctx, &pb.ElectionRequest{CandidateId: lb.Priority})
			if err == nil && resp.GetStepDown() {
				select {
				case stepDownReceived <- struct{}{}:
				default:
				}
			}
		}(p)
	}

	// Aspettiamo la prima risposta utile, oppure il timeout di elezione se
	// tutti i nodi con priorita' maggiore sono irraggiungibili (presunti morti).
	//
	// C'e' un terzo evento possibile a cui il semplice "stepDown o timeout"
	// non basta a reagire: un nodo con priorita' ancora piu' alta di quella
	// dei nostri "higherPeers" puo' essersi auto-eletto ED essersi gia'
	// annunciato (AnnounceCoordinator, che chiude "done") PRIMA che le nostre
	// RPC di elezione ricevano risposta — tipico al boot, quando i processi
	// non partono tutti nello stesso istante esatto. Senza il case <-done,
	// il timeout scatterebbe comunque alla scadenza e ci autoeleggeremmo
	// sopra un leader legittimo gia' riconosciuto (bug riprodotto e osservato
	// nei log di verifica: vedi PDF, Q4 "concurrent elections").
	select {
	case <-stepDownReceived:
		go func() { wg.Wait() }()
		lb.awaitCoordinatorOrRetry(done)
	case <-done:
		// A legitimate announce is already arrived: nothing to do
	case <-time.After(lb.Config.LB.ElectionTimeout()):
		// select chooses randomly (if done and timeout expire at the same time):
		// let's verify the real state
		lb.election.mu.Lock()
		stillMine := lb.election.electionDone == done
		lb.election.mu.Unlock()
		if !stillMine {
			return
		}
		lb.becomeLeader()
	}
}

// peersWithHigherPriority return peer with higher priority
func (lb *LoadBalancer) peersWithHigherPriority() []*PeerNode {
	var higher []*PeerNode
	for _, p := range lb.Peers {
		if p.Priority > lb.Priority {
			higher = append(higher, p)
		}
	}
	return higher
}

// awaitCoordinatorOrRetry waits for the announcement of the new leader (closure of the
// // "done" channel associated with this election attempt). If the timeout expires
// // without any Coordinator being announced, the node with higher priority
// // that had responded "step down" has itself failed in the
// // meantime: the process starts over from the beginning.
func (lb *LoadBalancer) awaitCoordinatorOrRetry(done chan struct{}) {
	select {
	case <-done:
	case <-time.After(lb.Config.LB.CoordinatorWaitTimeout()):
		log.Printf("[%s] Nessun annuncio di coordinatore ricevuto in tempo: rieleggo.", lb.ID)
		lb.election.mu.Lock()
		lb.election.electionInProgress = false
		lb.election.mu.Unlock()
		lb.StartElection()
	}
}

// ---------------------------------------------------------------------
// Becoming leader / acknowledge a new leader
// ---------------------------------------------------------------------

func (lb *LoadBalancer) becomeLeader() {
	lb.election.mu.Lock()
	lb.election.state = StateLeader
	lb.election.leaderID = lb.Priority
	lb.election.leaderAddr = lb.SelfGRPCAddress
	lb.election.electionInProgress = false
	lb.closeElectionDoneLocked()
	lb.stopWatchdogLocked() // leader doesn't suspect itself
	lb.election.mu.Unlock()

	log.Printf("[%s] 👑 Sono il nuovo LEADER (priority=%d)", lb.ID, lb.Priority)

	for _, p := range lb.Peers {
		go func(peer *PeerNode) {
			// 3 attempts
			for attempt := 0; attempt < 3; attempt++ {
				ctx, cancel := context.WithTimeout(context.Background(), lb.Config.LB.RPCTimeout())
				_, err := peer.Client.AnnounceCoordinator(ctx, &pb.CoordinatorAnnouncement{
					LeaderId:      lb.Priority,
					LeaderAddress: lb.SelfGRPCAddress,
				})
				cancel()
				if err == nil {
					return // Recapito riuscito
				}
				time.Sleep(300 * time.Millisecond)
			}
			log.Printf("[%s] ⚠️ Peer %s offline (AnnounceCoordinator non recapitato)", lb.ID, peer.ID)
		}(p)
	}

	lb.StartLeaderDuties()
}

// StartElection (RPC server-side): another node asks me to participate
// // in an election as a candidate with lower or equal priority to mine
// // (by design, in the Bully algorithm, only a node with higher priority
// // than the caller receives this RPC).
func (lb *LoadBalancer) HandleStartElection(ctx context.Context, req *pb.ElectionRequest) (*pb.ElectionResponse, error) {
	if req.GetCandidateId() < lb.Priority {
		// I initiate an election of my own only if I
		// do not already have a recognized leader (true bootstrap, leaderID still 0):
		// Verify that I am already the leader, or a follower with a valid leader, to avoid concurrent elections.
		lb.election.mu.RLock()
		hasKnownLeader := lb.election.state == StateLeader ||
			(lb.election.state == StateFollower && lb.election.leaderID != 0)
		lb.election.mu.RUnlock()
		if !hasKnownLeader {
			go lb.StartElection()
		}
		return &pb.ElectionResponse{StepDown: true}, nil
	}
	return &pb.ElectionResponse{StepDown: false}, nil
}

func (lb *LoadBalancer) HandleAnnounceCoordinator(ctx context.Context, req *pb.CoordinatorAnnouncement) (*pb.Ack, error) {
	lb.election.mu.Lock()

	// If I am the leader with higher priority: deny
	if lb.election.state == StateLeader && req.GetLeaderId() < lb.Priority {
		lb.election.mu.Unlock()
		log.Printf("[%s] AnnounceCoordinator da leader_id=%d ignorato: sono gia' leader con priority=%d",
			lb.ID, req.GetLeaderId(), lb.Priority)
		return &pb.Ack{Success: false}, nil
	}

	// If I am a follower with a legittimate leader, which has higher priority than the announced one: deny
	if lb.election.state == StateFollower && lb.election.leaderID > req.GetLeaderId() {
		lb.election.mu.Unlock()
		log.Printf("[%s] AnnounceCoordinator da leader_id=%d ignorato: riconosco gia' leader_id=%d",
			lb.ID, req.GetLeaderId(), lb.election.leaderID)
		return &pb.Ack{Success: false}, nil
	}

	wasLeader := lb.election.state == StateLeader
	lb.election.state = StateFollower
	lb.election.leaderID = req.GetLeaderId()
	lb.election.leaderAddr = req.GetLeaderAddress()
	lb.election.electionInProgress = false
	lb.closeElectionDoneLocked()
	lb.election.mu.Unlock()

	lb.resetWatchdog()

	if wasLeader {
		// If a node was a leader but it gets a message from a higher-priority node:
		lb.StopLeaderDuties()
	}

	log.Printf("[%s] Nuovo leader riconosciuto: %s (priority=%d)", lb.ID, req.GetLeaderAddress(), req.GetLeaderId())
	return &pb.Ack{Success: true}, nil
}

func (lb *LoadBalancer) closeElectionDoneLocked() {
	if lb.election.electionDone != nil {
		close(lb.election.electionDone)
		lb.election.electionDone = nil
	}
}

// ---------------------------------------------------------------------
// Watchdog: leader's death detection
// ---------------------------------------------------------------------

// resetWatchdog resets the suspected-dead-leader timer. It must be called every
// time a sign of life is received from the current leader (AnnounceCoordinator
// or SyncState, which here also serves as a periodic leader-to-follower heartbeat).
func (lb *LoadBalancer) resetWatchdog() {
	d := lb.Config.LB.LeaderSuspicionTimeout()
	lb.election.mu.Lock()
	defer lb.election.mu.Unlock()
	if lb.election.state == StateLeader {
		return // il leader non arma un watchdog su se stesso
	}
	if lb.election.watchdog == nil {
		lb.election.watchdog = time.AfterFunc(d, lb.onLeaderSuspectedDead)
	} else {
		lb.election.watchdog.Reset(d)
	}

	log.Printf("[%s] ⏱️  Watchdog resettato per %v", lb.ID, d)
}

func (lb *LoadBalancer) stopWatchdogLocked() {
	if lb.election.watchdog != nil {
		lb.election.watchdog.Stop()
	}
}

func (lb *LoadBalancer) onLeaderSuspectedDead() {
	lb.election.mu.RLock()
	isLeader := lb.election.state == StateLeader
	lb.election.mu.RUnlock()
	if isLeader {
		return
	}
	log.Printf("[%s] ⚠️  Nessun segnale dal leader da %v: presunto morto. Avvio elezione.",
		lb.ID, lb.Config.LB.LeaderSuspicionTimeout())
	lb.StartElection()
}
