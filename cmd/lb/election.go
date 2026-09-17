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

// NodeState rappresenta il ruolo corrente del nodo nel cluster LB.
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

// PeerNode e' un altro nodo del cluster LB, raggiungibile via ClusterService.
type PeerNode struct {
	ID       string
	Priority int32
	Address  string
	Client   pb.ClusterServiceClient
}

// ElectionState raggruppa tutto lo stato mutabile dell'algoritmo di elezione.
// E' tenuto separato dal resto di LoadBalancer solo per leggibilita'.
type ElectionState struct {
	mu sync.RWMutex

	state      NodeState
	leaderID   int32
	leaderAddr string

	electionInProgress bool
	electionDone       chan struct{} // chiuso quando l'elezione corrente si conclude (vittoria propria o altrui)

	watchdog *time.Timer // fa scattare una nuova elezione se il leader tace troppo a lungo

	leaderDutiesStop chan struct{} // non-nil se questo nodo sta correntemente facendo le "leader duties"
}

// ConnectToPeers crea i client gRPC verso tutti gli altri nodi del cluster
// elencati in config.docker.yaml (lb.nodes), escludendo se stesso (ID e Priority di
// questo nodo sono gia' stati risolti in main() prima di questa chiamata).
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
// Avvio dell'elezione (lato "candidato")
// ---------------------------------------------------------------------

// StartElection avvia un tentativo di elezione Bully. Puo' essere chiamata:
//   - al boot, esplicitamente da main(), perche' nessun nodo conosce ancora il leader;
//   - dal watchdog, quando un follower sospetta il leader morto (N heartbeat mancati);
//   - da un candidato la cui attesa del Coordinator scade (retry).
func (lb *LoadBalancer) StartElection() {
	lb.election.mu.Lock()
	if lb.election.electionInProgress {
		lb.election.mu.Unlock()
		return // un'elezione e' gia' in corso: non se ne avviano di concorrenti dallo stesso nodo
	}
	lb.election.electionInProgress = true
	lb.election.state = StateCandidate
	done := make(chan struct{})
	lb.election.electionDone = done
	lb.election.mu.Unlock()

	log.Printf("[%s] 🗳️  Avvio elezione Bully (priority=%d)", lb.ID, lb.Priority)

	higherPeers := lb.peersWithHigherPriority()

	if len(higherPeers) == 0 {
		// Nessun nodo con priorita' maggiore ancora vivo dal mio punto di vista: mi eleggo.
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
		go func() { wg.Wait() }() // lascia terminare le goroutine restanti senza bloccare
		lb.awaitCoordinatorOrRetry(done)
	case <-done:
		// Risolta da altrove (un annuncio legittimo e' gia' arrivato): nulla da fare.
	case <-time.After(lb.Config.LB.ElectionTimeout()):
		// Controllo finale, autoritativo, sotto lock: anche se "done" e il
		// timeout scattano nello stesso istante (select sceglie a caso tra
		// case pronti contemporaneamente), qui verifichiamo lo stato vero
		// invece di fidarci ciecamente della sola scadenza del timer.
		lb.election.mu.Lock()
		stillMine := lb.election.electionDone == done
		lb.election.mu.Unlock()
		if !stillMine {
			return
		}
		lb.becomeLeader()
	}
}

// peersWithHigherPriority ritorna i peer con priorita' maggiore della propria:
// nel Bully si manda Election solo "verso l'alto".
func (lb *LoadBalancer) peersWithHigherPriority() []*PeerNode {
	var higher []*PeerNode
	for _, p := range lb.Peers {
		if p.Priority > lb.Priority {
			higher = append(higher, p)
		}
	}
	return higher
}

// awaitCoordinatorOrRetry aspetta l'annuncio del nuovo leader (chiusura del
// canale "done" associato a questo tentativo di elezione). Se scade il tempo
// senza che nessun Coordinator sia stato annunciato, il nodo con priorita'
// maggiore che aveva risposto "step down" e' a sua volta caduto nel
// frattempo: si riprova daccapo.
func (lb *LoadBalancer) awaitCoordinatorOrRetry(done chan struct{}) {
	select {
	case <-done:
		// Stato aggiornato altrove (AnnounceCoordinator o becomeLeader): nulla da fare.
	case <-time.After(lb.Config.LB.CoordinatorWaitTimeout()):
		log.Printf("[%s] Nessun annuncio di coordinatore ricevuto in tempo: rieleggo.", lb.ID)
		lb.election.mu.Lock()
		lb.election.electionInProgress = false
		lb.election.mu.Unlock()
		lb.StartElection()
	}
}

// ---------------------------------------------------------------------
// Diventare leader / riconoscere un nuovo leader
// ---------------------------------------------------------------------

func (lb *LoadBalancer) becomeLeader() {
	lb.election.mu.Lock()
	lb.election.state = StateLeader
	lb.election.leaderID = lb.Priority
	lb.election.leaderAddr = lb.SelfGRPCAddress
	lb.election.electionInProgress = false
	lb.closeElectionDoneLocked()
	lb.stopWatchdogLocked() // il leader non sospetta se stesso
	lb.election.mu.Unlock()

	log.Printf("[%s] 👑 Sono il nuovo LEADER (priority=%d)", lb.ID, lb.Priority)

	for _, p := range lb.Peers {
		go func(peer *PeerNode) {
			// Riprova fino a 3 volte con un breve intervallo per dare tempo al peer di avviare gRPC
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

	// Solo il leader fa polling Scaphandre sui worker e propaga lo stato.
	lb.StartLeaderDuties()
}

// StartElection (RPC server-side): un altro nodo mi chiede di partecipare
// a un'elezione come candidato di priorita' inferiore/uguale alla mia
// (per costruzione, nel Bully riceve questa RPC solo chi ha priorita' maggiore
// del chiamante).
func (lb *LoadBalancer) HandleStartElection(ctx context.Context, req *pb.ElectionRequest) (*pb.ElectionResponse, error) {
	if req.GetCandidateId() < lb.Priority {
		// Rispondo "fermati, ci penso io". Avvio una MIA elezione solo se non
		// ho gia' un leader riconosciuto (vero bootstrap, leaderID ancora 0):
		// se sono gia' leader, o sono follower con un leader valido, la mia
		// elezione non aggiungerebbe nulla e sovraccaricherebbe il cluster di
		// round ridondanti — che e' esattamente cio' che ha causato la falsa
		// elezione osservata nel test (vedi PDF, Q4: "concurrent elections").
		lb.election.mu.RLock()
		hasKnownLeader := lb.election.state == StateLeader ||
			(lb.election.state == StateFollower && lb.election.leaderID != 0)
		lb.election.mu.RUnlock()
		if !hasKnownLeader {
			go lb.StartElection()
		}
		return &pb.ElectionResponse{StepDown: true}, nil
	}
	// Non dovrebbe accadere in un Bully "per manuale" (si notifica solo chi ha
	// priorita' maggiore), gestito comunque in modo difensivo.
	return &pb.ElectionResponse{StepDown: false}, nil
}

// AnnounceCoordinator (RPC server-side): un nodo annuncia di essere il nuovo leader.
func (lb *LoadBalancer) HandleAnnounceCoordinator(ctx context.Context, req *pb.CoordinatorAnnouncement) (*pb.Ack, error) {
	lb.election.mu.Lock()

	// Difesa in profondita', oltre al fix nel timeout di StartElection: se so
	// per certo di essere vivo e con priorita' maggiore di chi si annuncia
	// (sono Leader), un annuncio piu' debole non puo' essere corretto — al
	// piu' e' un residuo di una race gia' risolta altrove. Non e' ambiguo
	// come il caso "follower con leaderID magari stale": qui la certezza
	// (sono vivo, la mia priorita' e' nota) e' assoluta.

	// 1. Se sono leader con priorità maggiore, rifiuto
	if lb.election.state == StateLeader && req.GetLeaderId() < lb.Priority {
		lb.election.mu.Unlock()
		log.Printf("[%s] AnnounceCoordinator da leader_id=%d ignorato: sono gia' leader con priority=%d",
			lb.ID, req.GetLeaderId(), lb.Priority)
		return &pb.Ack{Success: false}, nil
	}

	// 2. Se sono follower e riconosco già un leader con priorità MAGGIORE di chi si annuncia, rifiuto!
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
		// Ero leader ma e' arrivato un annuncio da un nodo con priorita'
		// maggiore (es. rientrato dopo un crash): smetto di fare polling/broadcast.
		lb.StopLeaderDuties()
	}

	log.Printf("[%s] Nuovo leader riconosciuto: %s (priority=%d)", lb.ID, req.GetLeaderAddress(), req.GetLeaderId())
	return &pb.Ack{Success: true}, nil
}

// closeElectionDoneLocked chiude (una sola volta) il canale che sblocca chi e'
// in attesa dell'esito dell'elezione corrente. Va chiamata con election.mu gia' locked.
func (lb *LoadBalancer) closeElectionDoneLocked() {
	if lb.election.electionDone != nil {
		close(lb.election.electionDone)
		lb.election.electionDone = nil
	}
}

// ---------------------------------------------------------------------
// Watchdog: rilevamento del leader morto lato follower
// ---------------------------------------------------------------------

// resetWatchdog riarma il timer di sospetto-leader-morto. Va chiamata ogni
// volta che arriva un segnale di vita dal leader corrente (AnnounceCoordinator
// o SyncState, che qui funge anche da heartbeat periodico leader->follower).
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

// stopWatchdogLocked ferma il watchdog. Va chiamata con election.mu gia' locked.
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
