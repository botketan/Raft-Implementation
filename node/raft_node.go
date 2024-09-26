package node

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

/*
*
To record the state of nodes participating in raft
*/
type State int

const (
	Follower State = iota
	Candidate
	Leader
	Dead // The node is shutdown
)

const (
	electionTimeout  = time.Duration(300 * time.Millisecond)
	heartbeatTimeout = time.Duration(50 * time.Millisecond)
)

func (s State) Stringify() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("Invalid State")
	}
}

// Volatile State stored by leaders regarding other peers
type followerState struct {
	nextIndex  uint64
	matchIndex uint64
}

// This interface is to be implemented by `RaftNode`
type RaftInterface interface {
	// Restore the state incase of waking up from failure
	restoreStates() error
}

type Configuration struct {
	// Node ID -> address map
	members map[string]string
}

type RaftNode struct {
	// For concurrency
	mu sync.Mutex
	wg sync.WaitGroup

	// Condition variables
	applyCond    *sync.Cond
	commitCond   *sync.Cond
	electionCond *sync.Cond

	// Persistent states to be stored
	id          string
	currentTerm uint64 // Default value is 0
	address     string
	votedFor    string // Default value is "None"
	config      *Configuration

	// Volatile states
	commitIndex   uint64
	lastApplied   uint64
	leaderId      string
	state         State
	lastContact   time.Time                 // To store the last time some leader has contacted - used for handling timeouts
	followersList map[string]*followerState // To map address to other follower state

	node *Node
	// TODO: Log struct
	// log Log
}

func InitRaftNode(id in, address string) (*RaftNode, error) {
	node, err := InitNode(address)
	if err != nil {
		return nil, err
	}

	raft := &RaftNode{
		id:            id,
		node:          node,
		currentTerm:   0,
		state:         Follower,
		followersList: make(map[string]*followerState),
	}

	raft.applyCond = sync.NewCond(&raft.mu)
	raft.commitCond = sync.NewCond(&raft.mu)
	raft.electionCond = sync.NewCond(&raft.mu)

	err = raft.restoreStates()
	if err != nil {
		return nil, err
	}

	return raft, nil
}

func (r *RaftNode) restoreStates() error {
	return fmt.Errorf("not yet implemented!!")
}

// Gets a random timeout between [min, max]
func RandomTimeout(min time.Duration, max time.Duration) time.Duration {
	n := rand.Int63n(max.Milliseconds()-min.Milliseconds()) + min.Milliseconds()
	return time.Duration(n)
}

// This handles the electionTimeout
func (r *RaftNode) electionClock() {
	defer r.wg.Done()

	for {
		// timeout is random time between the given ranges
		timeout := RandomTimeout(electionTimeout, 2*electionTimeout)
		time.Sleep(timeout * time.Millisecond)

		r.mu.Lock()
		if r.state == Dead {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()

		r.electionCond.Broadcast()
	}
}

// electionLoop is a long running loop that will attempt to start an
// election when electionCond is broadcasted
func (r *RaftNode) electionLoop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.wg.Done()

	for r.state != Dead {
		// This will temporarily release the mutex and when it wakes up, locks the mutex again
		r.electionCond.Wait()
		r.startElection()
	}
}

// Starts the election , this should be called under thread safe conditions
// Currently it is called after waiting on a condition, so its thread safe!
func (r* RaftNode) startElection()
{
	if r.state == Leader || r.state == Dead || time.Since(r.lastContact) < electionTimeout {
		log.Println("election timed out but not started")
		return 
	}

	if r.state == Follower || r.state == Candidate {
		// Becomes a candidate
		r.state = Candidate
		r.currentTerm++
		r.votedFor = r.id
		// TODO Write the currentTerm and votedFor in the mongodb, maybe create a function for it
		log.Println("node %w transitioned to candidate state")
	}

	for id, addr := range r.config.members {
		if id != r.id {
			go r.SendRequestVoteRPC(addr)
		}
	}
	
}

// Send the Request Vote RPC to an address
func (r* RaftNode) SendRequestVoteRPC(addr string)
{
	return
}

// To be done after calling InitServer
func (r *RaftNode) start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.restoreStates(); err != nil {
		return err
	}

	//TODO Register the RPCs

	// Initalise the followers list
	for id := range r.config.members {
		r.followersList[id] = new(followerState)
	}
	r.state = Follower
	r.lastContact = time.Now()

	// TODO add the wgs and start the loops

	if err := r.node.Start(); err != nil {
		return err
	}
	log.Println("Server with ID: %w is started", r.id)

	return nil
}
