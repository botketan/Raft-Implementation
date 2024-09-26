package node

import (
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

func InitRaftNode(ID string, address string) (*RaftNode, error) {
	node, err := InitNode(address)
	if err != nil {
		return nil, err
	}

	raft := &RaftNode{
		id:          id,
		node:        node,
		currentTerm: 0,
		state:       Follower,
	}

	return raft, nil
}
