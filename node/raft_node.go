package node

import (
	"fmt"
	"log"
	"math/rand"
	mongodb "raft/mongoDb"
	pb "raft/protos"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
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

func (s State) String() string {
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
	nextIndex  int64
	matchIndex int64
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

type LogEntry struct {
	Index int64
	Term  int64
	Data  []byte
}

type Log struct {
	entries []LogEntry
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
	currentTerm int64 // Default value is 0
	address     string
	votedFor    string // Default value is ""
	config      *Configuration

	// Volatile states
	commitIndex   int64
	lastApplied   int64
	leaderId      string
	state         State
	lastContact   time.Time                 // To store the last time some leader has contacted - used for handling timeouts
	followersList map[string]*followerState // To map id to other follower state

	node *Node
	log  Log

	//MongoDB Connection
	mongoClient *mongo.Client
}

func InitRaftNode(ID string, address string) (*RaftNode, error) {
	node, err := InitNode(address)
	if err != nil {
		return nil, err
	}

	raft := &RaftNode{
		id:            ID,
		node:          node,
		currentTerm:   0,
		state:         Follower,
		followersList: make(map[string]*followerState),
		commitIndex:   -1,
		votedFor:      "",
	}

	raft.applyCond = sync.NewCond(&raft.mu)
	raft.commitCond = sync.NewCond(&raft.mu)
	raft.electionCond = sync.NewCond(&raft.mu)

	//MongoDb Connection
	raft.mongoClient, err = mongodb.Connect()

	if err != nil {
		return nil, err
	}

	err = raft.restoreStates()
	if err != nil {
		return nil, err
	}

	return raft, nil
}

func (r *RaftNode) restoreStates() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	client, err := mongodb.Connect()
	if err != nil {
		return err
	}
	r.mongoClient = client
	NodeLog, err := mongodb.GetNodeLog(*r.mongoClient, r.id)
	if err != nil {
		return err
	}
	r.currentTerm = NodeLog.CurrentTerm
	r.address = NodeLog.Address
	r.votedFor = NodeLog.VotedFor
	r.config.members = NodeLog.Config.Members
	return nil
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
func (r *RaftNode) startElection() {
	if r.state == Leader || r.state == Dead || time.Since(r.lastContact) < electionTimeout {
		log.Println("election timed out but not started")
		return
	}

	if r.state == Follower || r.state == Candidate {
		r.becomeCandidate()
	}

	votesReceived := 1
	for id, addr := range r.config.members {
		if id != r.id {
			go r.sendRequestVote(id, addr, &votesReceived)
		}
	}

}

// Send the Request Vote RPC to an node (id, address) and process it
func (r *RaftNode) sendRequestVote(id string, addr string, votesRcd *int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Fetch last log entry details
	lastLogIndex := int64(-1)
	lastLogTerm := int64(0)
	if len(r.log.entries) > 0 {
		lastLogIndex = r.log.entries[len(r.log.entries)-1].Index
		lastLogTerm = r.log.entries[len(r.log.entries)-1].Term
	}

	req := &pb.RequestVoteRequest{
		CandidateId:  r.id,
		Term:         int64(r.currentTerm),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	r.mu.Unlock()
	resp, err := r.node.SendRequestVoteRPC(addr, req)
	r.mu.Lock()

	// Send only when you are a candidate and alive
	if err != nil || r.state == Dead || r.state != Candidate {
		return
	}

	// Handle the case when the node has already started election in some other thread
	if r.currentTerm > int64(req.GetTerm()) {
		return
	}

	if resp.GetVoteGranted() {
		*votesRcd++
	}

	// The peer has a higher term - switch to follower
	if resp.GetTerm() > req.GetTerm() {
		r.becomeFollower(id, int64(resp.GetTerm()))
		return
	}

	if r.state == Candidate && r.hasMajority(*votesRcd) {
		r.becomeLeader()
	}
}

// Handler to send RequestVoteResponse - to be registered
func (r *RaftNode) RequestVoteHandler(req *pb.RequestVoteRequest, resp *pb.RequestVoteResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Dead {
		return fmt.Errorf("node is in dead state, can't reply to RequestVote")
	}

	log.Println("Received RequestVote RPC from candiate: %w", req.GetCandidateId())

	resp.Term = int64(r.currentTerm)
	resp.VoteGranted = false

	// If some disruptive servers keep sending requests and we keep getting RPCs but election hasn't been timedout yet, ignore the RPCS
	if time.Since(r.lastContact) < electionTimeout {
		log.Println("rejecting RequestVote RPC: node has a leader %w already known", r.leaderId)
		return nil
	}

	// Reject outdated terms
	if req.GetTerm() < int64(r.currentTerm) {
		log.Println("rejecting RequestVote RPC: node's term is %w, req's term is %w", r.currentTerm, req.GetTerm())
		return nil
	}

	// become follower if my term is outdated
	if req.GetTerm() > int64(r.currentTerm) {
		r.becomeFollower(req.GetCandidateId(), int64(req.GetTerm()))
		resp.Term = req.GetTerm()
	}

	// Reject if I have already voted to someone else
	if r.votedFor != "" && r.votedFor != req.GetCandidateId() {
		log.Println("rejecting RequestVote RPC: already voted for %w", r.votedFor)
		return nil
	}

	// TODO: Check and complete log Condition for granting vote

	resp.VoteGranted = true
	r.lastContact = time.Now()
	r.votedFor = req.GetCandidateId()

	r.saveStateToDB()

	log.Println(
		"requestVote RPC successful: votedFor = %w, term = %w",
		req.GetCandidateId(),
		r.currentTerm,
	)

	return nil
}

// Send append entries to a node (id, address) and process it
func (r *RaftNode) sendAppendEntries(id string, addr string, respRcd *int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Dont send if r is not the leader
	if r.state != Leader {
		return
	}

	peer := r.followersList[id]
	nextIndex := peer.nextIndex
	prevLogIndex := nextIndex - 1
	var prevLogTerm int64 = -1
	if prevLogIndex >= 0 {
		prevLogTerm = r.log.entries[prevLogIndex].Term
	}
	entries := r.log.entries[nextIndex:]

	req := &pb.AppendEntriesRequest{
		LeaderId:     r.id,
		Term:         int64(r.currentTerm),
		LeaderCommit: r.commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      convertToProtoEntries(entries),
	}

	r.mu.Unlock()
	resp, err := r.node.SendAppendEntriesRPC(addr, req)
	r.mu.Lock()

	if r.state != Leader || err != nil {
		return
	}

	// Become a follower if the reply term is greater
	if resp.GetTerm() > r.currentTerm {
		r.becomeFollower(id, resp.GetTerm())
		return
	}

	*respRcd++

	// TODO: Complete it

}

// Appends new log entries to the log on receiving a request from the leader
func (r *RaftNode) AppendEntriesHandler(req *pb.AppendEntriesRequest, resp *pb.AppendEntriesResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If the leader's term is less than our current term, reject the request
	if req.Term < r.currentTerm {
		resp.Term = r.currentTerm
		resp.Success = false
		return nil
	}

	// If the leader's term is greater, we step down as a follower
	if req.Term > r.currentTerm {
		r.becomeFollower(req.LeaderId, req.Term)
	}

	// Reset the timeout since we've received a valid append from the leader
	r.lastContact = time.Now()

	// Check if we have the previous log entry at PrevLogIndex and PrevLogTerm
	if req.PrevLogIndex > 0 {
		if len(r.log.entries) == 0 || req.PrevLogIndex > int64(len(r.log.entries)) || r.log.entries[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			// Log inconsistency detected, reject the append
			resp.Term = r.currentTerm
			resp.Success = false
			return nil
		}
	}

	// Append new entries to the log if any
	for _, entry := range req.Entries {
		// If there is already an entry at this index, replace it (log overwrite protection)
		if entry.Index <= int64(len(r.log.entries)) {
			r.log.entries[entry.Index-1] = LogEntry{
				Index: entry.Index,
				Term:  entry.Term,
				Data:  entry.Data,
			}
		} else {
			// Append new log entries
			r.log.entries = append(r.log.entries, LogEntry{
				Index: entry.Index,
				Term:  entry.Term,
				Data:  entry.Data,
			})
		}
	}

	// Update commit index if leaderCommit is greater than our commitIndex
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = min(req.LeaderCommit, int64(len(r.log.entries)))
		r.commitCond.Broadcast()
	}

	resp.Term = r.currentTerm
	resp.Success = true
	return nil
}

func (r *RaftNode) becomeCandidate() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	r.saveStateToDB()
	log.Println("node %w transitioned to candidate state", r.id)
}

func (r *RaftNode) becomeFollower(leaderID string, term int64) {
	r.state = Follower
	r.leaderId = leaderID
	r.votedFor = ""
	r.currentTerm = term
	r.saveStateToDB()
	log.Println("node %w transitioned to follower state", r.id)
}

func (r *RaftNode) becomeLeader() {
	r.leaderId = r.id
	for _, follower := range r.followersList {
		follower.nextIndex = int64(len(r.log.entries))
		follower.matchIndex = -1
	}

	responsesRcd := 1
	for id, addr := range r.config.members {
		if id != r.id {
			go r.sendAppendEntries(id, addr, &responsesRcd)
		}
	}
	log.Println("node %w transitioned to leader state", r.id)
}

// returns true if majority has been reached for the input number of votes
// should be called inside a thread safe func
func (r *RaftNode) hasMajority(count int) bool {
	return count > len(r.config.members)
}

// To be done after calling InitServer
func (r *RaftNode) start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.restoreStates(); err != nil {
		return err
	}

	r.node.registerRequestVoteHandler(r.RequestVoteHandler)
	r.node.registerAppendEntriesHandler(r.AppendEntriesHandler)

	// Initalise the followers list
	for id := range r.config.members {
		r.followersList[id] = new(followerState)
	}
	r.state = Follower
	r.lastContact = time.Now()

	// TODO add the remaining loops
	r.wg.Add(2)
	go r.electionClock()
	go r.electionLoop()

	if err := r.node.Start(); err != nil {
		return err
	}
	log.Println("Server with ID: %w is started", r.id)

	return nil
}

// saveStateToDB saves the current state of the node to the database
func (r *RaftNode) saveStateToDB() error {
	err := mongodb.Voted(*r.mongoClient, r.id, r.votedFor, r.currentTerm)
	if err != nil {
		return fmt.Errorf("error while saving state to database: %w", err)
	}
	return nil
}

// convertToProtoEntries converts log entries to protobuf format for AppendEntriesRequest
func convertToProtoEntries(entries []LogEntry) []*pb.LogEntry {
	var protoEntries []*pb.LogEntry
	for _, entry := range entries {
		protoEntries = append(protoEntries, &pb.LogEntry{
			Index: entry.Index,
			Term:  entry.Term,
			Data:  entry.Data,
		})
	}
	return protoEntries
}

// min is a utility function to get the minimum of two int64 values
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
