package node

import (
	"fmt"
	"math/rand"
	fsm "raft/fsm"
	mongodb "raft/mongoDb"
	pb "raft/protos"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/protobuf/proto"
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
	Client
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
	case Client:
		return "Client"
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

type LogEntryType int32

const (
	NORMAL_OP LogEntryType = 0
	CONFIG_OP LogEntryType = 1
)

func (s LogEntryType) String() string {
	switch s {
	case NORMAL_OP:
		return "NORMAL_OP"
	case CONFIG_OP:
		return "CONFIG_OP"
	default:
		panic("Invalid Operation")
	}
}

type LogEntry struct {
	Index     int64
	Term      int64
	Data      []byte
	seqNo     int64
	clientID  string
	entryType LogEntryType
}

func (e LogEntry) String() string {
	return fmt.Sprintf("Index: %d, Term: %d, Data: %s, SeqNo: %d, clientID: %s, operationType: %s", e.Index, e.Term, string(e.Data), e.seqNo, e.clientID, e.entryType)
}

type Log struct {
	entries []LogEntry
}

func (l Log) String() string {
	var result string
	for _, entry := range l.entries {
		result += entry.String() + "\n"
	}
	return result
}

type RaftNode struct {
	// For concurrency
	mu sync.Mutex
	wg sync.WaitGroup

	// Condition variables
	applyCond     *sync.Cond
	commitCond    *sync.Cond
	electionCond  *sync.Cond
	heartbeatCond *sync.Cond

	// Persistent states to be stored
	id          string
	currentTerm int64 // Default value is 0
	address     string
	votedFor    string // Default value is ""

	// Volatile states
	commitIndex      int64
	lastApplied      int64
	leaderId         string
	config           *pb.Configuration
	commitedConfig   *pb.Configuration
	state            State
	lastContact      time.Time                 // To store the last time some leader has contacted - used for handling timeouts
	followersList    map[string]*followerState // To map id to other follower state
	operationManager *operationManager         // Operation Manager to handle operations
	configManager    *configManager            // Configuration Manager to handle configuration changes
	fsm              fsm.FSM                   // State Machine to apply operations

	node *Node
	log  *Log

	//MongoDB Connection
	mongoClient *mongo.Client

	// logger
	logger Logger
}

func InitRaftNode(ID string, address string, config *pb.Configuration, fsm fsm.FSM) (*RaftNode, error) {
	node, err := InitNode(address)
	if err != nil {
		return nil, err
	}

	// Initialize the RaftNode with default values
	raft := &RaftNode{
		id:               ID,
		node:             node,
		currentTerm:      0,
		state:            Follower,
		followersList:    make(map[string]*followerState),
		commitIndex:      -1,
		lastApplied:      -1,
		config:           config,
		votedFor:         "",
		log:              &Log{}, // Initialize log to prevent nil pointer dereference
		operationManager: newOperationManager(),
		configManager:    newConfigManager(),
		fsm:              fsm, // FSM to get the logs
	}

	raft.applyCond = sync.NewCond(&raft.mu)
	raft.commitCond = sync.NewCond(&raft.mu)
	raft.electionCond = sync.NewCond(&raft.mu)
	raft.heartbeatCond = sync.NewCond(&raft.mu)

	//MongoDb Connection
	raft.mongoClient, err = mongodb.Connect()

	if err != nil {
		return nil, err
	}

	// Initialise logger
	raft.logger, err = NewLogger(raft.id, &raft.state)
	if err != nil {
		return nil, err
	}

	raft.restoreStates()
	return raft, nil
}

func (r *RaftNode) restoreStates() error {

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

	r.log.entries = []LogEntry{}
	for _, entry := range NodeLog.LogEntries {
		r.log.entries = append(r.log.entries, LogEntry{
			Index:     entry.Index,
			Term:      entry.Term,
			Data:      entry.Data,
			seqNo:     int64(entry.SeqNo),
			clientID:  entry.ClientID,
			entryType: LogEntryType(entry.EntryType),
		})
	}

	// A second config entry is processed only after first one gets committed, so
	// if there is only one config entry in log, it maynot have been committed
	// If there were 2 or more config entries, last one is current config and penultimate one is committed config
	var penultimateConfig = &pb.Configuration{}
	var lastConfig = &pb.Configuration{}
	for _, entry := range r.log.entries {
		if entry.entryType == CONFIG_OP {
			penultimateConfig = lastConfig
			err := proto.Unmarshal(entry.Data, lastConfig)
			if err != nil {
				r.logger.Log("Error while un-marshalling: %v", err.Error())
				continue
			}
		}
	}

	if !proto.Equal(lastConfig, &pb.Configuration{}) {
		r.logger.Log("Last configuration restored from log: %v", lastConfig)
		r.config = lastConfig
	}

	if !proto.Equal(penultimateConfig, &pb.Configuration{}) {
		r.logger.Log("Last committed configuration restored from log: %v", penultimateConfig)
		r.commitedConfig = penultimateConfig
	}

	r.logger.Log("state restored, currentTerm = %d, votedFor = %s, log = %v", r.currentTerm, r.votedFor, r.log.entries)
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

// Sends Append Entries RPC to the followers when node is a leader
func (r *RaftNode) heartbeatLoop() {
	defer r.wg.Done()

	for {
		time.Sleep(heartbeatTimeout)
		r.mu.Lock()
		if r.state != Leader {
			r.mu.Unlock()
			continue
		}
		if r.state == Leader {
			r.logger.Log("heartbeat Timeout, sending AE")
			responsesRcd := 0
			for id, addr := range r.config.Members {
				if id != r.id {
					go r.sendAppendEntries(id, addr, &responsesRcd)
				}
			}
			r.mu.Unlock()
		}
	}
}

// Commit Loop to commit new entries
func (r *RaftNode) commitLoop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.wg.Done()

	for r.state != Dead {
		r.commitCond.Wait()
		r.commitEntries()
	}
}

// Commit Function to commit the log entries
func (r *RaftNode) commitEntries() {
	//Followers Can Skip
	if r.state != Leader {
		return
	}

	//Assuming No commit has happened
	anyCommit := false

	for i := r.commitIndex + 1; i < int64(len(r.log.entries)); i++ {

		// Dont commit if the term is not the current term
		if r.log.entries[i].Term != r.currentTerm {
			continue
		}

		count := 1
		for id, follower := range r.followersList {
			//Skip the leader
			if id == r.id {
				continue
			}
			if follower.matchIndex >= i {
				count++
			}
		}

		//If majority has been reached, commit the entry
		if r.hasMajority(count) {
			r.commitIndex = i
			r.logger.Log("CommitIndex updated to %d", i)
			anyCommit = true
		}
	}

	if anyCommit {
		r.logger.Log("CommitIndex was advanced, wakening up the apply loop")

		r.applyCond.Broadcast()
		for id, addr := range r.config.Members {
			var temp int
			if id != r.id {
				go r.sendAppendEntries(id, addr, &temp)
			}
		}
	}
}

func (r *RaftNode) applyLoop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.wg.Done()

	for r.state != Dead {
		r.applyCond.Wait()
		r.logger.Log("Apply Loop Wake Up")
		r.applyEntries()
	}
}

func (r *RaftNode) applyEntries() {
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		entry := r.log.entries[r.lastApplied]
		r.logger.Log("Applying entry: %s", entry)

		switch entry.entryType {
		case CONFIG_OP:
			configuration, err := decodeConfiguration(entry.Data)
			if err != nil {
				r.logger.Log("Error while decoding configuration: %v", err.Error())
				continue
			}
			// If the configuration is already committed, skip
			if r.commitedConfig != nil && configuration.Index <= r.commitedConfig.LogIndex {
				return
			}
			// Transition to new configuration
			r.nextConfiguration(&configuration)

			r.commitedConfig = configuration.toProto()
			respond(r.configManager.pendingReplicated[entry.Index], protoToConfiguration(r.config), nil)

		case NORMAL_OP:
			responseCh := r.operationManager.pendingReplicated[entry.Index]
			delete(r.operationManager.pendingReplicated, entry.Index)
			operation := Operation{
				LogIndex: entry.Index,
				LogTerm:  entry.Term,
				Bytes:    entry.Data,
			}

			clientReq := &fsm.ClientOperationRequest{
				Operation: operation.Bytes,
				SeqNo:     entry.seqNo,
			}
			response := OperationResponse{
				Operation:           operation,
				ApplicationResponse: r.fsm.Apply(clientReq),
			}
			r.logger.Log("Reached here :%s", response)
			select {
			case responseCh <- &result[OperationResponse]{success: response, err: nil}:
			default:
			}
			r.logger.Log("Applied entry: %s", entry)
		}
	}
}

// appendConfiguration sets the log index associated with the
// configuration and appends it to the log. to be used inside a threadsafe function
func (r *RaftNode) appendConfiguration(configuration *Configuration) {
	data, err := encodeConfiguration(configuration)
	if err != nil {
		r.logger.Log("failed to encode configuration: error = %v", err)
	}
	entry := LogEntry{Index: configuration.Index, Term: r.currentTerm, Data: data, seqNo: 0, clientID: "", entryType: CONFIG_OP}
	r.log.entries = append(r.log.entries, entry)
	error := mongodb.AddLog(*r.mongoClient, r.id, entry.Term, entry.Index, entry.Data, entry.seqNo, entry.clientID, mongodb.LogEntryType(entry.entryType))
	if error != nil {
		r.logger.Log("failed to add configuration to log: error = %v", error)
	}
	configuration.Index = r.log.entries[len(r.log.entries)-1].Index + 1
}

// nextConfiguration transitions this node from its current configuration to
// to the provided configuration.
func (r *RaftNode) nextConfiguration(next *Configuration) {
	r.logger.Log("transitioning to new configuration: configuration = %s", next.String())

	defer func() {
		r.config = next.toProto()
	}()

	// Step down if this node is being removed and it is the leader.
	if _, ok := next.Members[r.id]; !ok {
		if r.state == Leader {
			r.stepdown()
		}
	}

	// Delete removed nodes from followers.
	for id := range r.config.Members {
		if _, ok := next.Members[id]; !ok {
			delete(r.followersList, id)
		}
	}

	// Create entry for added nodes.
	for id := range next.Members {
		if _, ok := r.config.Members[id]; !ok {
			r.followersList[id] = &followerState{nextIndex: 0, matchIndex: -1}
		}
	}
}

// stepDown transitions a node from the leader state to the follower state when it
// has been removed from the cluster. Unlike becomeFollower, stepDown does not persist
// the current term and vote.
func (r *RaftNode) stepdown() {
	r.state = Follower

	// Cancel any pending operations.
	r.operationManager.notifyLostLeaderShip()
	r.operationManager = newOperationManager()

	r.logger.Log("stepped down to the follower state")
}

// Add server RPC handler
func (r *RaftNode) AddServerHandler(req *pb.AddServerRequest, resp *pb.AddServerResponse) error {
	r.logger.Log("Received Add Server Request: %v", req.String())

	// Only the leader can make membership changes.
	if r.state != Leader {
		resp.Status = "NOT_LEADER"
		resp.LeaderHint = r.leaderId
		return nil
	}

	future := r.AddServer(req.GetNodeId(), req.GetAddress(), false, 500*time.Millisecond)
	configuration := future.Await()

	if configuration.Error() != nil {
		resp.Status = configuration.Error().Error()
		resp.LeaderHint = r.leaderId
		return nil
	}

	resp.Status = "OK"
	resp.LeaderHint = r.leaderId
	return nil
}

// Remove server RPC handler
func (r *RaftNode) RemoveServerHandler(req *pb.RemoveServerRequest, resp *pb.RemoveServerResponse) error {
	return fmt.Errorf("not implemented")
}

// Add a new server to the configuration
func (r *RaftNode) AddServer(
	id string,
	address string,
	isVoter bool,
	timeout time.Duration,
) Future[Configuration] {
	r.mu.Lock()
	defer r.mu.Unlock()

	configurationFuture := newFuture[Configuration](timeout)
	config := protoToConfiguration(r.config)

	// Membership changes may not be submitted until a log entry for this term is committed.
	if !r.committedThisTerm() {
		respond(configurationFuture.responseCh, Configuration{}, fmt.Errorf("no committed log entry for this term"))
		return configurationFuture
	}

	// The membership change is still pending - wait until it completes.
	if r.pendingConfigurationChange() {
		respond(configurationFuture.responseCh, Configuration{}, fmt.Errorf("pending configuration change"))
		return configurationFuture
	}

	// The provided node is already a part of the cluster.
	if r.isMember(id) && r.isVoter(id) == isVoter {
		respond(configurationFuture.responseCh, config, nil)
		return configurationFuture
	}

	// Create the configuration that includes the new node as a non-voter.
	newConfiguration := config.Clone()
	newConfiguration.Members[id] = address
	newConfiguration.IsVoter[id] = isVoter

	// Add the configuration to the log.
	r.appendConfiguration(&newConfiguration)

	r.config = newConfiguration.toProto()
	r.followersList[id] = &followerState{nextIndex: 0, matchIndex: -1}

	r.configManager.pendingReplicated[newConfiguration.Index] = configurationFuture.responseCh

	// Send AppendEntries RPCs to all nodes to replicate the configuration change.
	responsesRcd := 1
	for id, addr := range r.config.Members {
		if id != r.id {
			go r.sendAppendEntries(id, addr, &responsesRcd)
		}
	}

	r.logger.Log(
		"request to add node submitted: id = %s, address = %s, voter = %t, logIndex = %d",
		id,
		address,
		isVoter,
		newConfiguration.Index,
	)

	return configurationFuture
}

// Submit operation RPC handler
func (r *RaftNode) SubmitOperationHandler(req *pb.SubmitOperationRequest, resp *pb.SubmitOperationResponse) error {

	r.logger.Log("Received Submit Operation from Client : %v", req.String())

	if r.state != Leader {
		resp.Success = false
		if r.leaderId != "" {
			resp.Message = "REDIRECT " + r.leaderId
		} else {
			resp.Message = "Not a Leader, and Leader Unknown"
		}
		return nil
	}

	clientReq := &fsm.ClientOperationRequest{
		Operation: req.Operation,
		SeqNo:     req.SeqNo,
		ClientID:  req.ClientId,
	}

	operationFuture := r.SubmitOperation(clientReq, 500*time.Millisecond)
	// Wait for the operation to be replicated
	operationResult := operationFuture.Await()
	if operationResult.Error() != nil {
		resp.Success = false
		resp.Message = operationResult.Error().Error()
		return nil
	}

	resp.Success = true
	resp.Message = operationResult.Success().String()

	return nil
}

func (r *RaftNode) SubmitOperation(clientReq *fsm.ClientOperationRequest, timeout time.Duration) Future[OperationResponse] {
	r.mu.Lock()
	defer r.mu.Unlock()

	operationFuture := newFuture[OperationResponse](timeout)
	operationBytes := clientReq.Operation

	r.logger.Log("operation submitted: %s, seqNo: %s", string(operationBytes), clientReq.SeqNo)

	if r.state != Leader {
		operationFuture.responseCh <- &result[OperationResponse]{err: fmt.Errorf("not a leader")}
		return operationFuture
	}

	entry := LogEntry{
		Index:     int64(len(r.log.entries)),
		Term:      r.currentTerm,
		Data:      operationBytes,
		seqNo:     clientReq.SeqNo,
		clientID:  clientReq.ClientID,
		entryType: NORMAL_OP,
	}

	r.log.entries = append(r.log.entries, entry)

	mongodb.AddLog(*r.mongoClient, r.id, entry.Term, entry.Index, entry.Data, entry.seqNo, entry.clientID, mongodb.LogEntryType(entry.entryType))

	r.operationManager.pendingReplicated[entry.Index] = operationFuture.responseCh

	numResponses := 1
	for id, address := range r.config.Members {
		if id != r.id {
			go r.sendAppendEntries(id, address, &numResponses)
		}
	}

	r.logger.Log(
		"operation submitted at logIndex = %d, logTerm = %d, seqNo = %d",
		entry.Index,
		entry.Term,
		entry.seqNo,
	)

	return operationFuture
}

// Starts the election , this should be called under thread safe conditions
// Currently it is called after waiting on a condition, so its thread safe!
func (r *RaftNode) startElection() {
	if r.state == Leader || r.state == Dead || time.Since(r.lastContact) < electionTimeout {
		return
	}

	if r.state == Follower || r.state == Candidate {
		r.logger.Log("election timeout")
		r.becomeCandidate()
	}

	votesReceived := 1
	for id, addr := range r.config.Members {
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

	r.logger.Log("sent RequestVote RPC to Node %s with Term: %d, LastLogIndex: %d, LastLogterm: %d", id, req.GetTerm(), req.GetLastLogIndex(), req.GetLastLogTerm())
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

	r.logger.Log("received Votes: %d", *votesRcd)
	if r.state == Candidate && r.hasMajority(*votesRcd) {
		r.becomeLeader()
	}
}

// Handler to send RequestVoteResponse - to be registered
func (r *RaftNode) RequestVoteHandler(req *pb.RequestVoteRequest, resp *pb.RequestVoteResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Dead {
		return fmt.Errorf("node is in dead state, can't reply to RequestVote RPC")
	}

	r.logger.Log("Received RequestVote RPC from candiate: %s", req.GetCandidateId())

	resp.Term = int64(r.currentTerm)
	resp.VoteGranted = false

	// If some disruptive servers keep sending requests and we keep getting RPCs but election hasn't been timedout yet, ignore the RPCS
	if time.Since(r.lastContact) < electionTimeout {
		r.logger.Log("rejecting RequestVote RPC: node has a leader %s already known", r.leaderId)
		return nil
	}

	// Reject outdated terms
	if req.GetTerm() < int64(r.currentTerm) {
		r.logger.Log("rejecting RequestVote RPC: node's term is %d, req's term is %d", r.currentTerm, req.GetTerm())
		return nil
	}

	// become follower if my term is outdated
	if req.GetTerm() > int64(r.currentTerm) {
		r.becomeFollower(req.GetCandidateId(), int64(req.GetTerm()))
		resp.Term = req.GetTerm()
	}

	// Reject if I have already voted to someone else
	if r.votedFor != "" && r.votedFor != req.GetCandidateId() {
		r.logger.Log("rejecting RequestVote RPC: already voted for %s", r.votedFor)
		return nil
	}

	sz := len(r.log.entries)
	if sz > 0 {
		if r.log.entries[sz-1].Term > req.LastLogTerm || (r.log.entries[sz-1].Term == req.LastLogTerm && r.log.entries[sz-1].Index > req.LastLogIndex) {
			r.logger.Log("rejecting RequestVote RPC: current log : (Term = %d, Index = %d) is more updated than the candidate's log : (Term = %d, Index = %d)", r.log.entries[sz-1].Term, r.log.entries[sz-1].Index, req.GetLastLogTerm(), req.GetLastLogIndex())
			return nil

		}
	}

	// Grant vote
	resp.VoteGranted = true
	r.lastContact = time.Now()
	r.votedFor = req.GetCandidateId()

	r.saveStateToDB()

	r.logger.Log(
		"requestVote RPC successful: votedFor = %s, CurrentTerm = %d",
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
	var prevLogTerm int64 = 0
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
	r.logger.Log("sent AppendEntries RPC to Node %s with Term: %d, LeaderCommit: %d, PrevLogIndex: %d, PrevLogTerm: %d",
		id, req.Term, req.LeaderCommit, req.PrevLogIndex, req.PrevLogTerm)

	r.mu.Unlock()
	resp, err := r.node.SendAppendEntriesRPC(addr, req)
	r.mu.Lock()

	r.logger.Log("received AppendEntries Response from Node %s with Term: %d, ConflictIndex: %d, ConflictTerm: %d, Success: %v", id, resp.GetTerm(), resp.GetConflictIndex(), resp.GetConflictTerm(), resp.GetSuccess())

	if r.state != Leader || err != nil {
		return
	}

	// Become a follower if the reply term is greater
	if resp.GetTerm() > r.currentTerm {
		r.becomeFollower(id, resp.GetTerm())
		return
	}

	*respRcd++

	// At this point r.state is definitely a leader
	if resp.GetTerm() != req.GetTerm() {
		return
	}

	r.logger.Log("before updating, Node %s's nextIndex: %d, matchIndex: %d", id, peer.nextIndex, peer.matchIndex)

	// There is a conflict in log
	if !resp.GetSuccess() {
		if resp.GetConflictTerm() == 0 {
			// This means prevLogIndex is too high, so just update the nextIndex with ConflictIndex
			// and try again in next append entries loop!
			peer.nextIndex = resp.GetConflictIndex()
		} else {
			// This means we have to skip through all the term entries as in resp.ConflictTerm
			// Find the first Index of term after the Conflict Term and set it as new nextIndex
			var lastIndexOfConflictTerm int64 = -1
			for idx := len(r.log.entries) - 1; idx >= 0; idx-- {
				if r.log.entries[idx].Term == resp.GetConflictTerm() {
					lastIndexOfConflictTerm = int64(idx)
					break
				}
			}
			if lastIndexOfConflictTerm != -1 {
				// If there exists a term mentioned by conflict term in this log
				peer.nextIndex = lastIndexOfConflictTerm + 1
			} else {
				peer.nextIndex = min(peer.nextIndex-1, resp.GetConflictIndex())
			}
		}

		r.logger.Log("AE fail, Updated Node %s 's nextIndex: %d, matchIndex: %d", id, peer.nextIndex, peer.matchIndex)
		return
	}

	// If its successful response
	peer.nextIndex = nextIndex + int64(len(entries))
	peer.matchIndex = peer.nextIndex - 1

	r.logger.Log("AE success, Updated Node %s 's nextIndex: %d, matchIndex: %d", id, peer.nextIndex, peer.matchIndex)

	// Entries beyond the already committed entries are replicated, so there can be a possibility of commiting
	if peer.matchIndex > r.commitIndex {
		r.logger.Log("Waking up commitCond Loop, commitIndex: %d, curr MatchIndex: %d", r.commitIndex, peer.matchIndex)
		r.commitCond.Broadcast()
	}
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

	r.leaderId = req.GetLeaderId()

	// If the leader's term is greater, we step down as a follower
	if req.Term > r.currentTerm {
		r.becomeFollower(req.LeaderId, req.Term)
	}

	// Reset the timeout since we've received a valid append from the leader
	r.lastContact = time.Now()

	// If the node is still in candidate state, become a follower
	if req.GetTerm() == r.currentTerm && r.state == Candidate {
		r.becomeFollower(req.GetLeaderId(), req.GetTerm())
	}

	// Check if we have the previous log entry at PrevLogIndex and PrevLogTerm
	if req.PrevLogIndex >= 0 {
		if len(r.log.entries) == 0 || req.PrevLogIndex >= int64(len(r.log.entries)) || r.log.entries[req.PrevLogIndex].Term != req.PrevLogTerm {
			// Log inconsistency detected, reject the append
			resp.Term = r.currentTerm
			if req.PrevLogIndex >= int64(len(r.log.entries)) {
				resp.ConflictTerm = 0
				resp.ConflictIndex = int64(len(r.log.entries))
			} else {
				resp.ConflictTerm = r.log.entries[req.PrevLogIndex].Term
				// Find the first index of the conflicting term in the log
				firstInd := int64(-1)
				for i := req.PrevLogIndex; i >= 0; i-- {
					if r.log.entries[i].Term != resp.ConflictTerm {
						firstInd = i + 1
						break
					}
				}
				if firstInd == -1 {
					firstInd = 0
				}
				resp.ConflictIndex = firstInd
			}
			resp.Success = false
			return nil
		}
	}

	// If we have conflicting entries after PrevLogIndex, we delete those entries
	if len(r.log.entries) > 0 && req.PrevLogIndex < int64(len(r.log.entries))-1 {
		// Delete conflicting entries starting from PrevLogIndex + 1
		r.log.entries = r.log.entries[:req.PrevLogIndex+1]
		mongodb.TrimLog(*r.mongoClient, r.id, req.PrevLogIndex+1)
	}

	// Append new entries to the log if any (The requests are 0-based)
	for _, entry := range req.Entries {
		// If there is already an entry at this index, replace it (log overwrite protection)
		if entry.Index < int64(len(r.log.entries)) {
			r.log.entries[entry.Index] = LogEntry{
				Index:     entry.Index,
				Term:      entry.Term,
				Data:      entry.Data,
				seqNo:     entry.SeqNo,
				clientID:  entry.ClientID,
				entryType: LogEntryType(entry.EntryType),
			}
			mongodb.ChangeLog(*r.mongoClient, r.id, entry.Index, entry.Term, entry.Index, entry.Data, entry.SeqNo, entry.ClientID, mongodb.LogEntryType(entry.EntryType))

		} else {
			// Append new log entries
			r.log.entries = append(r.log.entries, LogEntry{
				Index:     entry.Index,
				Term:      entry.Term,
				Data:      entry.Data,
				seqNo:     entry.SeqNo,
				clientID:  entry.ClientID,
				entryType: LogEntryType(entry.EntryType),
			})

			mongodb.AddLog(*r.mongoClient, r.id, entry.Term, entry.Index, entry.Data, entry.SeqNo, entry.ClientID, mongodb.LogEntryType(entry.EntryType))
		}
	}

	r.logger.Log("Log Entries:\n%s", r.log)

	// Update commit index if leaderCommit is greater than our commitIndex
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = min(req.LeaderCommit, int64(len(r.log.entries)))
		// Commit loop has been advanced, so try to apply the operations to state machines
		r.applyCond.Broadcast()
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

	// Notify that leadership is lost to the client
	r.operationManager.notifyLostLeaderShip()
	r.operationManager = newOperationManager()

	r.logger.Log("transitioned to candidate state with currentTerm: %d", r.currentTerm)
}

func (r *RaftNode) becomeFollower(leaderID string, term int64) {
	r.state = Follower
	r.leaderId = leaderID
	r.votedFor = ""
	r.currentTerm = term
	r.saveStateToDB()

	// Notify that leadership is lost to the client
	r.operationManager.notifyLostLeaderShip()
	r.operationManager = newOperationManager()

	r.logger.Log("transitioned to follower state with currentTerm: %d", r.currentTerm)
}

func (r *RaftNode) becomeLeader() {
	r.state = Leader
	r.leaderId = r.id
	for _, follower := range r.followersList {
		follower.nextIndex = int64(len(r.log.entries)) // Log indexing
		follower.matchIndex = -1
	}

	responsesRcd := 1
	for id, addr := range r.config.Members {
		if id != r.id {
			go r.sendAppendEntries(id, addr, &responsesRcd)
		}
	}

	r.logger.Log("Node transitioned to leader state with currentTerm: %d", r.currentTerm)
}

// returns true if majority has been reached for the input number of votes
// should be called inside a thread safe func
func (r *RaftNode) hasMajority(count int) bool {
	return count > len(r.config.Members)/2
}

// To be done after calling InitServer
func (r *RaftNode) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.restoreStates()

	r.node.registerRequestVoteHandler(r.RequestVoteHandler)
	r.node.registerAppendEntriesHandler(r.AppendEntriesHandler)
	r.node.registerSubmitOperationHandler(r.SubmitOperationHandler)

	// Initalise the followers list
	for id := range r.config.Members {
		r.followersList[id] = new(followerState)
	}
	r.state = Follower
	r.lastContact = time.Now()

	r.wg.Add(5)
	go r.electionClock()
	go r.electionLoop()
	go r.heartbeatLoop()
	go r.commitLoop()
	go r.applyLoop()

	if err := r.node.Start(); err != nil {
		return err
	}
	r.logger.Log("Server is started")

	return nil
}

// saveStateToDB saves the current state of the node to the database
func (r *RaftNode) saveStateToDB() error {
	err := mongodb.Voted(*r.mongoClient, r.id, r.votedFor, r.currentTerm)
	if err != nil {
		return fmt.Errorf("error while saving state to database: %s", err)
	}
	return nil
}

// convertToProtoEntries converts log entries to protobuf format for AppendEntriesRequest
func convertToProtoEntries(entries []LogEntry) []*pb.LogEntry {
	var protoEntries []*pb.LogEntry
	for _, entry := range entries {
		protoEntries = append(protoEntries, &pb.LogEntry{
			Index:     entry.Index,
			Term:      entry.Term,
			Data:      entry.Data,
			SeqNo:     entry.seqNo,
			ClientID:  entry.clientID,
			EntryType: pb.LogEntry_LogEntryType(entry.entryType),
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

// Getter for currentTerm
func (r *RaftNode) GetCurrentTerm() int64 {
	return r.currentTerm
}

// Setter for currentTerm (useful for testing)
func (r *RaftNode) SetCurrentTerm(term int64) {
	r.currentTerm = term
}

// Getter for state
func (r *RaftNode) GetState() State {
	return r.state
}

// Setter for state (useful for testing)
func (r *RaftNode) SetState(state State) {
	r.state = state
}

// Getter for leaderId
func (r *RaftNode) GetLeaderId() string {
	return r.leaderId
}

// Setter for leaderId (useful for testing)
func (r *RaftNode) SetLeaderId(leaderId string) {
	r.leaderId = leaderId
}

// Getter for log
func (r *RaftNode) GetLog() *Log {
	return r.log
}

// Setter for log (useful for testing)
func (r *RaftNode) SetLog(log *Log) {
	r.log = log
}

// Getter for commitIndex
func (r *RaftNode) GetCommitIndex() int64 {
	return r.commitIndex
}

// Setter for commitIndex (useful for testing)
func (r *RaftNode) SetCommitIndex(commitIndex int64) {
	r.commitIndex = commitIndex
}

// Getter for entries in Log
func (l *Log) GetEntries() []LogEntry {
	return l.entries
}

// Setter for entries in Log (useful for testing)
func (l *Log) SetEntries(entries []LogEntry) {
	l.entries = entries
}

// committedThisTerm returns true if a log entry from the current term
// has been committed and false otherwise.
func (r *RaftNode) committedThisTerm() bool {
	// Find the index of the first log entry from the current term
	firstIndex := int64(-1)
	for i := len(r.log.entries) - 1; i >= 0; i-- {
		if r.log.entries[i].Term == r.currentTerm {
			firstIndex = int64(i)
		}
	}

	// If no log entry from the current term exists, return false
	if firstIndex == -1 {
		return false
	}

	// Check if the first log entry from the current term has been committed
	return r.commitIndex >= firstIndex
}

// pendingConfigurationChange returns true if the current configuration
// has not been committed and false otherwise.
func (r *RaftNode) pendingConfigurationChange() bool {
	return r.commitedConfig == nil || r.commitedConfig.LogIndex != r.config.LogIndex
}

// isVoter returns true if the node with the provided ID
// is a voting member of the cluster and false otherwise.
func (r *RaftNode) isVoter(id string) bool {
	return r.config.IsVoter[id]
}

// isMember returns true if the node with the provided ID
// is a member of the cluster and false otherwise.
func (r *RaftNode) isMember(id string) bool {
	_, ok := r.config.Members[id]
	return ok
}
