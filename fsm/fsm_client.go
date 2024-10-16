package fsm

import (
	"fmt"
	"strings"
	"sync"
)

type Type int

const (
	Unknown Type = iota
	Get
	Set
)

func (t Type) String() string {
	switch t {
	case Get:
		return "GET"
	case Set:
		return "SET"
	default:
		return "UNKNOWN"
	}
}

// Stores the last applied result and the corresponding sequence number
// So that things doesnt get applied twice
type lastApplied struct {
	sequenceNo int64
	lastResult interface{}
}

// Tells us about the command if it was a get or set
// what is the key and value - These to be ignored if it was a set operation
type command struct {
	cmdType Type
	key     string
	value   string
}

// Creates a basic KeyValue store
type FSMManager struct {
	// Key -> value Map
	kvmap map[string]string

	// Client ID (different from Raft node id) -> lastApplied map
	sessionMap map[string]*lastApplied

	// serverID for debugging process
	serverID string

	mu sync.RWMutex
}

func NewFSMManager(sID string) *FSMManager {
	return &FSMManager{
		kvmap:      make(map[string]string),
		sessionMap: make(map[string]*lastApplied),
		serverID:   sID,
	}
}

func (f *FSMManager) decodeOperation(op []byte) (command, error) {
	opStr := strings.ToLower(string(op))
	parts := strings.Fields(opStr)

	if len(parts) < 2 || len(parts) > 3 {
		return command{}, fmt.Errorf("invalid operation: %s", opStr)
	}

	operation := command{
		key: parts[1], // The key (e.g., "x")
	}

	// Determine the command based on the first part
	switch parts[0] {
	case "get":
		operation.cmdType = Get
	case "set":
		operation.cmdType = Set
	default:
		operation.cmdType = Unknown
	}

	// If there's a third part, it's the value
	if len(parts) == 3 {
		operation.value = parts[2]
	}

	return operation, nil
}

func (f *FSMManager) HandleGet(key string) interface{} {
	f.mu.RLock()
	defer f.mu.RUnlock()

	val, ok := f.kvmap[key]

	if !ok {
		return fmt.Errorf("key: %s not found in the KV Store", key)
	}

	return val
}

func (f *FSMManager) HandleSet(key string, value string, clientId string, seqNo int64) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	sesh, ok := f.sessionMap[clientId]

	if ok && sesh.sequenceNo >= seqNo {
		fmt.Printf("[%s] Incoming SeqNo: %d, Found in session store SeqNo: %d\n", f.serverID, seqNo, sesh.sequenceNo)
		return sesh.lastResult
	}

	fmt.Printf("[%s] Incoming SeqNo: %d, not found in session store. Serving Fresh!\n", f.serverID, seqNo)

	if !ok {
		f.sessionMap[clientId] = &lastApplied{}
	}

	f.kvmap[key] = value
	resp := fmt.Sprintf("Successfully set {Key: %s, Value: %s} in KVStore", key, value)
	f.sessionMap[clientId].sequenceNo = seqNo
	f.sessionMap[clientId].lastResult = resp
	return resp
}

// Applies the operation to the FSM

func (f *FSMManager) Apply(clientReq *ClientOperationRequest) interface{} {
	command, err := f.decodeOperation(clientReq.Operation)
	if err != nil {
		return err
	}

	switch command.cmdType {
	case Get:
		return f.HandleGet(command.key)
	case Set:
		return f.HandleSet(command.key, command.value, clientReq.ClientID, clientReq.SeqNo)
	default:
		return fmt.Errorf("unknown operation %s by clientID: %s, please try again", string(clientReq.Operation), clientReq.ClientID)
	}
}
