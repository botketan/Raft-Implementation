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

// Stores the last applied result and returns the response
// So that things doesnt get applied twice
type lastApplied struct {
	sequenceNo int64
	lastResult []byte
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

	// Node id -> lastApplied map
	sessionMap map[string]lastApplied

	mu sync.RWMutex
}

func NewFSMManager() *FSMManager {
	return &FSMManager{
		kvmap:      make(map[string]string),
		sessionMap: make(map[string]lastApplied),
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

func (f *FSMManager) HandleSet(key string, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.kvmap[key] = value
	return fmt.Sprintf("Successfully set {Key: %s, Value: %s} in KVStore", key, value)
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
		return f.HandleSet(command.key, command.value)
	default:
		return fmt.Errorf("unknown operation %s, please try again", string(clientReq.Operation))
	}
}
