package node

import "fmt"

type Operation struct {
	Bytes []byte
	// The log entry index associated with the operation.
	// Valid only if this is a replicated operation and the operation was successful.
	LogIndex int64

	// The log entry term associated with the operation.
	// Valid only if this is a replicated operation and the operation was successful.
	LogTerm int64
}

func (o Operation) String() string {
	return fmt.Sprintf(
		"Operation{Bytes: %s, LogIndex: %d, LogTerm: %d}",
		string(o.Bytes), o.LogIndex, o.LogTerm,
	)
}

type OperationResponse struct {
	// The operation applied to the state machine.
	Operation Operation

	// The response returned by the state machine after applying the operation.
	ApplicationResponse interface{}
}

func (r OperationResponse) String() string {
	return fmt.Sprintf(
		"OperationResponse{Operation: %s, ApplicationResponse: %v}",
		r.Operation.String(), r.ApplicationResponse,
	)
}

type operationManager struct {

	// Maps log index associated with the operation to its response channel.
	pendingReplicated map[int64]chan Result[OperationResponse]
}

func newOperationManager() *operationManager {
	return &operationManager{
		pendingReplicated: make(map[int64]chan Result[OperationResponse]),
	}
}

// This is used to tell clients of lost leadership
func (r *operationManager) notifyLostLeaderShip() {
	for _, responseCh := range r.pendingReplicated {
		responseCh <- &result[OperationResponse]{err: fmt.Errorf("not a leader")}
	}
	r.pendingReplicated = make(map[int64]chan Result[OperationResponse])
}