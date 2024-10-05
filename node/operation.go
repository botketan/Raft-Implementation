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

type Result[T OperationResponse] interface {
	// Success returns the response associated with an operation.
	// Error should always be called before Success - the result
	// returned by Success is only valid if Error returns nil.
	Success() T

	// Error returns any error that occurred during the
	// operation that was to produce the response.
	Error() error
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

type result[T OperationResponse] struct {
	// The actual result of an operation.
	success T

	// Any error that occurred during the processing of the result.
	err error
}

func (r *result[T]) Success() T {
	return r.success
}

func (r *result[T]) Error() error {
	return r.err
}
