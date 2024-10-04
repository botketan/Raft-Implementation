package node

import (
	"errors"
	"time"
)

// ErrTimeout is returned when a future timed out waiting for a result.
var ErrTimeout = errors.New(
	"a timeout occurred while waiting for the result - try submitting the operation again",
)

// Future represents an operation that will occur at a later point in time.
type Future[T OperationResponse] interface {
	// Await retrieves the result of the future.
	Await() Result[T]
}

// future implements the Future interface.
type future[T OperationResponse] struct {
	// The channel that will receive the result.
	responseCh chan Result[T]

	// The amount of time to wait on a result before timing out.
	timeout time.Duration

	// The result of the future.
	response Result[T]
}

func newFuture[T OperationResponse](timeout time.Duration) *future[T] {
	return &future[T]{
		timeout:    timeout,
		responseCh: make(chan Result[T], 1),
	}
}

func (f *future[T]) Await() Result[T] {
	if f.response != nil {
		return f.response
	}
	select {
	case response := <-f.responseCh:
		f.response = response
	case <-time.After(f.timeout):
		f.response = &result[T]{err: ErrTimeout}
	}
	return f.response
}
