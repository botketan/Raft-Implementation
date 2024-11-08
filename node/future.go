package node

import (
	"errors"
	"time"
)

// ErrTimeout is returned when a future timed out waiting for a result.
var ErrTimeout = errors.New(
	"a timeout occurred while waiting for the result - try submitting the operation again",
)

// Response is the concrete result produced by a node after processing a client submitted operation.
type Response interface {
	OperationResponse | Configuration
}

// Future represents an operation that will occur at a later point in time.
type Future[T Response] interface {
	// Await retrieves the result of the future.
	Await() Result[T]
}

// future implements the Future interface.
type future[T Response] struct {
	// The channel that will receive the result.
	responseCh chan Result[T]

	// The amount of time to wait on a result before timing out.
	timeout time.Duration

	// The result of the future.
	response Result[T]
}

func newFuture[T Response](timeout time.Duration) *future[T] {
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

// Result represents an abstract result produced by a node after processing a
// client submitted operation.
type Result[T Response] interface {
	// Success returns the response associated with an operation.
	// Error should always be called before Success - the result
	// returned by Success is only valid if Error returns nil.
	Success() T

	// Error returns any error that occurred during the
	// operation that was to produce the response.
	Error() error
}

// result implements the Result interface.
type result[T Response] struct {
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

// respond creates a result with the provided response and error and sends
// it to the response channel without blocking.
func respond[T Response](responseCh chan Result[T], response T, err error) {
	select {
	case responseCh <- &result[T]{success: response, err: err}:
	default:
	}
}