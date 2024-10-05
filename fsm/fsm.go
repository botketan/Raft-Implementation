package fsm

type FSM interface {
	// Apply the operation specified by the input
	// eg: If the operation is like "SET X 5", then the FSM should set it to 5
	Apply(clientReq *ClientOperationRequest) interface{}
}

// This is the format of the client operation that they submit
// Contains the sequence number to avoid re-applying again
type ClientOperationRequest struct {
	SeqNo     int64
	ClientID  string
	Operation []byte
}
