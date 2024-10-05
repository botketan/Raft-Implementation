package fsm

type FSM interface {
	// Apply the operation specified by the input
	// eg: If the operation is like "SET X 5", then the FSM should set it to 5
	Apply(operation []byte) interface{}
}
