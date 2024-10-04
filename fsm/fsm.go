package fsm

type FSM interface {
	// Apply the operation specified by the input
	// eg: If the operation is like "SET X 5", then the FSM should set it to 5
	Apply(operation []byte) interface{}
}

type FSMManager struct {
	// The FSM instance
}

func NewFSMManager() FSM {
	return &FSMManager{}
}

func (f *FSMManager) Apply(operation []byte) interface{} {
	// Apply the operation to the FSM
	return nil
}
