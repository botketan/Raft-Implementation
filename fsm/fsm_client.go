package fsm

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
