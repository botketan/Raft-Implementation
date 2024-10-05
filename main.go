package main

import (
	"fmt"
	"os"
	"os/signal"
	"raft/fsm"
	"raft/node"
	r "raft/node"
	"syscall"
	"time"
)

func main() {
	//mongoDb.TestConnect()

	config := &r.Configuration{
		Members: map[string]string{
			"1": "localhost:8000",
			"2": "localhost:8005",
			"3": "localhost:8021",
		},
	}
	raft1, err := r.InitRaftNode("1", "localhost:8000", config, fsm.NewFSMManager())
	if err != nil {
		panic(err)
	}
	raft2, err := r.InitRaftNode("2", "localhost:8005", config, fsm.NewFSMManager())
	if err != nil {
		panic(err)
	}
	raft3, err := r.InitRaftNode("3", "localhost:8021", config, fsm.NewFSMManager())
	if err != nil {
		panic(err)
	}

	raft1.SetLog(&node.Log{})
	// raft1.GetLog().SetEntries([]node.LogEntry{
	// 	// {Index: 0, Term: 1, Data: []byte("entry1")},
	// 	// {Index: 1, Term: 1, Data: []byte("entry2")},
	// 	// {Index: 2, Term: 1, Data: []byte("entry3")},
	// 	// {Index: 3, Term: 2, Data: []byte("entry4")},
	// 	// {Index: 4, Term: 2, Data: []byte("entry5")},
	// 	// {Index: 5, Term: 3, Data: []byte("entry6")},
	// 	// {Index: 6, Term: 3, Data: []byte("entry7")},
	// 	// {Index: 7, Term: 3, Data: []byte("entry8")},
	// })

	raft2.SetLog(&node.Log{})
	// raft2.GetLog().SetEntries([]node.LogEntry{
	// 	// {Index: 0, Term: 1, Data: []byte("entry1")},
	// 	// {Index: 1, Term: 1, Data: []byte("entry2")},
	// })

	raft3.SetLog(&node.Log{})

	err = raft1.Start()
	err = raft2.Start()
	err = raft3.Start()

	<-time.After(time.Second * 2)
	fmt.Println("Submitting more operations...")
	clReq1 := &fsm.ClientOperationRequest{
		Operation: []byte("set x 5"),
		SeqNo:     1,
	}
	clReq2 := &fsm.ClientOperationRequest{
		Operation: []byte("set y 10"),
		SeqNo:     2,
	}
	clReq3 := &fsm.ClientOperationRequest{
		Operation: []byte("set z 15"),
		SeqNo:     3,
	}

	ft1 := raft1.SubmitOperation(clReq1, time.Second*5)
	ft2 := raft2.SubmitOperation(clReq2, time.Second*5)
	ft3 := raft3.SubmitOperation(clReq3, time.Second*5)

	result1 := ft1.Await()
	result2 := ft2.Await()
	result3 := ft3.Await()

	if result1.Error() != nil {
		fmt.Println(result1.Error())
	} else {
		fmt.Println(result1.Success())
	}
	if result2.Error() != nil {
		fmt.Println(result2.Error())
	} else {
		fmt.Println(result2.Success())
	}
	if result3.Error() != nil {
		fmt.Println(result3.Error())
	} else {
		fmt.Println(result3.Success())
	}

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM)
	<-quitCh
}
