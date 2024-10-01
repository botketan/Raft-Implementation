package main

import (
	"os"
	"os/signal"
	"raft/node"
	r "raft/node"
	"syscall"
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
	raft1, err := r.InitRaftNode("1", "localhost:8000", config)
	if err != nil {
		panic(err)
	}
	raft2, err := r.InitRaftNode("2", "localhost:8005", config)
	if err != nil {
		panic(err)
	}
	raft3, err := r.InitRaftNode("3", "localhost:8021", config)
	if err != nil {
		panic(err)
	}

	raft1.SetLog(&node.Log{})
	raft1.GetLog().SetEntries([]node.LogEntry{
		{Index: 0, Term: 1, Data: []byte("entry1")},
		{Index: 1, Term: 1, Data: []byte("entry2")},
		{Index: 2, Term: 1, Data: []byte("entry3")},
		{Index: 3, Term: 2, Data: []byte("entry4")},
		{Index: 4, Term: 2, Data: []byte("entry5")},
		{Index: 5, Term: 3, Data: []byte("entry6")},
		{Index: 6, Term: 3, Data: []byte("entry7")},
		{Index: 7, Term: 3, Data: []byte("entry8")},
	})

	raft2.SetLog(&node.Log{})
	raft2.GetLog().SetEntries([]node.LogEntry{
		{Index: 0, Term: 1, Data: []byte("entry1")},
		{Index: 1, Term: 1, Data: []byte("entry2")},
	})

	raft3.SetLog(&node.Log{})

	err = raft1.Start()
	err = raft2.Start()
	err = raft3.Start()

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM)
	<-quitCh
}
