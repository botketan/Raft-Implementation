package main

import (
	"os"
	"os/signal"
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

	err = raft1.Start()
	err = raft2.Start()
	err = raft3.Start()

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM)
	<-quitCh
}
