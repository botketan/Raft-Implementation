package main

import (
	"fmt"
	"os"
	"os/signal"
	"raft/client"
	"raft/fsm"
	r "raft/node"
	pb "raft/protos"
	"syscall"
)

func main() {

	config := &pb.Configuration{
		Members: map[string]string{
			"1": "localhost:8000",
			"2": "localhost:8005",
			"3": "localhost:8021",
		},
		LogIndex: -1,
	}

	raft1, err := r.InitRaftNode("1", "localhost:8000", config, fsm.NewFSMManager("1"))
	if err != nil {
		panic(err)
	}
	raft2, err := r.InitRaftNode("2", "localhost:8005", config, fsm.NewFSMManager("2"))
	if err != nil {
		panic(err)
	}
	raft3, err := r.InitRaftNode("3", "localhost:8021", config, fsm.NewFSMManager("3"))
	if err != nil {
		panic(err)
	}

	err = raft1.Start()
	err = raft2.Start()
	err = raft3.Start()

	// Client 1
	cl, err := client.NewRaftClient("client1", map[string]string{
		"1": "localhost:8000",
		"2": "localhost:8005",
		"3": "localhost:8021",
	})

	if err != nil {
		fmt.Println(err)
	}

	go func() {
		client.SimulateClientCommands(cl)
	}()

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, syscall.SIGINT, syscall.SIGTERM)
	<-quitCh
}
