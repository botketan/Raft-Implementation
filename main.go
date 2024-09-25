package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	node "raft/node"
	pb "raft/protos"
	"sync"
	"time"
)

type Server struct {
	id         int
	address    string
	httpServer *http.Server
	mutex      sync.Mutex
}

func getRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("got / request\n")
	io.WriteString(w, "Normal Request\n")
}

func serverTest() {
	port := os.Args[1]
	fmt.Println(port)
	http.HandleFunc("/", getRoot)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		fmt.Println("Error in ListenAndServe: ", err)
		return
	}
}

type raftServer struct {
	n *node.Node
}

func main() {
	//mongoDb.TestConnect()
	node1, _ := node.InitNode("localhost:8001")
	server1 := raftServer{
		n: node1,
	}
	node2, _ := node.InitNode("localhost:8005")
	server2 := raftServer{
		n: node2,
	}
	server1.n.Start()
	server2.n.Start()
	defer server1.n.Shutdown()
	defer server2.n.Shutdown()

	time.Sleep(100 * time.Millisecond)

	helloReq := &pb.Hello{Servername: "Node1"}
	response, err := server1.n.SendHelloHelper("127.0.0.1:8005", helloReq)
	if err != nil {
		log.Fatalf("Failed to send Hello RPC from node1 to node2: %v", err)
	}

	log.Printf("Received response from Node2: %s", response.Clientname)

}
