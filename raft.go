package raft

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
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

func main() {
	port := os.Args[1]
	fmt.Println(port)
	http.HandleFunc("/", getRoot)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		fmt.Println("Error in ListenAndServe: ", err)
		return
	}
}
