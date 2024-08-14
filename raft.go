package main

import (
	"fmt"
	"net/http"
	"sync"
)

type Server struct {
	id         int
	address    string
	httpServer *http.Server
	mutex      sync.Mutex
}

func main() {
	fmt.Println("Hello, world.")
}
