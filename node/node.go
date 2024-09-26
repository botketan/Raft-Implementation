package node

import (
	"fmt"
	"net"
	pb "raft/protos"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Struct to store the information about the peer and the gRPC clients
type peer struct {
	conn   *grpc.ClientConn
	client pb.RaftClient
}

type peers struct {
	peerMap map[string]*peer
	mu      sync.Mutex
}

// Initialise the peers and return an instance
func InitPeers() *peers {
	return &peers{
		peerMap: make(map[string]*peer),
	}
}

// gets a peer client of an address mentioned, if it doesn't exist it will be created
func (p *peers) getPeerClient(address string) (pb.RaftClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if peer, ok := p.peerMap[address]; ok {
		return peer.client, nil
	}

	// Create a new client with that adress if it doesn't exits
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("error establishing connection to a Raft Peer: %w", err)
	}

	// Create a new peer and add it to the map
	newPeer := &peer{
		conn:   conn,
		client: pb.NewRaftClient(conn),
	}
	p.peerMap[address] = newPeer

	return p.peerMap[address].client, nil
}

// Close all peer connections - cleanup function
func (p *peers) closeAllPeerConn() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, peer := range p.peerMap {
		peer.conn.Close()
		delete(p.peerMap, addr)
	}
}

type Node struct {
	pb.UnimplementedRaftServer

	address  net.Addr     // Address of this server to make RPC calls
	mu       sync.RWMutex // Mutex to ensure concurrency
	server   *grpc.Server // gRPC server for nodes
	peerList *peers       // Struct to manage the peers
	running  bool         // Is the server running?

	// RPC Handlers (to be registered in main raft node)
	appendEntriesHandler func(*pb.AppendEntriesRequest, *pb.AppendEntriesResponse) error
	requestVoteHandler   func(*pb.RequestVoteRequest, *pb.RequestVoteResponse) error
}

func InitNode(addr string) (*Node, error) {
	netAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("error resolving TCP address : %w", err)
	}
	return &Node{
		address:  netAddr,
		peerList: InitPeers(),
	}, nil
}

// Registers the handlers for RPCs
func (n *Node) registerAppendEntriesHandler(
	handler func(*pb.AppendEntriesRequest, *pb.AppendEntriesResponse) error,
) {
	n.appendEntriesHandler = handler
}

func (n *Node) registerRequestVoteHandler(
	handler func(*pb.RequestVoteRequest, *pb.RequestVoteResponse) error,
) {
	n.requestVoteHandler = handler
}

// Start the node and receive RPCs
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.running {
		return nil
	}

	list, err := net.Listen(n.address.Network(), n.address.String())
	if err != nil {
		return fmt.Errorf("error starting the raft node: %w", err)
	}

	// Intialise the new gRPC server and register it
	n.server = grpc.NewServer()
	pb.RegisterRaftServer(n.server, n)

	go n.server.Serve(list)
	n.running = true
	return nil
}

// Shutdown the RPC server
func (n *Node) Shutdown() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	defer n.peerList.closeAllPeerConn()

	if !n.running {
		return nil
	}

	n.running = false

	stopped := make(chan interface{})
	go func() {
		n.server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-time.After(350 * time.Millisecond):
		n.server.Stop()
	case <-stopped:
		n.server.Stop()
	}

	return nil
}
