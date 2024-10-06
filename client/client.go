package client

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	pb "raft/protos" // Protobuf definitions for Raft

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RaftClient defines the client structure to interact with the Raft cluster.
type RaftClient struct {
	clientID    string                   // Unique ID for this client
	seqNo       int64                    // Sequence number for operations
	leaderID    string                   // Current known leader ID
	raftNodes   map[string]string        // Map of all known Raft node addresses
	clientsList map[string]pb.RaftClient // Map of client connections to Raft nodes
}

// NewRaftClient creates a new RaftClient with a unique clientID and list of node addresses.
func NewRaftClient(clientID string, nodes map[string]string) (*RaftClient, error) {
	clients := make(map[string]pb.RaftClient, len(nodes))
	for id, address := range nodes {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}

		clients[id] = pb.NewRaftClient(conn)
	}
	return &RaftClient{
		clientID:    clientID,
		seqNo:       0, // Initialize sequence number to 0
		raftNodes:   nodes,
		leaderID:    "", // Initially, the leader is unknown
		clientsList: clients,
	}, nil
}

// SubmitOperation submits an operation to the Raft cluster with a clientID and seqNo.
func (client *RaftClient) SubmitOperation(op []byte, timeout time.Duration) (string, error) {
	var err error
	var response string

	for start := time.Now(); time.Since(start) < timeout; {
		// If the leader is known, try submitting to the leader
		if client.leaderID != "" {
			response, err = client.submit(client.leaderID, op)
			if response == "REDIRECT" {
				log.Printf("Redirected to new leader at %s", client.leaderID)
				continue
			}
			if err == nil {
				return response, nil
			}
			client.leaderID = "" // Reset leader if submission to leader failed
			log.Printf("Failed to submit to leader at %s: %v", client.leaderID, err)
		}

		// If leader is unknown or submission to leader failed, try all nodes
		for node := range client.raftNodes {
			log.Printf("Sending to %v\n", node)
			response, err = client.submit(node, op)
			log.Printf("Got response: %v, err: %v\n", response, err)
			if err == nil {
				return response, nil
			}

			// If the node redirected us to a new leader, retry with the new leader
			if client.leaderID != "" {
				log.Printf("Client Discovered new leader at %s", client.leaderID)
				break
			}
		}
	}

	return "", fmt.Errorf("timeout while submitting operation: %v", err)
}

// submit tries to submit the operation to the current leader node.
func (client *RaftClient) submit(leaderID string, op []byte) (string, error) {
	raftClient := client.clientsList[leaderID]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	req := &pb.SubmitOperationRequest{
		ClientId:  client.clientID,
		SeqNo:     client.seqNo,
		Operation: op,
	}
	
	resp, err := raftClient.SubmitOperation(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to submit operation: %v", err)
	}

	// Increment the sequence number if the operation was submitted successfully
	client.seqNo++

	if !resp.GetSuccess() {
		// If the node we contacted is not the leader, it provides the correct leader address
		if strings.HasPrefix(resp.GetMessage(), "REDIRECT") {
			client.leaderID = strings.TrimPrefix(resp.GetMessage(), "REDIRECT ")
			return "REDIRECT", fmt.Errorf("redirected to new leader at %s", client.leaderID)
		}
		return "", fmt.Errorf("operation submission returned error: %v", resp.GetMessage())
	}
	
	log.Printf("Operation successfully submitted and committed: %s", resp.GetMessage())

	return resp.GetMessage(), nil
}

func SimulateClientCommands(client *RaftClient) {
	<-time.After(time.Second * 2)
	log.Printf("Submitting operations...")
	resp, err := client.SubmitOperation([]byte("GET X"), 2*time.Minute)
	if err != nil {
		fmt.Printf("Error for GET X : %v\n", err)
	} else {
		fmt.Printf("Response for GET X : %v\n", resp)
	}
	resp, err = client.SubmitOperation([]byte("SET X fuckyou"), 2*time.Minute)
	if err != nil {
		fmt.Printf("Error for SET X: %v\n", err)
	} else {
		fmt.Printf("Response for SET X: %v\n", resp)
	}
	<-time.After(time.Second * 1)
	resp, err = client.SubmitOperation([]byte("GET X"), 2*time.Minute)
	if err != nil {
		fmt.Printf("Error for GET X: %v\n", err)
	} else {
		fmt.Printf("Response for GET X: %v\n", resp)
	}
	resp, err = client.SubmitOperation([]byte("GET X"), 2*time.Minute)
	if err != nil {
		fmt.Printf("Error for GET X: %v\n", err)
	} else {
		fmt.Printf("Response for GET X: %v\n", resp)
	}
}
