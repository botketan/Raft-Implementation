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
	clientID      string   // Unique ID for this client
	seqNo         int64    // Sequence number for operations
	leaderAddress string   // Current known leader address
	raftNodes     []string // All known Raft node addresses
}

// NewRaftClient creates a new RaftClient with a unique clientID and list of node addresses.
func NewRaftClient(clientID string, nodes []string) *RaftClient {
	return &RaftClient{
		clientID:      clientID,
		seqNo:         0, // Initialize sequence number to 0
		raftNodes:     nodes,
		leaderAddress: "", // Initially, the leader is unknown
	}
}

// SubmitOperation submits an operation to the Raft cluster with a clientID and seqNo.
func (client *RaftClient) SubmitOperation(op []byte) error {
	var err error

	for {
		// If the leader is known, try submitting to the leader
		if client.leaderAddress != "" {
			err = client.submit(client.leaderAddress, op)
			if err == nil {
				return nil
			}
			log.Printf("Failed to submit to leader at %s: %v", client.leaderAddress, err)
		}

		// If leader is unknown or submission to leader failed, try all nodes
		for _, node := range client.raftNodes {
			err = client.submit(node, op)
			if err == nil {
				return nil
			}

			// If the node redirected us to a new leader, retry with the new leader
			if client.leaderAddress != "" {
				log.Printf("Discovered new leader at %s", client.leaderAddress)
				break
			}
		}
	}
}

// submit tries to submit the operation to the current leader node.
func (client *RaftClient) submit(leaderAddress string, op []byte) error {
	conn, err := grpc.NewClient(leaderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %v", err)
	}
	defer conn.Close()

	raftClient := pb.NewRaftClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Increment the sequence number for each operation
	client.seqNo++

	req := &pb.SubmitOperationRequest{
		ClientId: client.clientID,
		SeqNo:    client.seqNo,
		Operation: op,
	}

	resp, err := raftClient.SubmitOperation(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to submit operation: %v", err)
	}

	if !resp.GetSuccess() {
		// If the node we contacted is not the leader, it provides the correct leader address
		if strings.HasPrefix(resp.GetMessage(), "REDIRECT") {
			client.leaderAddress = strings.TrimPrefix(resp.GetMessage(), "REDIRECT ")
			return fmt.Errorf("redirected to new leader at %s", client.leaderAddress)
		}
		return fmt.Errorf("operation submission failed: %v", resp.GetMessage())
	}

	log.Printf("Operation successfully submitted and committed: %s", resp.GetMessage())
	return nil
}