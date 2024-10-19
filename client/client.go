package client

import (
	"context"
	"fmt"
	mongodb "raft/mongodb"
	lgr "raft/node"
	pb "raft/protos" // Protobuf definitions for Raft
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RaftClient defines the client structure to interact with the Raft cluster.
type RaftClient struct {
	clientID    string                   // Unique ID for this client
	seqNo       int64                    // Sequence number for operations
	leaderID    string                   // Current known leader ID
	raftNodes   map[string]string        // Map of all known Raft node addresses
	clientsList map[string]pb.RaftClient // Map of client connections to Raft node
	logger      lgr.Logger
	mongoClient *mongo.Client
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
	var clientState = lgr.Client
	logger, _ := lgr.NewLogger(clientID, &clientState)
	mclient, err := mongodb.Connect()
	if err != nil {
		fmt.Print("Error connecting to mongo :")
		fmt.Println(err)
	}
	ct := mongodb.GetClient(*mclient, clientID)
	seqNo := 1
	if ct.SeqNo > 0 {
		seqNo = int(ct.SeqNo)
	}
	return &RaftClient{
		clientID:    clientID,
		seqNo:       int64(seqNo), // Initialize sequence number to 1
		raftNodes:   nodes,
		leaderID:    "", // Initially, the leader is unknown
		clientsList: clients,
		logger:      logger,
		mongoClient: mclient,
	}, nil
}

func (client *RaftClient) UpdateSeq() error {
	return mongodb.UpdateClient(*client.mongoClient, client.clientID, client.seqNo)
}

// SubmitOperation submits an operation to the Raft cluster with a clientID and seqNo.
func (client *RaftClient) SubmitOperation(op []byte, timeout time.Duration) (string, error) {
	defer func() {
		// Increase sequence number after every successful response
		client.seqNo++
		err := client.UpdateSeq()
		if err != nil {
			client.logger.Log("Error in Mongo :%v", err)
		}
	}()

	var err error
	var response string

	for start := time.Now(); time.Since(start) < timeout; {
		// If the leader is known, try submitting to the leader
		if client.leaderID != "" {
			response, err = client.submit(client.leaderID, op)
			if response == "REDIRECT" {
				client.logger.Log("Redirected to new leader at %s", client.leaderID)
				continue
			}
			if err == nil {
				return response, nil
			}
			client.leaderID = "" // Reset leader if submission to leader failed
			client.logger.Log("Failed to submit to leader at %s: %v", client.leaderID, err)
		}

		// If leader is unknown or submission to leader failed, try all nodes
		for node := range client.raftNodes {
			response, err = client.submit(node, op)
			client.logger.Log("Got response: %v, err: %v from Server ID: %v\n\n", response, err, node)
			if err == nil {
				return response, nil
			}

			// If the node redirected us to a new leader, retry with the new leader
			if client.leaderID != "" {
				client.logger.Log("Client Discovered new leader at %s", client.leaderID)
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

	client.logger.Log("Sending Operation {%v} to Server ID: %v\n\n", req, leaderID)

	resp, err := raftClient.SubmitOperation(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to submit operation: %v", err)
	}

	if !resp.GetSuccess() {
		// If the node we contacted is not the leader, it provides the correct leader address
		if strings.HasPrefix(resp.GetMessage(), "REDIRECT") {
			client.leaderID = strings.TrimPrefix(resp.GetMessage(), "REDIRECT ")
			return "REDIRECT", fmt.Errorf("redirected to new leader at %s", client.leaderID)
		}
		return "", fmt.Errorf("operation submission returned error: %v", resp.GetMessage())
	}

	return resp.GetMessage(), nil
}

func SimulateClientCommands(client *RaftClient) {
	<-time.After(time.Second * 2)
	client.logger.Log("Submitting operations...")
	resp, err := client.SubmitOperation([]byte("GET X"), 2*time.Minute)
	if err != nil {
		client.logger.Log("Error for GET X : %v\n\n", err)
	} else {
		client.logger.Log("Response for GET X : %v\n\n", resp)
	}
	resp, err = client.SubmitOperation([]byte("SET Y 9"), 2*time.Minute)
	if err != nil {
		client.logger.Log("Error for SET Y 9: %v\n\n", err)
	} else {
		client.logger.Log("Response for SET Y 9: %v\n\n", resp)
	}
	<-time.After(time.Second * 1)
	resp, err = client.SubmitOperation([]byte("GET Y"), 2*time.Minute)
	if err != nil {
		client.logger.Log("Error for GET Y: %v\n\n", err)
	} else {
		client.logger.Log("Response for GET Y: %v\n\n", resp)
	}
	resp, err = client.SubmitOperation([]byte("GET X"), 2*time.Minute)
	if err != nil {
		client.logger.Log("Error for GET X: %v\n\n", err)
	} else {
		client.logger.Log("Response for GET X: %v\n\n", resp)
	}
	<-time.After(time.Second * 6)
	resp, err = client.SubmitOperation([]byte("SET X new_val"), 2*time.Minute)
	if err != nil {
		client.logger.Log("Error for SET X new_val: %v\n\n", err)
	} else {
		client.logger.Log("Response for SET X new_val: %v\n\n", resp)
	}
	<-time.After(time.Second * 10)
	resp, err = client.SubmitOperation([]byte("GET X"), 2*time.Minute)
	if err != nil {
		client.logger.Log("Error for GET X: %v\n\n", err)
	} else {
		client.logger.Log("Response for GET X: %v\n\n", resp)
	}
}
