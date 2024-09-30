package node_test

import (
	"fmt"
	"raft/node"
	pb "raft/protos"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupRaftNode() *node.RaftNode {
	config := &node.Configuration{
		Members: map[string]string{
			"node1": "localhost:5001",
			"node2": "localhost:5002",
			"node3": "localhost:5003",
		},
	}
	raftNode, err := node.InitRaftNode("node1", "localhost:5001", config)
	if err != nil {
		fmt.Println("Error initializing Raft node:", err)
	}
	return raftNode
}

// Test case: Reject AppendEntries with lower term
func TestAppendEntriesLowerTerm(t *testing.T) {
	raftNode := setupRaftNode()

	// Set CurrentTerm using setter
	raftNode.SetCurrentTerm(2)

	req := &pb.AppendEntriesRequest{
		Term: 1, // lower than current term
	}

	resp := &pb.AppendEntriesResponse{}
	err := raftNode.AppendEntriesHandler(req, resp)

	assert.NoError(t, err)
	assert.Equal(t, int64(2), resp.Term)
	assert.False(t, resp.Success, "AppendEntries should fail with lower term")
}

// Test case: AppendEntries updates follower's term and becomes follower
func TestAppendEntriesUpdateTermAndBecomeFollower(t *testing.T) {
	raftNode := setupRaftNode()

	// Set the node's term lower than the request term
	raftNode.SetCurrentTerm(1)
	raftNode.SetState(node.Candidate) // Set the state to Candidate

	// Create an AppendEntriesRequest with a higher term
	req := &pb.AppendEntriesRequest{
		Term:     2, // higher than current term
		LeaderId: "leader2",
	}

	resp := &pb.AppendEntriesResponse{}
	err := raftNode.AppendEntriesHandler(req, resp)

	assert.NoError(t, err)
	assert.Equal(t, int64(2), raftNode.GetCurrentTerm(), "Node's term should be updated to the leader's term")
	assert.Equal(t, int64(2), resp.Term)
	assert.Equal(t, "leader2", raftNode.GetLeaderId(), "Node should update the leader ID")
	assert.True(t, resp.Success, "AppendEntries should succeed")
	assert.Equal(t, node.Follower, raftNode.GetState(), "Node should become a follower")
}

// Test case: Reject AppendEntries if previous log doesn't match
func TestAppendEntriesLogInconsistency(t *testing.T) {
	raftNode := setupRaftNode()

	// Set the current term and leader ID
	raftNode.SetCurrentTerm(2)
	raftNode.SetLeaderId("leader1")

	// Set the log entries using the setter method
	raftNode.SetLog(&node.Log{})
	raftNode.GetLog().SetEntries([]node.LogEntry{
		{Index: 0, Term: 1, Data: []byte("entry1")},
	})

	// Create an AppendEntriesRequest with a mismatching PrevLogIndex and PrevLogTerm
	req := &pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "leader1",
		PrevLogIndex: 0, // Log entry exists at index 1 but with a different term
		PrevLogTerm:  2, // This should cause a failure
	}

	resp := &pb.AppendEntriesResponse{}
	err := raftNode.AppendEntriesHandler(req, resp)

	assert.NoError(t, err)
	assert.Equal(t, int64(2), resp.Term)
	assert.False(t, resp.Success, "AppendEntries should fail due to log inconsistency")
}

// Test case: Append new log entries and update commit index
func TestAppendEntriesAppendAndCommit(t *testing.T) {
	raftNode := setupRaftNode()

	// Set the current term and leader ID
	raftNode.SetCurrentTerm(2)
	raftNode.SetLeaderId("leader1")
	raftNode.SetCommitIndex(0)

	// Create an AppendEntriesRequest with new log entries
	req := &pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "leader1",
		PrevLogIndex: 0, // No previous log entry
		PrevLogTerm:  0,
		LeaderCommit: 2, // Commit index update
		Entries: []*pb.LogEntry{
			{Index: 1, Term: 2, Data: []byte("entry1")},
			{Index: 2, Term: 2, Data: []byte("entry2")},
		},
	}

	resp := &pb.AppendEntriesResponse{}
	err := raftNode.AppendEntriesHandler(req, resp)

	assert.NoError(t, err)
	assert.True(t, resp.Success, "AppendEntries should succeed")

	// Ensure the log is updated correctly using the getter method
	entries := raftNode.GetLog().GetEntries()
	assert.Equal(t, 2, len(entries), "Log should have two new entries")
	assert.Equal(t, int64(2), raftNode.GetCommitIndex(), "Commit index should be updated")
}
