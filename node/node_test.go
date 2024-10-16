package node_test

import (
	"fmt"
	"raft/fsm"
	"raft/node"
	pb "raft/protos"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupRaftNode() *node.RaftNode {
	config := &pb.Configuration{
		Members: map[string]string{
			"node1": "localhost:5001",
			"node2": "localhost:5002",
			"node3": "localhost:5003",
		},
		LogIndex: -1,
	}
	raftNode, err := node.InitRaftNode("node1", "localhost:5001", config, fsm.NewFSMManager("node1"))
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
		Term:         2, // higher than current term
		LeaderId:     "leader2",
		PrevLogIndex: -1, // No previous log entry
		PrevLogTerm:  0,
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
		{Index: 1, Term: 2, Data: []byte("entry2")},
		{Index: 2, Term: 2, Data: []byte("entry3")},
	})

	// Create an AppendEntriesRequest with a mismatching PrevLogIndex and PrevLogTerm
	req := &pb.AppendEntriesRequest{
		Term:         4,
		LeaderId:     "leader1",
		PrevLogIndex: 2, // Log entry exists at index 1 but with a different term
		PrevLogTerm:  3, // This should cause a failure
	}

	resp := &pb.AppendEntriesResponse{}
	err := raftNode.AppendEntriesHandler(req, resp)

	assert.NoError(t, err)
	assert.Equal(t, int64(4), resp.Term)
	assert.Equal(t, int64(2), resp.ConflictTerm, "Conflict term should be the term of the mismatching log entry")
	assert.Equal(t, int64(1), resp.ConflictIndex, "Conflict index should be the index of the first mismatching log entry for that term")
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
		PrevLogIndex: -1, // No previous log entry
		PrevLogTerm:  0,
		LeaderCommit: 2, // Commit index update
		Entries: []*pb.LogEntry{
			{Index: 0, Term: 2, Data: []byte("entry1")},
			{Index: 1, Term: 2, Data: []byte("entry2")},
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

// Test case: AppendEntries with matching log entries (no changes should be made)
func TestAppendEntriesWithMatchingLog(t *testing.T) {
	raftNode := setupRaftNode()

	// Set current term and log entries
	raftNode.SetCurrentTerm(3)
	raftNode.SetLog(&node.Log{})
	raftNode.GetLog().SetEntries([]node.LogEntry{
		{Index: 0, Term: 3, Data: []byte("entry1")},
		{Index: 1, Term: 3, Data: []byte("entry2")},
	})

	// Create AppendEntriesRequest with matching log entries
	req := &pb.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "leader1",
		PrevLogIndex: 1,
		PrevLogTerm:  3,
		LeaderCommit: 1,
		Entries: []*pb.LogEntry{
			{Index: 1, Term: 3, Data: []byte("entry2")}, // Matching entry
		},
	}

	resp := &pb.AppendEntriesResponse{}
	err := raftNode.AppendEntriesHandler(req, resp)

	assert.NoError(t, err)
	assert.True(t, resp.Success, "AppendEntries should succeed")
	assert.Equal(t, 2, len(raftNode.GetLog().GetEntries()), "No new entries should be added")
}

// Test case: AppendEntries with empty entries but valid term (heartbeat)
func TestAppendEntriesHeartbeat(t *testing.T) {
	raftNode := setupRaftNode()

	// Set current term
	raftNode.SetCurrentTerm(3)

	// Create AppendEntriesRequest with no new entries (heartbeat)
	req := &pb.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "leader1",
		PrevLogIndex: -1,
		PrevLogTerm:  0,
		LeaderCommit: 0,
		Entries:      []*pb.LogEntry{},
	}

	resp := &pb.AppendEntriesResponse{}
	err := raftNode.AppendEntriesHandler(req, resp)

	assert.NoError(t, err)
	assert.True(t, resp.Success, "Heartbeat should succeed")
}

// Test case: AppendEntries with conflicting log entries (overwrite conflicting entries)
func TestAppendEntriesWithConflictingEntries(t *testing.T) {
	raftNode := setupRaftNode()

	// Set current term and log entries
	raftNode.SetCurrentTerm(3)
	raftNode.SetLog(&node.Log{})
	raftNode.GetLog().SetEntries([]node.LogEntry{
		{Index: 0, Term: 2, Data: []byte("entry1")},
		{Index: 1, Term: 2, Data: []byte("entry2")},
	})

	// Create AppendEntriesRequest with conflicting entries
	req := &pb.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  2,
		LeaderCommit: 2,
		Entries: []*pb.LogEntry{
			{Index: 1, Term: 3, Data: []byte("new entry2")}, // Conflict at index 1
			{Index: 2, Term: 3, Data: []byte("new entry3")},
		},
	}

	resp := &pb.AppendEntriesResponse{}
	err := raftNode.AppendEntriesHandler(req, resp)

	assert.NoError(t, err)
	assert.True(t, resp.Success, "AppendEntries should succeed")

	// Ensure the conflicting log entry was overwritten and new entries appended
	entries := raftNode.GetLog().GetEntries()
	assert.Equal(t, 3, len(entries), "Log should have 3 entries")
	assert.Equal(t, []byte("new entry2"), entries[1].Data, "Conflicting entry should be overwritten")
	assert.Equal(t, []byte("new entry3"), entries[2].Data, "New entry should be appended")
}

// Test case: Delete conflicting log entries and append new entries
func TestAppendEntriesDeleteConflictingEntries(t *testing.T) {
	raftNode := setupRaftNode()

	// Set current term and log entries
	raftNode.SetCurrentTerm(3)
	raftNode.SetLog(&node.Log{})
	raftNode.GetLog().SetEntries([]node.LogEntry{
		{Index: 0, Term: 2, Data: []byte("entry1")},
		{Index: 1, Term: 2, Data: []byte("entry2")},
	})

	// Create AppendEntriesRequest with conflicting entries
	req := &pb.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  2,
		LeaderCommit: 2,
		Entries: []*pb.LogEntry{
			{Index: 1, Term: 3, Data: []byte("new entry2")}, // Conflict at index 1
			{Index: 2, Term: 3, Data: []byte("new entry3")},
		},
	}

	resp := &pb.AppendEntriesResponse{}
	err := raftNode.AppendEntriesHandler(req, resp)

	assert.NoError(t, err)
	assert.True(t, resp.Success, "AppendEntries should succeed")

	// Ensure the conflicting log entry was deleted and new entries appended
	entries := raftNode.GetLog().GetEntries()
	assert.Equal(t, 3, len(entries), "Log should have 3 entries")
	assert.Equal(t, []byte("new entry2"), entries[1].Data, "Conflicting entry should be deleted")
	assert.Equal(t, []byte("new entry3"), entries[2].Data, "New entry should be appended")
}
