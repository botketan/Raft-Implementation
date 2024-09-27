package mongodb

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Configuration struct {
	members map[string]string `bson:"members,omitempty"`
}

type NodeLog struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	NodeId      string             `bson:"node_id,omitempty"`
	CurrentTerm uint64             `bson:"current_term,omitempty,default:0"`
	Address     string             `bson:"address,omitempty"`
	VotedFor    string             `bson:"voted_for,omitempty,deafult:''"`
	Config      Configuration      `bson:"config,omitempty"`
	LogEntries  []LogEntry         `bson:"log_entry,omitempty"`
}

type LogEntry struct {
	// The index of the log entry.
	Index uint64 `bson:"index,omitempty"`

	// The term of the log entry.
	Term uint64 `bson:"term,omitempty"`

	// The data of the log entry.
	Data []byte `bson:"data,omitempty"`

	// The type of the log entry.
	EntryType uint32 `bson:"entry_type,omitempty"`
}
