package mongodb

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type NodeLog struct {
	ID         primitive.ObjectID `bson:"_id,omitempty"`
	Port       string             `bson:"port,omitempty"`
	LogEntries []LogEntry         `bson:"log_entry,omitempty"`
}

type LogEntry struct {
	// The index of the log entry.
	Index uint64 `bson:"index,omitempty"`

	// The term of the log entry.
	Term uint64 `bson:"term,omitempty"`

	// The offset of the log entry.
	Offset int64 `bson:"offset,omitempty"`

	// The data of the log entry.
	Data []byte `bson:"data,omitempty"`

	// The type of the log entry.
	EntryType uint32 `bson:"entry_type,omitempty"`
}
