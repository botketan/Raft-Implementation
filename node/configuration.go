package node

import (
	"fmt"
	"strings"

	pb "raft/protos"

	"google.golang.org/protobuf/proto"
)

// Configuration represents a cluster of nodes.
type Configuration struct {
	// All members of the cluster. Maps node ID to address.
	Members map[string]string

	// The log index of the configuration.
	Index int64
}

// NewConfiguration creates a new configuration with the provided
// members and index. By default, all members in the returned configuration
// will have voter status.
func NewConfiguration(index int64, members map[string]string) *Configuration {
	configuration := &Configuration{
		Index:   index,
		Members: members,
	}
	return configuration
}

// Clone creates a deep-copy of the configuration.
func (c *Configuration) Clone() Configuration {
	configuration := Configuration{
		Index:   c.Index,
		Members: make(map[string]string, len(c.Members)),
	}

	for id := range c.Members {
		configuration.Members[id] = c.Members[id]
	}

	return configuration
}

// protoToConfiguration converts a protobuf configuration to a Configuration.
func protoToConfiguration(pbConfiguration *pb.Configuration) Configuration {
	configuration := Configuration{
		Index:   pbConfiguration.GetLogIndex(),
		Members: pbConfiguration.GetMembers(),
	}
	return configuration
}

// ToProto converts the configuration to a protobuf message.
func (c *Configuration) ToProto() *pb.Configuration {
	return &pb.Configuration{
		Members:  c.Members,
		LogIndex: c.Index,
	}
}

// String returns a string representation of the configuration.
func (c *Configuration) String() string {
	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("logIndex: %d members: ", c.Index))
	return fmt.Sprintf("{%s}", strings.TrimSuffix(builder.String(), ","))
}

func encodeConfiguration(configuration *Configuration) ([]byte, error) {
	pbConfiguration := &pb.Configuration{
		Members:  configuration.Members,
		LogIndex: configuration.Index,
	}
	data, err := proto.Marshal(pbConfiguration)
	if err != nil {
		return nil, fmt.Errorf("could not marshal protobuf message: %w", err)
	}
	return data, nil
}

func decodeConfiguration(data []byte) (Configuration, error) {
	pbConfiguration := &pb.Configuration{}
	if err := proto.Unmarshal(data, pbConfiguration); err != nil {
		return Configuration{}, fmt.Errorf("could not unmarshal protobuf message: %w", err)
	}
	configuration := Configuration{
		Members: pbConfiguration.GetMembers(),
		Index:   pbConfiguration.GetLogIndex(),
	}
	return configuration, nil
}

type configManager struct {
	// Maps log index associated with the configuration to its response channel.
	pendingReplicated map[int64]chan Result[Configuration]
}

func newConfigManager() *configManager {
	return &configManager{
		pendingReplicated: make(map[int64]chan Result[Configuration]),
	}
}
