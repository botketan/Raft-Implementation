package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

func Voted(NodeId string, VotedFor string, CurrentTerm int64) error {
	client, err := connect()
	if err != nil {
		return fmt.Errorf("error while connecting to mongoDB: %w", err)
	}
	defer client.Disconnect(context.Background())

	db := client.Database("raft")
	Collection := db.Collection("NodeLog")
	_, err = Collection.UpdateOne(context.TODO(),
		bson.M{"node_id": NodeId},
		bson.M{"$set": bson.M{"voted_for": VotedFor, "current_term": CurrentTerm}})
	return err
}

func GetNodeLog(NodeId string) (NodeLog, error) {
	client, err := connect()
	if err != nil {
		return NodeLog{}, fmt.Errorf("error while connecting to mongoDB: %w", err)
	}
	defer client.Disconnect(context.Background())

	db := client.Database("raft")
	Collection := db.Collection("NodeLog")
	var node NodeLog
	err = Collection.FindOne(context.TODO(), bson.M{"node_id": NodeId}).Decode(&node)
	return node, err
}
