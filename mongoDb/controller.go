package mongodb

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Voted(client mongo.Client, NodeId string, VotedFor string, CurrentTerm int64) error {
	db := client.Database("raft")
	Collection := db.Collection("NodeLog")
	_, err := Collection.UpdateOne(context.TODO(),
		bson.M{"node_id": NodeId},
		bson.M{"$set": bson.M{"voted_for": VotedFor, "current_term": CurrentTerm}},
		options.Update().SetUpsert(true))

	return err
}

func GetNodeLog(client mongo.Client, NodeId string) (NodeLog, error) {
	db := client.Database("raft")
	Collection := db.Collection("NodeLog")
	var node NodeLog
	err := Collection.FindOne(context.TODO(), bson.M{"node_id": NodeId}).Decode(&node)
	return node, err
}
