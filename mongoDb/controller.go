package mongodb

import (
	"context"
	"fmt"

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

func AddLog(client mongo.Client, NodeId string, term int64, index int64, data []byte) error {
	db := client.Database("raft")
	Collection := db.Collection("NodeLog")
	_, err := Collection.UpdateOne(context.TODO(),
		bson.M{"node_id": NodeId},
		bson.M{"$push": bson.M{"log_entry": bson.M{"index": index,
			"term":       term,
			"data":       data,
			"entry_type": 1}}})
	return err
}

func ChangeLog(client mongo.Client, NodeId string, logindex int64, term int64, index int64, data []byte) error {
	db := client.Database("raft")
	Collection := db.Collection("NodeLog")
	_, err := Collection.UpdateOne(context.TODO(),
		bson.M{"node_id": NodeId},
		bson.M{"$set": bson.M{("log_entry." + fmt.Sprint(logindex) + ".data"): data,
			("log_entry." + fmt.Sprint(logindex) + ".term"):  term,
			("log_entry." + fmt.Sprint(logindex) + ".index"): index}})
	return err
}

func TrimLog(client mongo.Client, NodeId string, logindex int64) error {
	db := client.Database("raft")
	Collection := db.Collection("NodeLog")
	result, err := Collection.UpdateOne(context.TODO(),
		bson.M{"node_id": NodeId},
		bson.M{"$set": bson.M{"log_entry": bson.M{
			"$slice": bson.A{"$log_entry", logindex}}}})
	fmt.Println(result)
	return err
}
