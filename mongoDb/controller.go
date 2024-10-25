package mongoDb

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

func AddLog(client mongo.Client, NodeId string, term int64, index int64, data []byte, seqNo int64, clientID string, entryType LogEntryType) error {
	db := client.Database("raft")
	Collection := db.Collection("NodeLog")
	_, err := Collection.UpdateOne(context.TODO(),
		bson.M{"node_id": NodeId},
		bson.M{"$push": bson.M{"log_entry": bson.M{"index": index,
			"term":       term,
			"data":       data,
			"seq_no":     seqNo,
			"client_id":  clientID,
			"entry_type": entryType}}})
	return err
}

func ChangeLog(client mongo.Client, NodeId string, logindex int64, term int64, index int64, data []byte, seqNo int64, clientID string, entryType LogEntryType) error {
	db := client.Database("raft")
	Collection := db.Collection("NodeLog")
	_, err := Collection.UpdateOne(context.TODO(),
		bson.M{"node_id": NodeId},
		bson.M{"$set": bson.M{("log_entry." + fmt.Sprint(logindex) + ".data"): data,
			("log_entry." + fmt.Sprint(logindex) + ".term"):       term,
			("log_entry." + fmt.Sprint(logindex) + ".index"):      index,
			("log_entry." + fmt.Sprint(logindex) + ".seq_no"):     seqNo,
			("log_entry." + fmt.Sprint(logindex) + ".client_id"):  clientID,
			("log_entry." + fmt.Sprint(logindex) + ".entry_type"): entryType,
		}})
	return err
}

func TrimLog(client mongo.Client, NodeId string, logindex int64) error {
	db := client.Database("raft")
	Collection := db.Collection("NodeLog")
	_, err := Collection.UpdateOne(context.TODO(),
		bson.M{"node_id": NodeId},
		[]bson.M{{"$set": bson.M{"log_entry": bson.M{
			"$slice": bson.A{"$log_entry", logindex}}}}})
	return err
}

func GetClient(client mongo.Client, ClientId string) Client {
	db := client.Database("raft")
	Collection := db.Collection("Client")
	ct := Collection.FindOne(context.Background(), bson.M{
		"client_id": ClientId,
	})
	var ret Client
	ct.Decode(&ret)
	return ret
}

func UpdateClient(client mongo.Client, ClientId string, seqNo int64) error {
	db := client.Database("raft")
	Collection := db.Collection("Client")
	_, err := Collection.UpdateOne(context.Background(), bson.M{
		"client_id": ClientId,
	},
		bson.M{
			"$set": bson.M{
				"seq_no": seqNo,
			},
		}, options.Update().SetUpsert(true))
	return err
}
