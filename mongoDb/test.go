package mongodb

import (
	"context"
	"fmt"
)

func TestConnect() error {
	client, err := connect()
	if err != nil {
		return fmt.Errorf("error while connecting to mongoDB: %w", err)
	}
	// fmt.Print(client)
	defer client.Disconnect(context.Background())
	db := client.Database("raft")
	dummyCollection := db.Collection("dummy")
	newData := Dummy{
		Data: "Dummy data",
	}
	insertResult, err := dummyCollection.InsertOne(context.TODO(), newData)
	if err != nil {
		return fmt.Errorf("error while connecting to mongoDB: %w", err)
	}
	fmt.Println(insertResult)
	NodelogCollection := db.Collection("NodeLog")
	newNode := NodeLog{
		LogEntries: []LogEntry{
			{
				Index: 2,
				Term:  2,
			},
			{
				Index: 1,
				Term:  1,
			},
		},
	}
	insertResult, err = NodelogCollection.InsertOne(context.TODO(), newNode)
	if err != nil {
		return fmt.Errorf("error while connecting to mongoDB: %w", err)
	}
	fmt.Println(insertResult)
	return nil
}
