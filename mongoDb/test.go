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
	fmt.Print(client)
	defer client.Disconnect(context.Background())
	return nil
}
