package mongodb

import (
	"context"
	"fmt"
)

func TestConnect() {
	client, err := connect()
	if err != nil {
		panic(err)
	}
	fmt.Print(client)
	defer client.Disconnect(context.Background())
}
