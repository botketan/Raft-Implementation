package mongodb

import (
	"context"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Connect() (*mongo.Client, error) {
	err := godotenv.Load()
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.TODO(),
		30*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("URI")))
	return client, err
}
