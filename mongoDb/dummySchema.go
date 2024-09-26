package mongodb

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Dummy struct {
	ID   primitive.ObjectID `bson:"_id,omitempty"`
	Data string             `bson:"data,omitempty"`
}
