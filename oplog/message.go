package oplog

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type (
	MessageN struct {
		CollectionPath string                 `json:"ns"`
		FullDocument   map[string]interface{} `json:"o"`
		OperationType  string                 `json:"op"`
		Timestamp      primitive.Timestamp    `json:"ts"`
	}
)
