package mongoreplay

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type (
	OperationTypeT string
	MessageN       struct {
		CollectionPath string                 `json:"ns"`
		FullDocument   map[string]interface{} `json:"o"`
		OperationType  OperationTypeT         `json:"op"`
		Timestamp      primitive.Timestamp    `json:"ts"`
	}
)

var (
	InsertOperation OperationTypeT = "i"
	UpdateOperation OperationTypeT = "u"
	DeleteOperation OperationTypeT = "d"
)
