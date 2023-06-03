package oplog

import (
	"encoding/json"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type (
	Message struct {
		ResumeToken   *ResumeToken           `json:"_id"`
		MessageNs     *MessageNs             `json:"ns"`
		OperationType string                 `json:"operationType"`
		FullDocument  map[string]interface{} `json:"fullDocument"`

		rawMessage string
	}
	ResumeToken struct {
		Data string `json:"_data"`
	}
	MessageNs struct {
		DbName         string `json:"db"`
		CollectionName string `json:"coll"`
	}

	MessageN struct {
		CollectionPath string                 `json:"ns"`
		FullDocument   map[string]interface{} `json:"o"`
		Operation      string                 `json:"op"`
		Timestamp      primitive.Timestamp    `json:"ts"`
	}
)

func NewMessage(messageBytes string) (message *Message, err error) {
	message = &Message{}
	if err = json.Unmarshal([]byte(messageBytes), message); err != nil {
		return
	}
	return
}
