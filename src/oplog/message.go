package oplog

import (
	"encoding/json"
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
		Db             string `json:"db"`
		CollectionName string `json:"coll"`
	}
)

func NewMessage(messageBytes string) (message *Message, err error) {
	message = &Message{}
	if err = json.Unmarshal([]byte(messageBytes), message); err != nil {
		return
	}
	return
}
