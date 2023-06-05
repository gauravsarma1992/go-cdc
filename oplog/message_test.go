package oplog

import (
	"log"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func GetDummyMessage() (message *MessageN) {
	message = &MessageN{
		CollectionPath: "dev.coll_one",
		FullDocument:   bson.M{"name": "gary"},
		OperationType:  InsertOperation,
		Timestamp:      primitive.Timestamp{T: uint32(time.Now().Unix()), I: 1},
	}
	return
}

var _ = Describe("Message", func() {
	log.Println("Message test Suite")

	message := GetDummyMessage()

	It("ensures message is not nil", func() { Expect(message).ToNot(BeNil()) })
	It("ensures message has ns", func() { Expect(message.CollectionPath).To(Equal("dev.coll_one")) })
})
