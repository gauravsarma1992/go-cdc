package mongoreplay

import (
	"context"
	"log"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("QueryGenerator", func() {
	log.Println("QueryGenerator test Suite")

	message := GetDummyMessage()
	newOplog, _ := New()
	newOplog.Connect()
	newQueryGen, err := NewQueryGenerator(context.TODO(), newOplog.DstCollections["coll_one"].MongoCollection)

	Describe("starting query lifecycle", func() {
		err = newQueryGen.Process(message)
		It("ensures no error", func() { Expect(err).To(BeNil()) })

		message.OperationType = InsertOperation
		message.FullDocument["age"] = "32"
		err = newQueryGen.Process(message)
		It("ensures no error", func() { Expect(err).To(BeNil()) })

		message.OperationType = UpdateOperation
		err = newQueryGen.Process(message)
		It("ensures no error", func() { Expect(err).To(BeNil()) })
	})
})
