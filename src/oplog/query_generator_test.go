package oplog

import (
	"log"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("QueryGenerator", func() {
	log.Println("QueryGenerator test Suite")

	message := GetDummyMessage()
	newOplog, _ := New()
	newOplog.Connect()
	newQueryGen, err := NewQueryGenerator(newOplog.dstCollections["coll_one"])

	Describe("starting query lifecycle", func() {
		err = newQueryGen.Process(message)
		It("ensures no error", func() { Expect(err).To(BeNil()) })

		message.OperationType = "u"
		message.FullDocument["age"] = "32"
		err = newQueryGen.Process(message)
		It("ensures no error", func() { Expect(err).To(BeNil()) })

		message.OperationType = "d"
		err = newQueryGen.Process(message)
		It("ensures no error", func() { Expect(err).To(BeNil()) })
	})
})
