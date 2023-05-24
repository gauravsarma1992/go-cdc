package oplog

import (
	"log"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestQueryGenerator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "QueryGenerator test Suite")
}

var _ = Describe("QueryGenerator", func() {
	log.Println("QueryGenerator test Suite")

	jsonString := `{"_id": {"_data": "8264686464000000012B022C0100296E5A1004BCD9AB31007A419F8944F0453F737AA846645F6964006464686464B3E1A361D907357A0004"},"operationType": "insert","clusterTime": {"$timestamp":{"t":1684563044,"i":1}},"fullDocument": {"_id": "64686464b3e1a361d907357a","name": "Gary"},"ns": {"db": "dev","coll": "coll_one"},"documentKey": {"_id": {"$oid":"64686464b3e1a361d907357a"}}}`
	message, _ := NewMessage(jsonString)
	newOplog, _ := New()
	newOplog.Connect()
	newQueryGen, err := NewQueryGenerator(newOplog.dstCollections["coll_one"])

	Describe("starting query lifecycle", func() {
		err = newQueryGen.Process(message)
		It("ensures no error", func() { Expect(err).To(BeNil()) })

		message.OperationType = "update"
		message.FullDocument["age"] = "32"
		err = newQueryGen.Process(message)
		It("ensures no error", func() { Expect(err).To(BeNil()) })

		message.OperationType = "delete"
		err = newQueryGen.Process(message)
		It("ensures no error", func() { Expect(err).To(BeNil()) })
	})
})
