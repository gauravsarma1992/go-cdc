package oplog

import (
	"log"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMessage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Message test Suite")
}

var _ = Describe("Message", func() {
	log.Println("Message test Suite")

	jsonString := `{"_id": {"_data": "8264686464000000012B022C0100296E5A1004BCD9AB31007A419F8944F0453F737AA846645F6964006464686464B3E1A361D907357A0004"},"operationType": "insert","clusterTime": {"$timestamp":{"t":1684563044,"i":1}},"fullDocument": {"_id": {"$oid":"64686464b3e1a361d907357a"},"name": "Gary"},"ns": {"db": "dev","coll": "coll_one"},"documentKey": {"_id": {"$oid":"64686464b3e1a361d907357a"}}}`
	message, err := NewMessage(jsonString)

	It("ensures no error", func() { Expect(err).To(BeNil()) })
	It("ensures message is not nil", func() { Expect(message).ToNot(BeNil()) })
	It("ensures message has resume token", func() {
		Expect(message.ResumeToken.Data).To(Equal("8264686464000000012B022C0100296E5A1004BCD9AB31007A419F8944F0453F737AA846645F6964006464686464B3E1A361D907357A0004"))
	})
	It("ensures message has ns", func() { Expect(message.MessageNs.DbName).To(Equal("dev")) })
})
