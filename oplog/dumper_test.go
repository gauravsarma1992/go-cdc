package oplog

import (
	"context"
	"log"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Dumper", func() {
	log.Println("Dumper test Suite")

	var (
		newOplog *Oplog
		dumper   *Dumper
		err      error
	)

	newOplog, _ = New()
	err = newOplog.Connect()
	dumper, err = NewDumper(
		context.TODO(),
		newOplog.srcCollections["coll_one"],
		newOplog.dstCollections["coll_one"],
	)

	It("ensures dumper is not nil", func() { Expect(dumper).ToNot(BeNil()) })
	It("ensures error is not nil", func() { Expect(err).To(BeNil()) })
})
