package oplog

import (
	"context"
	"log"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.mongodb.org/mongo-driver/bson"
)

var _ = Describe("Dumper", func() {
	log.Println("Dumper test Suite")

	var (
		newOplog *Oplog
		dumper   *Dumper
		docCount int64
		err      error
	)

	newOplog, _ = New()
	err = newOplog.Connect()
	dumper, err = NewDumper(
		context.TODO(),
		newOplog.srcCollections["coll_one"],
		newOplog.dstCollections["coll_one"],
	)
	dumper.buffer.Config.CountThreshold = 20
	dumper.DstCollection.Delete(bson.M{})

	seeder, _ := NewSeeder(100, newOplog.srcCollections["coll_one"])
	seeder.Seed()

	err = dumper.Dump()

	docCount, err = dumper.DstCollection.MongoCollection.CountDocuments(context.TODO(), bson.M{})

	It("ensures dumper is not nil", func() { Expect(dumper).ToNot(BeNil()) })
	It("ensures error is not nil", func() { Expect(err).To(BeNil()) })
	It("ensures docCount is equal to the number of seeded rows", func() { Expect(int(docCount)).To(Equal(100)) })
})
