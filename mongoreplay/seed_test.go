package mongoreplay

import (
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Seeder", func() {
	log.Println("Seeder test Suite")

	var (
		newOplog *Oplog
		seeder   *Seeder
		docCount int64
		err      error
	)

	newOplog, _ = New()
	err = newOplog.Connect()
	seeder, err = NewSeeder(
		10,
		newOplog.DstCollections["coll_one"],
	)
	err = seeder.Seed()

	docCount, err = seeder.Collection.MongoCollection.CountDocuments(context.TODO(), bson.M{})

	It("ensures seeder is not nil", func() { Expect(seeder).ToNot(BeNil()) })
	It("ensures error is not nil", func() { Expect(err).To(BeNil()) })
	It("ensures docCount is equal to the number of seeded rows", func() { Expect(int(docCount)).To(Equal(20)) })
})
