package oplog

import (
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type (
	Seeder struct {
		Count      int
		Collection *OplogCollection
		seedRows   []bson.M

		ShouldClean bool
	}
)

func NewSeeder(count int, collection *OplogCollection) (seeder *Seeder, err error) {
	seeder = &Seeder{
		Count:       count,
		Collection:  collection,
		ShouldClean: true,
	}
	seeder.GetRowsToSeed()
	seeder.CleanDb()
	return
}

func (seeder *Seeder) CleanDb() (err error) {
	if seeder.ShouldClean {
		seeder.Collection.MongoCollection.DeleteMany(context.TODO(), bson.M{})
	}
	return
}

func (seeder *Seeder) GetRowsToSeed() (err error) {
	seeder.seedRows = append(seeder.seedRows, bson.M{"name": "gary", "age": 30})
	seeder.seedRows = append(seeder.seedRows, bson.M{"name": "ria", "age": 29})
	return
}

func (seeder *Seeder) Seed() (err error) {
	for _, row := range seeder.seedRows {
		for idx := 0; idx < seeder.Count; idx++ {
			if _, err = seeder.Collection.MongoCollection.InsertOne(context.TODO(), row); err != nil {
				log.Println("Seeder error: ", err)
			}
		}
	}
	return
}

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
		newOplog.srcCollections["coll_one"],
	)
	err = seeder.Seed()

	docCount, err = seeder.Collection.MongoCollection.CountDocuments(context.TODO(), bson.M{})

	It("ensures seeder is not nil", func() { Expect(seeder).ToNot(BeNil()) })
	It("ensures error is not nil", func() { Expect(err).To(BeNil()) })
	It("ensures docCount is not equal to the number of seeded rows", func() { Expect(int(docCount)).To(Equal(20)) })
})
