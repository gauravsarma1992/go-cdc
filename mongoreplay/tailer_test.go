package mongoreplay

import (
	"context"
	"log"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type (
	PersonTest struct {
		Name string `json:"name"`
	}
)

var _ = Describe("Tailer", func() {
	log.Println("Tailer test Suite")

	var (
		newOplog      *Oplog
		newTailer     *OplogTailer
		stageExecutor StageExecutor
		err           error
	)
	newOplog, _ = New()
	err = newOplog.Connect()
	stageExecutor, err = NewOplogTailer(context.TODO(), newOplog.SrcCollections["coll_one"], newOplog.DstCollections["coll_one"])
	newTailer = stageExecutor.(*OplogTailer)
	newTailer.ShouldHonorWatchThreshold = true
	newTailer.WatchThreshold = 1
	newTailer.FetchCountThreshold = 1

	Describe("Tailer initialisation", func() {
		Describe("when default settings are used", func() {
			go func() {
				person := &PersonTest{Name: "Gary"}
				newTailer.Collection.MongoCollection.InsertOne(context.TODO(), person)
			}()
			newTailer.Run(&ResumeTokenStore{})
			It("ensures no error", func() { Expect(err).To(BeNil()) })
			It("ensures only 1 oplog entry", func() { Expect(newTailer.WatchCount).To(Equal(1)) })
		})
	})

	Describe("Fetching oplogs", func() {
		var (
			messages []*MessageN
		)
		messages, err = newTailer.FetchFromOplog(&ResumeTokenStore{})
		It("ensures no error", func() { Expect(err).To(BeNil()) })
		It("ensures message length", func() { Expect(len(messages)).To(BeNumerically("==", 1)) })
	})
})
