package oplog

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

var _ = Describe("Watcher", func() {
	log.Println("Watcher test Suite")

	var (
		newOplog   *Oplog
		newWatcher *OplogWatcher
		err        error
	)
	newOplog, _ = New()
	err = newOplog.Connect()
	newWatcher, err = NewOplogWatcher(newOplog.srcDb, newOplog.srcCollections["coll_one"])
	newWatcher.ShouldHonorWatchThreshold = true
	newWatcher.WatchThreshold = 1

	Describe("Watcher initialisation", func() {
		Describe("when default settings are used", func() {
			go func() {
				person := &PersonTest{Name: "Gary"}
				newWatcher.Collection.InsertOne(context.TODO(), person)
			}()
			newWatcher.Run(nil)
			It("ensures no error", func() { Expect(err).To(BeNil()) })
			It("ensures only 1 oplog entry", func() { Expect(newWatcher.WatchCount).To(Equal(1)) })
		})
	})

	Describe("Fetching oplogs", func() {
		var (
			messages []*MessageN
		)
		messages, err = newWatcher.FetchFromOplog()
		It("ensures no error", func() { Expect(err).To(BeNil()) })
		It("ensures message length", func() { Expect(len(messages)).To(BeNumerically(">", 1)) })
	})
})
