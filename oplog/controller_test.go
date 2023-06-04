package oplog

import (
	"context"
	"log"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func GetTestController() (newCtrlr *Controller, err error) {
	var (
		newOplog *Oplog
	)
	newOplog, _ = New()
	err = newOplog.Connect()
	newCtrlr, err = NewController(
		context.TODO(),
		newOplog.srcDb,
		newOplog.srcCollections["coll_one"],
		newOplog.dstDb,
		newOplog.dstCollections["coll_one"],
	)
	newCtrlr.watcher.WatchThreshold = 2
	newCtrlr.watcher.ShouldHonorWatchThreshold = true
	newCtrlr.watcher.FetchCountThreshold = 2

	return
}

var _ = Describe("Controller", func() {
	log.Println("Controller test Suite")

	Describe("Controller loop", func() {
		var (
			newCtrlr *Controller
			err      error
		)
		newCtrlr, err = GetTestController()

		go func() {
			time.Sleep(1 * time.Second)
			person := &PersonTest{Name: "Gary"}
			if _, err = newCtrlr.watcher.Collection.MongoCollection.InsertOne(context.TODO(), person); err != nil {
				log.Println(err)
			}
			time.Sleep(1 * time.Second)
			if _, err = newCtrlr.watcher.Collection.MongoCollection.InsertOne(context.TODO(), person); err != nil {
				log.Println(err)
			}
		}()
		newCtrlr.Run()

		It("ensures no error", func() { Expect(err).To(BeNil()) })
		It("checks buffer's length", func() { Expect(newCtrlr.buffer.Length()).To(BeNumerically(">=", 2)) })
	})

})
