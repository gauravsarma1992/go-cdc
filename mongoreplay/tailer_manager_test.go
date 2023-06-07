package mongoreplay

import (
	"context"
	"log"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func GetTestTailerManager() (newTailerMgr *TailerManager, err error) {
	var (
		newOplog      *Oplog
		stageExecutor StageExecutor
	)
	newOplog, _ = New()
	err = newOplog.Connect()
	stageExecutor, err = NewTailerManager(
		context.TODO(),
		newOplog.SrcCollections["coll_one"],
		newOplog.DstCollections["coll_one"],
	)
	newTailerMgr = stageExecutor.(*TailerManager)
	newTailerMgr.tailer.WatchThreshold = 2
	newTailerMgr.tailer.ShouldHonorWatchThreshold = true
	newTailerMgr.tailer.FetchCountThreshold = 2

	return
}

var _ = Describe("TailerManager", func() {
	log.Println("TailerManager test Suite")

	Describe("TailerManager loop", func() {
		var (
			newTailerMgr *TailerManager
			err          error
		)
		newTailerMgr, err = GetTestTailerManager()

		go func() {
			time.Sleep(1 * time.Second)
			person := &PersonTest{Name: "Gary"}
			if _, err = newTailerMgr.tailer.Collection.MongoCollection.InsertOne(context.TODO(), person); err != nil {
				log.Println(err)
			}
			time.Sleep(1 * time.Second)
			if _, err = newTailerMgr.tailer.Collection.MongoCollection.InsertOne(context.TODO(), person); err != nil {
				log.Println(err)
			}
		}()
		newTailerMgr.Run()

		It("ensures no error", func() { Expect(err).To(BeNil()) })
		It("checks buffer's length", func() { Expect(newTailerMgr.buffer.Length()).To(BeNumerically(">=", 2)) })
	})

})
