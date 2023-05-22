package oplog

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestController(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controller test Suite")
}

var _ = Describe("Controller", func() {
	var (
		newOplog *Oplog
		newCtrlr *Controller
		err      error
	)
	newOplog, _ = New()
	err = newOplog.Connect()
	newCtrlr, err = NewController(newOplog.db, newOplog.collections[0])
	newCtrlr.watcher.WatchThreshold = 2
	newCtrlr.watcher.ShouldHonorWatchThreshold = true

	go func() {
		time.Sleep(1 * time.Second)
		person := &PersonTest{Name: "Gary"}
		newCtrlr.watcher.Collection.InsertOne(context.TODO(), person)
		newCtrlr.watcher.Collection.InsertOne(context.TODO(), person)
	}()
	newCtrlr.Run()

	It("ensures no error", func() { Expect(err).To(BeNil()) })
	It("checks buffer's length", func() { Expect(newCtrlr.buffer.Length()).To(Equal(2)) })
})
