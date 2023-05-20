package oplog

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type (
	PersonTest struct {
		Name string `json:"name"`
	}
)

func TestWatcher(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Watcher test Suite")
}

var _ = Describe("Watcher", func() {
	Describe("Watcher initialisation", func() {
		var (
			newOplog   *Oplog
			newWatcher *OplogWatcher
			err        error
		)
		newOplog, _ = New()
		err = newOplog.Connect()
		newWatcher, err = NewOplogWatcher(newOplog.db, newOplog.collections[0])
		newWatcher.ShouldHonorWatchThreshold = true
		newWatcher.WatchThreshold = 1

		Describe("when default settings are used", func() {
			go func() {
				person := &PersonTest{Name: "Gary"}
				newWatcher.Collection.InsertOne(context.TODO(), person)
			}()
			newWatcher.Run()
			It("ensures no error", func() { Expect(err).To(BeNil()) })
			It("ensures only 1 oplog entry", func() { Expect(newWatcher.WatchCount).To(Equal(1)) })
		})

	})
})
