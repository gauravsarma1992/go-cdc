package mongoreplay

import (
	"log"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Oplog", func() {
	log.Println("Oplog test Suite")

	Describe("Oplog initialisation", func() {
		var (
			newOplog *Oplog
			err      error
		)

		BeforeEach(func() {
			newOplog, err = New()
		})

		Context("when default settings are used", func() {
			It("initialises the number of workers", func() { Expect(newOplog.noOfWorkers).To(Equal(uint8(4))) })
			It("ensures no error", func() { Expect(err).To(BeNil()) })
			It("sets host to localhost", func() { Expect(newOplog.sourceMongoConfig.Host).To(Equal("mongodb")) })
			It("sets port to default port", func() { Expect(newOplog.sourceMongoConfig.Port).To(Equal("27017")) })
		})
	})

	Describe("Mongo Config", func() {
		sourceMongoConfig, err := NewMongoConfig(DefaultSourceMongoConfigFile)
		mongoUrl := sourceMongoConfig.GetUrl()
		Context("when fetching URL", func() {
			It("ensures no error", func() { Expect(err).To(BeNil()) })
			It("sets host to localhost", func() { Expect(sourceMongoConfig.Host).To(Equal("mongodb")) })
			It("sets URL properly", func() { Expect(mongoUrl).To(Equal("mongodb://mongodb:27017/dev/?replicaSet=dbrs")) })
		})
	})

	Describe("Connecting to Mongo", func() {
		newOplog, err := New()
		err = newOplog.Connect()
		Context("when connecting to mongo", func() {
			It("ensures no error", func() { Expect(err).To(BeNil()) })
			It("ensures db name is set", func() { Expect(newOplog.srcDb.Name()).To(Equal("dev")) })
			It("ensures collections are provided", func() { Expect(len(newOplog.srcCollections)).ToNot(Equal(0)) })
		})
	})

	Describe("Running the oplog loop", func() {
		newOplog, err := New()
		go func() {
			time.Sleep(2 * time.Second)
			newOplog.closeCh <- true
		}()
		err = newOplog.Run()
		Context("when running the oplog loop", func() {
			It("ensures no error", func() { Expect(err).To(BeNil()) })
		})
	})

})
