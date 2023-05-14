package oplog

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBooks(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Oplog test Suite")
}

var _ = Describe("Oplog", func() {
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
			It("sets host to localhost", func() { Expect(newOplog.mongoConfig.Host).To(Equal("localhost")) })
			It("sets port to default port", func() { Expect(newOplog.mongoConfig.Port).To(Equal("27017")) })
		})
	})

	Describe("Mongo Config", func() {
		mongoConfig, err := NewMongoConfig()
		mongoUrl := mongoConfig.GetUrl()
		Context("when fetching URL", func() {
			It("ensures no error", func() { Expect(err).To(BeNil()) })
			It("sets host to localhost", func() { Expect(mongoConfig.Host).To(Equal("localhost")) })
			It("sets URL properly", func() { Expect(mongoUrl).To(Equal("mongodb://localhost:27017")) })
		})
	})

	Describe("Connecting to Mongo", func() {
		newOplog, err := New()
		err = newOplog.Connect()
		Context("when connecting to mongo", func() {
			It("ensures no error", func() { Expect(err).To(BeNil()) })
			It("ensures db name is set", func() { Expect(newOplog.db.Name()).To(Equal("dev")) })
			It("ensures collections are provided", func() { Expect(len(newOplog.collections)).ToNot(Equal(0)) })
		})
	})

	Describe("Running the oplog loop", func() {
		newOplog, err := New()
		err = newOplog.Run()
		Context("when running the oplog loop", func() {
			It("ensures no error", func() { Expect(err).To(BeNil()) })
		})
	})

})
