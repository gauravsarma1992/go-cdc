package oplog

import (
	"log"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBuffer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Buffer test Suite")
}

var _ = Describe("Buffer", func() {
	log.Println("Buffer test Suite")
	Describe("Buffer Initialisation", func() {
		buffer, err := NewBuffer(LogFlusherFunc)
		It("ensures no error", func() { Expect(err).To(BeNil()) })
		It("ensures buffer is not nil", func() { Expect(buffer).ToNot(BeNil()) })
		It("ensures default TimeInSecsThreshold", func() { Expect(buffer.Config.TimeInSecsThreshold).To(Equal(5)) })
		It("ensures default CountThreshold", func() { Expect(buffer.Config.CountThreshold).To(Equal(1000)) })
	})
	Describe("Buffer Store", func() {
		buffer, err := NewBuffer(LogFlusherFunc)
		err = buffer.Store(`{"some": "event"}`)
		It("ensures no error", func() { Expect(err).To(BeNil()) })
		It("ensures buffer length", func() { Expect(len(buffer.store)).To(Equal(1)) })
	})
	Describe("Buffer Flush", func() {
		Context("when flushing one event", func() {
			buffer, err := NewBuffer(LogFlusherFunc)
			err = buffer.Store(`{"some": "event"}`)
			err = buffer.Flush()
			It("ensures no error", func() { Expect(err).To(BeNil()) })
			It("ensures buffer flush", func() { Expect(buffer.CurrFlushIdx).To(Equal(1)) })
		})

		Context("when flushing 3 more event", func() {
			buffer, err := NewBuffer(LogFlusherFunc)
			err = buffer.Store(`{"some": "event"}`)
			err = buffer.Store(`{"some": "event"}`)
			err = buffer.Store(`{"some": "event"}`)
			err = buffer.Flush()
			It("ensures no error", func() { Expect(err).To(BeNil()) })
			It("ensures buffer flush", func() { Expect(buffer.CurrFlushIdx).To(Equal(3)) })
		})
	})
	Describe("Buffer ShouldFlush", func() {
		Context("when time passed is more than threshold", func() {
			buffer, _ := NewBuffer(LogFlusherFunc)
			buffer.LastFlushedAt = time.Now().Add(-5 * time.Second)
			It("ensures should flush", func() { Expect(buffer.ShouldFlush()).To(Equal(true)) })
		})
		Context("when time passed is not more than threshold", func() {
			buffer, _ := NewBuffer(LogFlusherFunc)
			buffer.LastFlushedAt = time.Now()
			It("ensures should flush", func() { Expect(buffer.ShouldFlush()).To(Equal(false)) })
		})
		Context("when count is not more than threshold", func() {
			buffer, _ := NewBuffer(LogFlusherFunc)
			buffer.Config.CountThreshold = 5
			buffer.Store(`{"some": "event"}`)
			buffer.Store(`{"some": "event"}`)
			buffer.Store(`{"some": "event"}`)
			It("ensures should flush", func() { Expect(buffer.ShouldFlush()).To(Equal(false)) })
		})
		Context("when count is not more than threshold", func() {
			buffer, _ := NewBuffer(LogFlusherFunc)
			buffer.Config.CountThreshold = 5
			buffer.Store(`{"some": "event"}`)
			buffer.Store(`{"some": "event"}`)
			buffer.Store(`{"some": "event"}`)
			buffer.Store(`{"some": "event"}`)
			buffer.Store(`{"some": "event"}`)
			It("ensures should flush", func() { Expect(buffer.ShouldFlush()).To(Equal(true)) })
		})
	})
})
