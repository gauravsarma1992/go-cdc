package mongoreplay

import (
	"context"
	"log"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func GetDummyBuffer(countThreshold int) (buffer *Buffer) {
	buffer, _ = NewBuffer(context.TODO(), LogFlusherFunc)
	buffer.Config.CountThreshold = countThreshold
	return
}

func FillBuffer(count int, buffer *Buffer) (err error) {
	for idx := 0; idx < count; idx++ {
		if err = buffer.Store(GetDummyMessage()); err != nil {
			return
		}
	}
	return
}

var _ = Describe("Buffer", func() {
	log.Println("Buffer test Suite")
	Describe("Buffer Initialisation", func() {
		buffer, _ := NewBuffer(context.TODO(), LogFlusherFunc)
		It("should ensure the default configurations", func() {
			Expect(buffer).ToNot(BeNil())
			Expect(buffer.Config.CountThreshold).To(Equal(3000))
			Expect(buffer.Config.RolloverThreshold).To(Equal(10000))
			Expect(buffer.Config.TimeInSecsThreshold).To(Equal(5))
		})
	})
	Describe("Buffer Store", func() {
		Context("When store is not full", func() {
		})
		Context("When store is full", func() {
		})
	})
	Describe("Buffer Flush", func() {
		Context("when flushing one event", func() {
		})
		Context("when flushing 3 more event", func() {
		})
		Context("when flushing entire buffer", func() {
		})
	})
	Describe("Buffer ShouldFlush", func() {
		Context("when time passed is more than threshold", func() {
			buffer := GetDummyBuffer(1000)
			buffer.LastFlushedAt = time.Now().Add(-5 * time.Second)
			It("ensures should flush", func() { Expect(buffer.ShouldFlush()).To(Equal(true)) })
		})
		Context("when time passed is not more than threshold", func() {
			buffer := GetDummyBuffer(1000)
			buffer.LastFlushedAt = time.Now()
			It("ensures should flush", func() { Expect(buffer.ShouldFlush()).To(Equal(false)) })
		})
		Context("when count is not more than threshold", func() {
			buffer := GetDummyBuffer(1000)
			buffer.Config.CountThreshold = 5
			FillBuffer(3, buffer)
			It("ensures should flush", func() { Expect(buffer.ShouldFlush()).To(Equal(false)) })
		})
		Context("when count is more than threshold", func() {
			buffer := GetDummyBuffer(1000)
			buffer.Config.CountThreshold = 5
			buffer.Config.RolloverThreshold = 5
			FillBuffer(5, buffer)
			It("ensures should flush", func() { Expect(buffer.ShouldFlush()).To(Equal(true)) })
		})
	})
})
