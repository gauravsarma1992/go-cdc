package mongoreplay

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"time"
)

const (
	DefaultBufferConfigFile = "./config/buffer_config.json"
)

type (
	FlusherFunc func([]*MessageN) error
	Buffer      struct {
		Ctx context.Context

		store         []*MessageN
		LastFlushedAt time.Time

		StartIdx int
		StopIdx  int

		Config  *BufferConfig
		Flusher FlusherFunc
	}
	BufferConfig struct {
		CountThreshold      int `json:"count_threshold"`
		RolloverThreshold   int `json:"rollover_threshold"`
		TimeInSecsThreshold int `json:"time_in_secs_threshold"`
	}
)

func NewBuffer(ctx context.Context, flusherFunc FlusherFunc) (buffer *Buffer, err error) {
	buffer = &Buffer{
		Ctx:           ctx,
		Flusher:       flusherFunc,
		LastFlushedAt: time.Now(),
	}
	if buffer.Config, err = NewBufferConfig(); err != nil {
		return
	}
	return
}

func LogFlusherFunc(events []*MessageN) (err error) {
	return
}

func NewBufferConfig() (bufferConfig *BufferConfig, err error) {
	var (
		fileB []byte
	)
	bufferConfig = &BufferConfig{}
	fileB, _ = ioutil.ReadFile(DefaultBufferConfigFile)
	json.Unmarshal(fileB, bufferConfig)

	if bufferConfig.CountThreshold == 0 {
		bufferConfig.CountThreshold = 3000
	}
	if bufferConfig.TimeInSecsThreshold == 0 {
		bufferConfig.TimeInSecsThreshold = 5
	}
	if bufferConfig.RolloverThreshold == 0 {
		bufferConfig.RolloverThreshold = 10000
	}
	return
}

func (buffer *Buffer) Length() (count int) {
	count = buffer.StopIdx - buffer.StartIdx
	return
}

func (buffer *Buffer) IsFull() (isFull bool) {
	if buffer.Length() >= buffer.Config.RolloverThreshold {
		isFull = true
	}
	return
}

func (buffer *Buffer) Store(event *MessageN) (err error) {
	if buffer.IsFull() {
		err = errors.New("[Buffer] Buffer is full")
		return
	}
	buffer.store = append(buffer.store, event)
	buffer.StopIdx += 1
	return
}

func (buffer *Buffer) ShouldFlush() (shouldFlush bool) {
	timePassed := time.Since(buffer.LastFlushedAt)
	if int(timePassed.Seconds()) > buffer.Config.TimeInSecsThreshold {
		shouldFlush = true
		return
	}
	if buffer.Length() >= buffer.Config.RolloverThreshold {
		shouldFlush = true
		return
	}
	return
}

func (buffer *Buffer) Rollover() (err error) {
	if len(buffer.store) < buffer.Config.RolloverThreshold {
		return
	}
	log.Println("[Buffer] Rollover", len(buffer.store))

	buffer.store = make([]*MessageN, 0)
	buffer.LastFlushedAt = time.Now()
	buffer.StartIdx = 0
	buffer.StopIdx = 0
	return
}

func (buffer *Buffer) GetEvents() (events []*MessageN, err error) {
	for idx := buffer.StartIdx; idx < buffer.StopIdx; idx++ {
		if idx >= len(buffer.store) {
			break
		}
		events = append(events, buffer.store[idx])
	}
	return
}

func (buffer *Buffer) AfterFlush(eventsLength int) (err error) {

	buffer.StartIdx = eventsLength + 1
	buffer.StopIdx = eventsLength + 1
	buffer.LastFlushedAt = time.Now()

	log.Println("[Buffer] Total events remaining:", buffer.Length())

	buffer.Rollover()
	return
}

func (buffer *Buffer) Flush() (lastFlushedResumeToken *ResumeTokenStore, err error) {
	var (
		events []*MessageN
	)
	if buffer.Length() <= 0 {
		return
	}
	log.Println("[Buffer] Trying to flush", "events", "from", buffer.StartIdx, "to", buffer.StopIdx)

	if events, err = buffer.GetEvents(); err != nil {
		return
	}
	log.Println("[Buffer] Total events fetched to flush", len(events))
	if err = buffer.Flusher(events); err != nil {
		log.Println(err)
	}

	lastFlushedResumeToken = &ResumeTokenStore{}
	lastFlushedResumeToken.Timestamp = events[len(events)-1].Timestamp

	if err = buffer.AfterFlush(len(events)); err != nil {
		return
	}
	return
}

func (buffer *Buffer) FlushAll() (lastFlushedResumeToken *ResumeTokenStore, err error) {

	for {
		if buffer.Length() == 0 {
			break
		}
		if lastFlushedResumeToken, err = buffer.Flush(); err != nil {
			return
		}
	}
	return
}
