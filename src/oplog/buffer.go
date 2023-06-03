package oplog

import (
	"encoding/json"
	"io/ioutil"
	"strconv"
	"time"
)

const (
	DefaultBufferConfigFile = "./config/buffer_config.json"
)

type (
	FlusherFunc func([]*MessageN) error
	Buffer      struct {
		store         []*MessageN
		LastFlushedAt time.Time
		CurrFlushIdx  int
		Config        *BufferConfig
		Flusher       FlusherFunc
	}
	BufferConfig struct {
		CountThreshold      int `json:"count_threshold"`
		TimeInSecsThreshold int `json:"time_in_secs_threshold"`
		RolloverThreshold   int `json:"rollover_threshold"`
	}
)

func NewBuffer(flusherFunc FlusherFunc) (buffer *Buffer, err error) {
	buffer = &Buffer{
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
		bufferConfig.CountThreshold = 1000
	}
	if bufferConfig.TimeInSecsThreshold == 0 {
		bufferConfig.TimeInSecsThreshold = 5
	}
	if bufferConfig.RolloverThreshold == 0 {
		bufferConfig.RolloverThreshold = 5000
	}
	return
}

func (buffer *Buffer) Length() (count int) {
	count = len(buffer.store) - buffer.CurrFlushIdx
	return
}

func (buffer *Buffer) Store(event *MessageN) (err error) {
	buffer.store = append(buffer.store, event)
	return
}

func (buffer *Buffer) ShouldFlush() (shouldFlush bool) {
	timePassed := time.Since(buffer.LastFlushedAt)
	if int(timePassed.Seconds()) > buffer.Config.TimeInSecsThreshold {
		shouldFlush = true
		return
	}
	if buffer.Length() >= buffer.Config.CountThreshold {
		shouldFlush = true
		return
	}
	return
}

func (buffer *Buffer) Rollover() (err error) {
	if len(buffer.store) > buffer.Config.RolloverThreshold {
		buffer.store = make([]*MessageN, 0)
		buffer.LastFlushedAt = time.Now()
		buffer.CurrFlushIdx = 0
	}
	return
}

func (buffer *Buffer) Flush() (lastFlushedResumeToken string, err error) {
	var (
		events []*MessageN
	)
	for idx := 0; idx < len(buffer.store); idx++ {
		events = append(events, buffer.store[idx])
	}
	if err = buffer.Flusher(events); err != nil {
		return
	}
	lastFlushedResumeToken = strconv.Itoa(int(events[len(events)-1].Timestamp.T))
	buffer.CurrFlushIdx = len(buffer.store)
	buffer.LastFlushedAt = time.Now()

	buffer.Rollover()
	return
}
