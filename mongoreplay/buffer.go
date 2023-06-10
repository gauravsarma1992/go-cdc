package mongoreplay

import (
	"context"
	"errors"
	"fmt"
)

type (
	FlusherFunc func(msg *MessageN) (err error)

	Buffer struct {
		Ctx     context.Context
		flusher FlusherFunc

		store []*MessageN

		startIdx int
		stopIdx  int

		Config *BufferConfig
	}

	BufferConfig struct {
		Capacity int `json:"capacity"`
	}
)

func NewBuffer(ctx context.Context, flusherFunc FlusherFunc) (buffer *Buffer, err error) {
	buffer = &Buffer{
		startIdx: 0,
		stopIdx:  0,
		flusher:  flusherFunc,
	}
	if err = buffer.updateConfig(); err != nil {
		return
	}
	buffer.store = make([]*MessageN, buffer.Config.Capacity)
	return
}

func (buffer *Buffer) updateConfig() (err error) {
	buffer.Config = &BufferConfig{
		Capacity: 1000,
	}
	return
}

func (buffer *Buffer) IsEmpty() (isEmpty bool) {
	if buffer.startIdx == buffer.stopIdx {
		isEmpty = true
		return
	}
	return
}

func (buffer *Buffer) IsFull() (isFull bool) {
	if buffer.startIdx == (buffer.stopIdx+1)%buffer.Config.Capacity {
		isFull = true
		return
	}
	return
}

func (buffer *Buffer) Store(msg *MessageN) (err error) {
	if buffer.IsFull() {
		err = errors.New("Buffer is full")
		return
	}

	buffer.store[buffer.stopIdx] = msg
	buffer.stopIdx = (buffer.stopIdx + 1) % buffer.Config.Capacity
	return
}

func (buffer *Buffer) Flush() (msg *MessageN, err error) {
	if buffer.IsEmpty() {
		return
	}
	msg = buffer.store[buffer.startIdx]
	if err = buffer.flusher(msg); err != nil {
		err = nil
	}
	buffer.startIdx = (buffer.startIdx + 1) % buffer.Config.Capacity
	return
}

func (buffer *Buffer) FlushAll() (msgs []*MessageN, err error) {
	for !buffer.IsEmpty() {
		var msg *MessageN
		if msg, err = buffer.Flush(); err != nil {
			return
		}
		msgs = append(msgs, msg)
	}
	return
}

func (buffer *Buffer) String() (displayStr string) {
	var (
		currIdx int
	)
	if buffer.IsEmpty() {
		displayStr = "empty"
		return
	}
	displayStr = "startIdx"
	currIdx = buffer.startIdx
	for {
		displayStr += fmt.Sprintf("<-%+v", buffer.store[currIdx])
		currIdx = (currIdx + 1) % buffer.Config.Capacity
		if currIdx == buffer.stopIdx {
			break
		}
	}
	displayStr += "<-stopIdx"
	return
}
