package oplog

import (
	"encoding/json"
	"io/ioutil"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
)

const (
	LastUpdatedResumeFile = "/tmp/last-updated-resume-token"
)

type (
	Controller struct {
		// Controller is the sole entity responsible for coordinating
		// between the watcher, buffer and managing the bookmarks.
		// It runs in the scope of the collection.
		SourceDatabase   *mongo.Database
		SourceCollection *mongo.Collection

		DestDatabase   *mongo.Database
		DestCollection *mongo.Collection

		watcher *OplogWatcher
		buffer  *Buffer

		trackerCloseCh chan bool
	}
)

func NewController(srcDb *mongo.Database, srcColl *mongo.Collection, dstDb *mongo.Database, dstColl *mongo.Collection) (ctrlr *Controller, err error) {
	ctrlr = &Controller{
		SourceDatabase:   srcDb,
		SourceCollection: srcColl,
		DestDatabase:     dstDb,
		DestCollection:   dstColl,

		trackerCloseCh: make(chan bool),
	}
	if ctrlr.watcher, err = NewOplogWatcher(srcDb, srcColl); err != nil {
		return
	}
	if ctrlr.buffer, err = NewBuffer(LogFlusherFunc); err != nil {
		return
	}
	return
}

func (ctrlr *Controller) updateLastResumeToken(resumeToken string) (err error) {
	var (
		resumeB []byte
	)
	if resumeB, err = json.Marshal(resumeToken); err != nil {
		return
	}
	if err = ioutil.WriteFile(LastUpdatedResumeFile, resumeB, 0755); err != nil {
		return
	}
	return
}

func (ctrlr *Controller) getLastResumeTokenFromStore() (resumeToken string, err error) {
	var (
		resumeB []byte
	)
	if resumeB, err = ioutil.ReadFile(LastUpdatedResumeFile); err != nil {
		return
	}
	if err = json.Unmarshal(resumeB, resumeToken); err != nil {
		return
	}
	return
}

func (ctrlr *Controller) trackWatcherMessages() (err error) {
	for {
		select {
		case <-ctrlr.trackerCloseCh:
			log.Println("Close signal received")
			return
		case msg := <-ctrlr.watcher.CtrlrCh:
			var (
				lastResumeToken string
			)
			if err = ctrlr.buffer.Store(msg); err != nil {
				log.Println("Error on storing message in buffer", msg, err)
			}
			if !ctrlr.buffer.ShouldFlush() {
				continue
			}
			if lastResumeToken, err = ctrlr.buffer.Flush(); err != nil {
				log.Println("Error on flushing messages in buffer", err)
			}
			if err = ctrlr.updateLastResumeToken(lastResumeToken); err != nil {
				log.Println(err)
				return
			}
		}
	}
}

func (ctrlr *Controller) Run() (err error) {
	var (
		lastResumeToken string
	)
	lastResumeToken, _ = ctrlr.getLastResumeTokenFromStore()
	go ctrlr.trackWatcherMessages()

	if err = ctrlr.watcher.Run(lastResumeToken); err != nil {
		log.Println(err)
	}
	ctrlr.trackerCloseCh <- true
	return
}
