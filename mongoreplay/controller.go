package mongoreplay

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
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

		Ctx context.Context

		SourceDatabase   *mongo.Database
		SourceCollection *OplogCollection

		DestDatabase   *mongo.Database
		DestCollection *OplogCollection

		watcher   *OplogWatcher
		buffer    *Buffer
		query_gen *QueryGenerator

		trackerCloseCh chan bool
	}

	ResumeTokenStore struct {
		Timestamp primitive.Timestamp `json:"timestamp"`
	}
)

func (resumeToken *ResumeTokenStore) Copy() (copied *ResumeTokenStore) {
	copied = &ResumeTokenStore{
		Timestamp: resumeToken.Timestamp,
	}
	return
}

func NewController(ctx context.Context,
	srcDb *mongo.Database,
	srcColl *OplogCollection,
	dstDb *mongo.Database,
	dstColl *OplogCollection,
) (ctrlr *Controller, err error) {

	var (
		stageExecutor StageExecutor
	)

	ctrlr = &Controller{
		Ctx:              ctx,
		SourceDatabase:   srcDb,
		SourceCollection: srcColl,
		DestDatabase:     dstDb,
		DestCollection:   dstColl,

		trackerCloseCh: make(chan bool),
	}
	if stageExecutor, err = NewOplogWatcher(ctrlr.Ctx, srcColl, dstColl); err != nil {
		return
	}
	ctrlr.watcher = stageExecutor.(*OplogWatcher)
	if ctrlr.query_gen, err = NewQueryGenerator(ctrlr.Ctx, srcColl.MongoCollection); err != nil {
		return
	}
	if ctrlr.buffer, err = NewBuffer(ctrlr.Ctx, ctrlr.query_gen.ProcessAll); err != nil {
		return
	}
	return
}

func (ctrlr *Controller) getFileName() (fileName string) {
	fileName = fmt.Sprintf("%s-%s-%s", LastUpdatedResumeFile, ctrlr.SourceDatabase.Name(), ctrlr.SourceCollection.MongoCollection.Name())
	return
}

func (ctrlr *Controller) updateLastResumeToken(resumeToken *ResumeTokenStore) (err error) {
	var (
		resumeB []byte
	)
	if resumeB, err = json.Marshal(resumeToken); err != nil {
		return
	}
	if err = ioutil.WriteFile(ctrlr.getFileName(), resumeB, 0755); err != nil {
		return
	}
	return
}

func (ctrlr *Controller) getLastResumeTokenFromStore() (resumeToken *ResumeTokenStore, err error) {
	var (
		resumeB []byte
	)
	if resumeB, err = ioutil.ReadFile(ctrlr.getFileName()); err != nil {
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
		case <-ctrlr.Ctx.Done():
			log.Println("[Controller] Close signal received")
			return
		case <-ctrlr.trackerCloseCh:
			log.Println("[Controller] Close signal received")
			return
		case msg := <-ctrlr.watcher.CtrlrCh:
			var (
				lastResumeToken *ResumeTokenStore
			)
			if err = ctrlr.buffer.Store(msg); err != nil {
				log.Println("[Controller] Error on storing message in buffer", msg, err)
			}
			if !ctrlr.buffer.ShouldFlush() {
				continue
			}
			if lastResumeToken, err = ctrlr.buffer.Flush(); err != nil {
				log.Println("[Controller] Error on flushing messages in buffer", err)
			}
			log.Println("[Controller] Updating resume token", lastResumeToken.Timestamp)
			if err = ctrlr.updateLastResumeToken(lastResumeToken); err != nil {
				log.Println(err)
				return
			}
		}
	}
}

func (ctrlr *Controller) Run() (err error) {
	var (
		lastResumeToken *ResumeTokenStore
	)
	if lastResumeToken, err = ctrlr.getLastResumeTokenFromStore(); err != nil {
		lastResumeToken = &ResumeTokenStore{}
	}
	go ctrlr.trackWatcherMessages()

	if err = ctrlr.watcher.Run(lastResumeToken); err != nil {
		log.Println("[Controller]", err)
	}
	ctrlr.trackerCloseCh <- true
	return
}
