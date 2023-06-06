package mongoreplay

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	LastUpdatedResumeFile = "/tmp/last-updated-resume-token"
)

type (
	TailerManager struct {
		// TailerManager is the sole entity responsible for coordinating
		// between the tailer, buffer and managing the bookmarks.
		// It runs in the scope of the collection.

		Ctx context.Context

		SourceCollection *OplogCollection

		DestCollection *OplogCollection

		tailer    *OplogTailer
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

func NewTailerManager(ctx context.Context, srcColl *OplogCollection, dstColl *OplogCollection) (stageExecutor StageExecutor, err error) {
	var (
		tailMgr *TailerManager
	)
	tailMgr = &TailerManager{
		Ctx:              ctx,
		SourceCollection: srcColl,
		DestCollection:   dstColl,

		trackerCloseCh: make(chan bool),
	}
	if stageExecutor, err = NewOplogTailer(tailMgr.Ctx, srcColl, dstColl); err != nil {
		return
	}
	tailMgr.tailer = stageExecutor.(*OplogTailer)
	if tailMgr.query_gen, err = NewQueryGenerator(tailMgr.Ctx, srcColl.MongoCollection); err != nil {
		return
	}
	if tailMgr.buffer, err = NewBuffer(tailMgr.Ctx, tailMgr.query_gen.ProcessAll); err != nil {
		return
	}
	stageExecutor = tailMgr
	return
}

func (tailMgr *TailerManager) getFileName() (fileName string) {
	fileName = fmt.Sprintf("%s-%s-%s", LastUpdatedResumeFile, tailMgr.SourceCollection.MongoDatabase.Name(), tailMgr.SourceCollection.MongoCollection.Name())
	return
}

func (tailMgr *TailerManager) updateLastResumeToken(resumeToken *ResumeTokenStore) (err error) {
	var (
		resumeB []byte
	)
	if resumeB, err = json.Marshal(resumeToken); err != nil {
		return
	}
	if err = ioutil.WriteFile(tailMgr.getFileName(), resumeB, 0755); err != nil {
		return
	}
	return
}

func (tailMgr *TailerManager) getLastResumeTokenFromStore() (resumeToken *ResumeTokenStore, err error) {
	var (
		resumeB []byte
	)
	if resumeB, err = ioutil.ReadFile(tailMgr.getFileName()); err != nil {
		return
	}
	if err = json.Unmarshal(resumeB, resumeToken); err != nil {
		return
	}
	return
}

func (tailMgr *TailerManager) trackTailerMessages() (err error) {
	for {
		select {
		case <-tailMgr.Ctx.Done():
			log.Println("[TailerManager] Close signal received")
			return
		case <-tailMgr.trackerCloseCh:
			log.Println("[TailerManager] Close signal received")
			return
		case msg := <-tailMgr.tailer.CtrlrCh:
			var (
				lastResumeToken *ResumeTokenStore
			)
			if err = tailMgr.buffer.Store(msg); err != nil {
				log.Println("[TailerManager] Error on storing message in buffer", msg, err)
			}
			if !tailMgr.buffer.ShouldFlush() {
				continue
			}
			if lastResumeToken, err = tailMgr.buffer.Flush(); err != nil {
				log.Println("[TailerManager] Error on flushing messages in buffer", err)
			}
			log.Println("[TailerManager] Updating resume token", lastResumeToken.Timestamp)
			if err = tailMgr.updateLastResumeToken(lastResumeToken); err != nil {
				log.Println(err)
				return
			}
		}
	}
}

func (tailMgr *TailerManager) Run(args ...interface{}) (err error) {
	var (
		lastResumeToken *ResumeTokenStore
	)
	if lastResumeToken, err = tailMgr.getLastResumeTokenFromStore(); err != nil {
		lastResumeToken = &ResumeTokenStore{}
	}
	go tailMgr.trackTailerMessages()

	if err = tailMgr.tailer.Run(lastResumeToken); err != nil {
		log.Println("[TailerManager]", err)
	}
	tailMgr.trackerCloseCh <- true
	return
}
