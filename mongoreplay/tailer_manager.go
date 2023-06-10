package mongoreplay

import (
	"context"
	"log"
	"time"
)

type (
	TailerManager struct {
		// TailerManager is the sole entity responsible for coordinating
		// between the tailer, buffer and managing the bookmarks.
		// It runs in the scope of the collection.

		Ctx context.Context

		SourceCollection *OplogCollection
		DestCollection   *OplogCollection

		tailer    *OplogTailer
		buffer    *Buffer
		query_gen *QueryGenerator

		LastResumeToken *ResumeTokenStore
		trackerCloseCh  chan bool
	}
)

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
	if tailMgr.query_gen, err = NewQueryGenerator(tailMgr.Ctx, dstColl.MongoCollection); err != nil {
		return
	}
	if tailMgr.buffer, err = NewBuffer(tailMgr.Ctx, tailMgr.query_gen.Process); err != nil {
		return
	}
	stageExecutor = tailMgr
	return
}

func (tailMgr *TailerManager) updateLastResumeToken(resumeToken *ResumeTokenStore) (err error) {
	if err = resumeToken.Store(); err != nil {
		return
	}
	return
}

func (tailMgr *TailerManager) getLastResumeTokenFromStore() (resumeToken *ResumeTokenStore, err error) {
	var (
		resumeTokenStore ResumeTokenStore
	)
	if resumeToken, err = resumeTokenStore.Fetch(); err != nil {
		return
	}
	return
}

func (tailMgr *TailerManager) flushAll() (err error) {
	var (
		msgs        []*MessageN
		resumeToken *ResumeTokenStore
	)
	if tailMgr.buffer.IsEmpty() {
		return
	}
	if msgs, err = tailMgr.buffer.FlushAll(); err != nil {
		log.Println("[TailerManager] Error on flushing messages in buffer", err)
	}
	resumeToken = &ResumeTokenStore{
		Timestamp: msgs[len(msgs)-1].Timestamp,
	}
	log.Println("[TailerManager] Updating resume token", resumeToken.Timestamp)
	if err = tailMgr.updateLastResumeToken(resumeToken); err != nil {
		log.Println(err)
		return
	}
	return
}

func (tailMgr *TailerManager) trackTailerMessages() (err error) {
	var (
		ticker *time.Ticker
	)
	ticker = time.NewTicker(1 * time.Second)
	for {
		select {
		case <-tailMgr.Ctx.Done():
			log.Println("[TailerManager] Close signal received from context")
			return
		case <-tailMgr.trackerCloseCh:
			log.Println("[TailerManager] Close signal received from tracker close channel")
			return
		case msg := <-tailMgr.tailer.CtrlrCh:
			if err = tailMgr.buffer.Store(msg); err != nil {
				log.Println("[TailerManager] Error on storing message in buffer", msg, err)
			}

		case <-ticker.C:
			if err = tailMgr.flushAll(); err != nil {
				log.Println("[TailerManager] Error on flushing messages in buffer", err)
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
	log.Println("[TailerManager] Starting tailer from token", lastResumeToken.Timestamp)
	go tailMgr.trackTailerMessages()

	if err = tailMgr.tailer.Run(lastResumeToken); err != nil {
		log.Println("[TailerManager]", err)
	}
	tailMgr.trackerCloseCh <- true
	return
}
