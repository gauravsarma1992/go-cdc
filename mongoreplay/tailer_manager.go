package mongoreplay

import (
	"context"
	"log"
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
	if tailMgr.query_gen, err = NewQueryGenerator(tailMgr.Ctx, srcColl.MongoCollection); err != nil {
		return
	}
	if tailMgr.buffer, err = NewBuffer(tailMgr.Ctx, tailMgr.query_gen.ProcessAll); err != nil {
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

func (tailMgr *TailerManager) trackTailerMessages() (err error) {
	for {
		select {
		case <-tailMgr.Ctx.Done():
			log.Println("[TailerManager] Close signal received from context")
			return
		case <-tailMgr.trackerCloseCh:
			log.Println("[TailerManager] Close signal received from tracker close channel")
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
	log.Println("[TailerManager] Starting tailer from token", lastResumeToken.Timestamp)
	go tailMgr.trackTailerMessages()

	if err = tailMgr.tailer.Run(lastResumeToken); err != nil {
		log.Println("[TailerManager]", err)
	}
	tailMgr.trackerCloseCh <- true
	return
}
