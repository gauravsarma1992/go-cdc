package mongoreplay

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type (
	OplogWatcher struct {
		Ctx context.Context

		Collection *OplogCollection

		FetchCountThreshold int

		WatchThreshold            int
		WatchCount                int
		ShouldHonorWatchThreshold bool

		CtrlrCh chan *MessageN
	}
)

func NewOplogWatcher(ctx context.Context, collection *OplogCollection, dstCollection *OplogCollection) (stageExecutor StageExecutor, err error) {
	watcher := &OplogWatcher{
		Ctx:                 ctx,
		Collection:          collection,
		WatchThreshold:      1000,
		FetchCountThreshold: 1000,

		CtrlrCh: make(chan *MessageN, 1024),
	}
	stageExecutor = watcher
	return
}

func (watcher *OplogWatcher) ShouldContinueProcessing() (shouldContinue bool) {
	if watcher.ShouldHonorWatchThreshold == true && watcher.WatchCount >= watcher.WatchThreshold {
		log.Println("[Watcher] Exiting to honor WatchThreshold")
		return
	}
	shouldContinue = true
	return
}

func (watcher *OplogWatcher) FetchFromOplog(resumeToken *ResumeTokenStore) (messages []*MessageN, err error) {
	var (
		oplogCollection *mongo.Collection
		findOptions     *options.FindOptions
		cursor          *mongo.Cursor
		results         []bson.M
		filters         bson.M
	)

	findOptions = options.Find()
	findOptions.SetLimit(int64(watcher.FetchCountThreshold))

	if filters, err = watcher.Collection.GetOplogFilter(resumeToken); err != nil {
		return
	}

	oplogCollection = watcher.Collection.MongoDatabase.Client().Database("local").Collection("oplog.rs")
	if cursor, err = oplogCollection.Find(context.TODO(), filters, findOptions); err != nil {
		return
	}

	if err = cursor.All(context.TODO(), &results); err != nil {
		log.Println(err)
	}

	for _, result := range results {
		var (
			message *MessageN
		)
		message = &MessageN{
			CollectionPath: result["ns"].(string),
			FullDocument:   result["o"].(bson.M),
			OperationType:  OperationTypeT(result["op"].(string)),
			Timestamp:      result["ts"].(primitive.Timestamp),
		}
		watcher.CtrlrCh <- message
		messages = append(messages, message)
	}
	return
}

func (watcher *OplogWatcher) Run(args ...interface{}) (err error) {
	var (
		currResumeToken *ResumeTokenStore
		ticker          *time.Ticker
		resumeToken     *ResumeTokenStore
	)
	resumeToken = args[0].(*ResumeTokenStore)
	ticker = time.NewTicker(1 * time.Second)
	currResumeToken = resumeToken.Copy()
	for {
		select {
		case <-watcher.Ctx.Done():
			log.Println("[Watcher] Exiting watcher")
			return
		case <-ticker.C:
			var (
				messages []*MessageN
			)
			if messages, err = watcher.FetchFromOplog(currResumeToken); err != nil {
				log.Println("[Watcher] Error in fetching from oplog", err)
			}
			// Update the resume token to the latest timestamp
			currResumeToken.Timestamp = messages[len(messages)-1].Timestamp

			watcher.WatchCount += len(messages)
			if watcher.ShouldHonorWatchThreshold == true && len(messages) >= watcher.WatchThreshold {
				return
			}
		}
	}
	return
}
