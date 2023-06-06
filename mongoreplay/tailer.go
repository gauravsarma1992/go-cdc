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
	OplogTailer struct {
		Ctx context.Context

		Collection *OplogCollection

		FetchCountThreshold int

		WatchThreshold            int
		WatchCount                int
		ShouldHonorWatchThreshold bool

		CtrlrCh chan *MessageN
	}
)

func NewOplogTailer(ctx context.Context, collection *OplogCollection, dstCollection *OplogCollection) (stageExecutor StageExecutor, err error) {
	tailer := &OplogTailer{
		Ctx:                 ctx,
		Collection:          collection,
		WatchThreshold:      1000,
		FetchCountThreshold: 1000,

		CtrlrCh: make(chan *MessageN, 1024),
	}
	stageExecutor = tailer
	return
}

func (tailer *OplogTailer) ShouldContinueProcessing() (shouldContinue bool) {
	if tailer.ShouldHonorWatchThreshold == true && tailer.WatchCount >= tailer.WatchThreshold {
		log.Println("[Tailer] Exiting to honor WatchThreshold")
		return
	}
	shouldContinue = true
	return
}

func (tailer *OplogTailer) FetchFromOplog(resumeToken *ResumeTokenStore) (messages []*MessageN, err error) {
	var (
		oplogCollection *mongo.Collection
		findOptions     *options.FindOptions
		cursor          *mongo.Cursor
		results         []bson.M
		filters         bson.M
	)

	findOptions = options.Find()
	findOptions.SetLimit(int64(tailer.FetchCountThreshold))

	if filters, err = tailer.Collection.GetOplogFilter(resumeToken); err != nil {
		return
	}

	oplogCollection = tailer.Collection.MongoDatabase.Client().Database("local").Collection("oplog.rs")
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
		tailer.CtrlrCh <- message
		messages = append(messages, message)
	}
	return
}

func (tailer *OplogTailer) Run(args ...interface{}) (err error) {
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
		case <-tailer.Ctx.Done():
			log.Println("[Tailer] Exiting tailer")
			return
		case <-ticker.C:
			var (
				messages []*MessageN
			)
			if messages, err = tailer.FetchFromOplog(currResumeToken); err != nil {
				log.Println("[Tailer] Error in fetching from oplog", err)
			}
			// Update the resume token to the latest timestamp
			currResumeToken.Timestamp = messages[len(messages)-1].Timestamp

			tailer.WatchCount += len(messages)
			if tailer.ShouldHonorWatchThreshold == true && len(messages) >= tailer.WatchThreshold {
				return
			}
		}
	}
	return
}
