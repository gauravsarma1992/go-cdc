package oplog

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type (
	OplogWatcher struct {
		Database   *mongo.Database
		Collection *mongo.Collection

		FetchCountThreshold int

		WatchThreshold            int
		WatchCount                int
		ShouldHonorWatchThreshold bool

		CtrlrCh chan *MessageN
	}
)

func NewOplogWatcher(db *mongo.Database, collection *mongo.Collection) (watcher *OplogWatcher, err error) {
	watcher = &OplogWatcher{
		Database:            db,
		Collection:          collection,
		WatchThreshold:      1000,
		FetchCountThreshold: 1000,

		CtrlrCh: make(chan *MessageN, 1024),
	}
	return
}

func (watcher *OplogWatcher) getStreamOpts(resumeToken string) (opts *options.ChangeStreamOptions) {
	opts = options.ChangeStream()
	opts.SetMaxAwaitTime(2 * time.Second)
	opts.SetFullDocument(options.UpdateLookup)
	//opts.SetResumeAfter(resumeToken.Data)
	return
}

func (watcher *OplogWatcher) ShouldContinueProcessing() (shouldContinue bool) {
	if watcher.ShouldHonorWatchThreshold == true && watcher.WatchCount >= watcher.WatchThreshold {
		log.Println("Exiting to honor WatchThreshold")
		return
	}
	shouldContinue = true
	return
}

func (watcher *OplogWatcher) FetchFromOplog() (messages []*MessageN, err error) {
	var (
		oplogCollection *mongo.Collection
		findOptions     *options.FindOptions
		cursor          *mongo.Cursor
		ns              string
		results         []bson.M
	)
	ns = fmt.Sprintf("%s.%s", watcher.Database.Name(), watcher.Collection.Name())

	findOptions = options.Find()
	findOptions.SetLimit(int64(watcher.FetchCountThreshold))

	oplogCollection = watcher.Database.Client().Database("local").Collection("oplog.rs")
	if cursor, err = (oplogCollection.Find(context.TODO(), bson.D{{"ns", ns}}, findOptions)); err != nil {
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
			OperationType:  result["op"].(string),
			Timestamp:      result["ts"].(primitive.Timestamp),
		}
		watcher.CtrlrCh <- message
		messages = append(messages, message)
	}
	return
}

func (watcher *OplogWatcher) Run(resumeToken string) (err error) {
	for {
		var (
			messages []*MessageN
		)
		if messages, err = watcher.FetchFromOplog(); err != nil {
			log.Println(err)
		}
		watcher.WatchCount += len(messages)
		if watcher.ShouldHonorWatchThreshold == true && len(messages) >= watcher.WatchThreshold {
			break
		}
	}
	return
}
