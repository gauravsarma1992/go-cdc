package oplog

import (
	"context"
	"encoding/json"
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

		CtrlrCh chan *Message
	}
)

func NewOplogWatcher(db *mongo.Database, collection *mongo.Collection) (watcher *OplogWatcher, err error) {
	watcher = &OplogWatcher{
		Database:            db,
		Collection:          collection,
		WatchThreshold:      1000,
		FetchCountThreshold: 1000,

		CtrlrCh: make(chan *Message, 1024),
	}
	return
}

func (watcher *OplogWatcher) getStreamOpts(resumeToken *ResumeToken) (opts *options.ChangeStreamOptions) {
	opts = options.ChangeStream()
	opts.SetMaxAwaitTime(2 * time.Second)
	opts.SetFullDocument(options.UpdateLookup)
	if resumeToken != nil && resumeToken.Data != "" {
		tokenB, _ := json.Marshal(resumeToken)
		log.Println("Resuming from token", string(tokenB))
		//opts.SetResumeAfter(resumeToken.Data)
	}
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
			Operation:      result["op"].(string),
			Timestamp:      result["ts"].(primitive.Timestamp),
		}
		messages = append(messages, message)
		//log.Println("looping", result, result["o"], result["ts"].(primitive.Timestamp).T)
	}
	return
}

func (watcher *OplogWatcher) Run(resumeToken *ResumeToken) (err error) {
	var (
		collectionStream *mongo.ChangeStream
	)
	matchStage := bson.D{{"$match", bson.D{{}}}}

	if collectionStream, err = watcher.Collection.Watch(
		context.TODO(),
		mongo.Pipeline{matchStage},
		watcher.getStreamOpts(resumeToken),
	); err != nil {
		log.Println(err)
		return
	}
	for collectionStream.Next(context.TODO()) {
		var (
			message *Message
		)
		if message, err = NewMessage(collectionStream.Current.String()); err != nil {
			log.Println("Failed to convert raw message to bytes", err)
			continue
		}
		// log.Println("Received oplog event", message, "with ResumeToken", collectionStream.ResumeToken())
		watcher.CtrlrCh <- message

		watcher.WatchCount += 1
		if !watcher.ShouldContinueProcessing() {
			break
		}
	}
	return
}
