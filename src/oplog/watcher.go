package oplog

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type (
	OplogWatcher struct {
		Database                  *mongo.Database
		Collection                *mongo.Collection
		Bookmark                  string
		WatchThreshold            int
		WatchCount                int
		ShouldHonorWatchThreshold bool
	}
)

func NewOplogWatcher(db *mongo.Database, collection *mongo.Collection) (watcher *OplogWatcher, err error) {
	watcher = &OplogWatcher{
		Database:       db,
		Collection:     collection,
		WatchThreshold: 1000,
	}
	return
}

func (watcher *OplogWatcher) getStreamOpts() (opts *options.ChangeStreamOptions) {
	opts = options.ChangeStream()
	opts.SetMaxAwaitTime(2 * time.Second)
	opts.SetFullDocument(options.UpdateLookup)
	return
}

func (watcher *OplogWatcher) Run() (err error) {
	var (
		collectionStream *mongo.ChangeStream
	)
	matchStage := bson.D{{"$match", bson.D{{}}}}

	log.Println("Starting watching oplog on", watcher.Database.Name())

	if collectionStream, err = watcher.Collection.Watch(
		context.TODO(),
		mongo.Pipeline{matchStage},
		watcher.getStreamOpts(),
	); err != nil {
		log.Println(err)
		return
	}
	for collectionStream.Next(context.TODO()) {
		log.Println("Received oplog event", collectionStream.Current)
		watcher.WatchCount += 1
		if watcher.ShouldHonorWatchThreshold == true && watcher.WatchCount >= watcher.WatchThreshold {
			log.Println("Exiting to honor WatchThreshold")
			break
		}
	}
	return
}
