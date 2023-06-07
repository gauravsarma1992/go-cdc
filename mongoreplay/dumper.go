package mongoreplay

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type (
	Dumper struct {
		Ctx           context.Context
		Config        *DumperConfig
		SrcCollection *OplogCollection
		DstCollection *OplogCollection

		LastResumeToken *ResumeTokenStore

		DumperCloseCh chan bool
		DumperCh      chan *MessageN

		buffer    *Buffer
		query_gen *QueryGenerator
	}
	DumperConfig struct {
		FetchCountThreshold int `json:"fetch_count_threshold"`
	}
)

func NewDumper(ctx context.Context, srcCollection *OplogCollection, dstCollection *OplogCollection) (stageExecutor StageExecutor, err error) {
	var (
		dumper *Dumper
	)
	dumper = &Dumper{
		Ctx:           ctx,
		SrcCollection: srcCollection,
		DstCollection: dstCollection,
		Config: &DumperConfig{
			FetchCountThreshold: 1000,
		},
		DumperCloseCh: make(chan bool),
		DumperCh:      make(chan *MessageN),
	}
	if dumper.query_gen, err = NewQueryGenerator(dumper.Ctx, dumper.DstCollection.MongoCollection); err != nil {
		return
	}
	if dumper.buffer, err = NewBuffer(dumper.Ctx, dumper.query_gen.ProcessAll); err != nil {
		return
	}
	stageExecutor = dumper
	return
}

func (dumper *Dumper) StartQuery() (err error) {
	var (
		filters bson.M
		cursor  *mongo.Cursor
	)
	filters = bson.M{}
	if err = dumper.SrcCollection.AddCollectionFilter(filters, false); err != nil {
		return
	}
	if cursor, err = dumper.SrcCollection.MongoCollection.Find(context.TODO(), filters); err != nil {
		return
	}
	log.Println("[Dumper] Starting query with length", cursor.RemainingBatchLength(),
		"on collection", dumper.SrcCollection.GetCollectionPath(), "with filters", filters)

	for cursor.Next(context.TODO()) {
		var (
			result  bson.M
			message *MessageN
		)
		if err = cursor.Decode(&result); err != nil {
			log.Println(err)
			continue
		}
		message = &MessageN{
			CollectionPath: dumper.SrcCollection.GetCollectionPath(),
			FullDocument:   result,
			OperationType:  InsertOperation,
			Timestamp:      result["createdAt"].(primitive.Timestamp),
		}
		dumper.DumperCh <- message
	}
	if err = cursor.Err(); err != nil {
		return
	}
	return
}

func (dumper *Dumper) trackRows() (err error) {
	for {
		select {
		case <-dumper.DumperCloseCh:
			log.Println("[Dumper] Close signal received in Dumper from close channel")
			return
		case <-dumper.Ctx.Done():
			log.Println("[Dumper] Close signal received in Dumper from context")
			return
		case msg := <-dumper.DumperCh:
			if err = dumper.buffer.Store(msg); err != nil {
				log.Println("[Dumper] Error on storing message in buffer", msg, err)
			}
			if !dumper.buffer.ShouldFlush() {
				continue
			}
			if dumper.LastResumeToken, err = dumper.buffer.Flush(); err != nil {
				log.Println("[Dumper] Error on flushing messages in buffer", err)
			}
		}
	}
}

func (dumper *Dumper) Run(args ...interface{}) (err error) {

	go dumper.trackRows()

	if err = dumper.DstCollection.Delete(bson.M{}); err != nil {
		return
	}
	if err = dumper.StartQuery(); err != nil {
		log.Println(err)
		return
	}

	dumper.DumperCloseCh <- true

	if dumper.LastResumeToken, err = dumper.buffer.FlushAll(); err != nil {
		log.Println("[Dumper] Error on flushing messages in buffer", err)
	}

	if err = dumper.LastResumeToken.Store(); err != nil {
		return
	}

	return
}
