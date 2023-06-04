package oplog

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type (
	Dumper struct {
		Ctx           context.Context
		Config        *DumperConfig
		SrcCollection *OplogCollection
		DstCollection *OplogCollection
		buffer        *Buffer
		query_gen     *QueryGenerator
	}
	DumperConfig struct {
		FetchCountThreshold int `json:"fetch_count_threshold"`
	}
)

func NewDumper(ctx context.Context, srcCollection *OplogCollection, dstCollection *OplogCollection) (dumper *Dumper, err error) {
	dumper = &Dumper{
		Ctx:           ctx,
		SrcCollection: srcCollection,
		DstCollection: dstCollection,
		Config: &DumperConfig{
			FetchCountThreshold: 1000,
		},
	}
	if dumper.query_gen, err = NewQueryGenerator(dumper.Ctx, dumper.DstCollection.MongoCollection); err != nil {
		return
	}
	if dumper.buffer, err = NewBuffer(dumper.Ctx, dumper.query_gen.ProcessAll); err != nil {
		return
	}
	return
}

func (dump *Dumper) GetQuery() (err error) {
	var (
		filters bson.M
		cursor  *mongo.Cursor
	)
	if err = dump.SrcCollection.AddCollectionFilter(filters); err != nil {
		return
	}
	if cursor, err = dump.SrcCollection.MongoCollection.Find(context.TODO(), filters); err != nil {
		return
	}
	for cursor.Next(context.TODO()) {
		var result bson.M
		if err = cursor.Decode(&result); err != nil {
			log.Println(err)
		}
	}
	if err = cursor.Err(); err != nil {
		return
	}
	return
}

func (dump *Dumper) Dump() (err error) {
	return
}
