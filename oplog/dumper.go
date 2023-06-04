package oplog

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
)

type (
	Dumper struct {
		Ctx        context.Context
		Config     *DumperConfig
		Collection *OplogCollection
	}
	DumperConfig struct {
		FetchCountThreshold int `json:"fetch_count_threshold"`
	}
)

func NewDumper(ctx context.Context) (dumper *Dumper, err error) {
	dumper = &Dumper{
		Ctx: ctx,
		Config: &DumperConfig{
			FetchCountThreshold: 1000,
		},
	}
	return
}

func (dump *Dumper) GetQuery() (err error) {
	dump.Collection.MongoCollection.Find(dump.Ctx, bson.M{})
	return
}

func (dump *Dumper) Dump() (err error) {
	return
}
