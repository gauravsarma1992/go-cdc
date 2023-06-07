package main

import (
	"context"
	"log"

	"github.com/gauravsarma1992/mongoreplay/mongoreplay"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {

	var (
		oplogCtx *mongoreplay.Oplog
		seeder   *mongoreplay.Seeder
		docCount int64
		err      error
	)

	if oplogCtx, err = mongoreplay.New(); err != nil {
		log.Fatal(err)
	}
	if err = oplogCtx.Connect(); err != nil {
		log.Fatal(err)
	}
	// Cleaning the collections on both sides
	if err = oplogCtx.SrcCollections["coll_one"].Delete(bson.M{}); err != nil {
		log.Fatal(err)
	}
	if err = oplogCtx.DstCollections["coll_one"].Delete(bson.M{}); err != nil {
		log.Fatal(err)
	}

	// Starting the seeding
	if seeder, err = mongoreplay.NewSeeder(1000, oplogCtx.DstCollections["coll_one"]); err != nil {
		log.Fatal(err)
	}

	if err = seeder.Seed(); err != nil {
		log.Fatal(err)
	}

	if docCount, err = seeder.Collection.MongoCollection.CountDocuments(context.TODO(), bson.M{}); err != nil {
		log.Fatal(err)
	}
	log.Println("[Seeder] Total documents seeded in", seeder.Collection.GetCollectionPath(), "-", docCount)
}
