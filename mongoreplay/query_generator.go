package mongoreplay

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type (
	QueryFunc      func(*MessageN) error
	QueryGenerator struct {
		Ctx        context.Context
		Collection *mongo.Collection

		queryMap map[OperationTypeT]QueryFunc
	}
)

func NewQueryGenerator(ctx context.Context, coll *mongo.Collection) (queryGen *QueryGenerator, err error) {
	queryGen = &QueryGenerator{
		Ctx:        ctx,
		Collection: coll,
		queryMap:   make(map[OperationTypeT]QueryFunc),
	}
	queryGen.prepareQueryMap()
	return
}

func (queryGen *QueryGenerator) prepareQueryMap() (err error) {
	queryGen.queryMap[InsertOperation] = queryGen.Insert
	queryGen.queryMap[UpdateOperation] = queryGen.Update
	queryGen.queryMap[DeleteOperation] = queryGen.Delete
	return
}

func (queryGen *QueryGenerator) routeQuery(msg *MessageN) (queryFunc QueryFunc, err error) {
	var (
		isPresent bool
	)
	if queryFunc, isPresent = queryGen.queryMap[msg.OperationType]; !isPresent {
		err = errors.New(fmt.Sprintf("Operation type %s not present", msg.OperationType))
		return
	}
	return
}

func (queryGen *QueryGenerator) Insert(msg *MessageN) (err error) {
	_, err = queryGen.Collection.InsertOne(context.TODO(), msg.FullDocument)
	return
}

func (queryGen *QueryGenerator) Update(msg *MessageN) (err error) {
	_, err = queryGen.Collection.UpdateOne(context.TODO(), bson.D{{"_id", msg.FullDocument["_id"]}}, bson.D{{"$set", msg.FullDocument}})
	return
}

func (queryGen *QueryGenerator) Delete(msg *MessageN) (err error) {
	_, err = queryGen.Collection.DeleteOne(context.TODO(), bson.D{{"_id", msg.FullDocument["_id"]}})
	return
}

func (queryGen *QueryGenerator) Process(msg *MessageN) (err error) {
	var (
		queryFunc QueryFunc
	)
	if queryFunc, err = queryGen.routeQuery(msg); err != nil {
		return
	}
	if err = queryFunc(msg); err != nil {
		return
	}
	return
}

func (queryGen *QueryGenerator) ProcessAll(messages []*MessageN) (err error) {
	for _, msg := range messages {
		if err = queryGen.Process(msg); err != nil {
			return
		}
	}
	return
}
