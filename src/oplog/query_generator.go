package oplog

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type (
	QueryFunc      func(*Message) error
	QueryGenerator struct {
		Collection *mongo.Collection

		queryMap map[string]QueryFunc
	}
)

func NewQueryGenerator(coll *mongo.Collection) (queryGen *QueryGenerator, err error) {
	queryGen = &QueryGenerator{
		Collection: coll,
		queryMap:   make(map[string]QueryFunc),
	}
	queryGen.prepareQueryMap()
	return
}

func (queryGen *QueryGenerator) prepareQueryMap() (err error) {
	queryGen.queryMap["insert"] = queryGen.Insert
	queryGen.queryMap["update"] = queryGen.Update
	queryGen.queryMap["delete"] = queryGen.Delete
	return
}

func (queryGen *QueryGenerator) routeQuery(msg *Message) (queryFunc QueryFunc, err error) {
	var (
		isPresent bool
	)
	if queryFunc, isPresent = queryGen.queryMap[msg.OperationType]; !isPresent {
		err = errors.New(fmt.Sprintf("Operation type %s not present", msg.OperationType))
		return
	}
	return
}

func (queryGen *QueryGenerator) Insert(msg *Message) (err error) {
	_, err = queryGen.Collection.InsertOne(context.TODO(), msg.FullDocument)
	return
}

func (queryGen *QueryGenerator) Update(msg *Message) (err error) {
	_, err = queryGen.Collection.UpdateOne(context.TODO(), bson.D{{"_id", msg.FullDocument["_id"]}}, bson.D{{"$set", msg.FullDocument}})
	return
}

func (queryGen *QueryGenerator) Delete(msg *Message) (err error) {
	_, err = queryGen.Collection.DeleteOne(context.TODO(), bson.D{{"_id", msg.FullDocument["_id"]}})
	return
}

func (queryGen *QueryGenerator) Process(msg *Message) (err error) {
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
