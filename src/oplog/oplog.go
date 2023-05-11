package oplog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/go-cdc/filters"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultOplogConfigFile = "./config/oplog_config.json"
	DefaultMongoConfigFile = "./config/mongo_config.json"
)

type (
	Oplog struct {
		noOfWorkers uint8
		oplogConfig *OplogConfig
		mongoConfig *MongoConfig
		db          *mongo.Database
		collections []*mongo.Collection
	}
	MongoConfig struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Host     string `json:"host"`
		Port     string `json:"port"`
		DbName   string `json:"db_name"`
	}
	OplogConfig struct {
		DbName      string            `json:"db_name"`
		Collections []OplogCollection `json:"collections"`
	}
	OplogCollection struct {
		Name    string           `json:"name"`
		Filters []filters.Filter `json:"filters"`
	}
)

func New() (oplogCtx *Oplog, err error) {
	oplogCtx = &Oplog{
		noOfWorkers: 4,
	}
	if oplogCtx.oplogConfig, err = NewOplogConfig(); err != nil {
		return
	}
	if oplogCtx.mongoConfig, err = NewMongoConfig(); err != nil {
		return
	}
	return
}

func NewOplogConfig() (oplogConfig *OplogConfig, err error) {
	var (
		fileB []byte
	)
	oplogConfig = &OplogConfig{}
	if fileB, err = ioutil.ReadFile(DefaultOplogConfigFile); err != nil {
		return
	}
	if err = json.Unmarshal(fileB, oplogConfig); err != nil {
		return
	}
	if oplogConfig.DbName == "" {
		err = errors.New("DbName should be set")
		return
	}
	if len(oplogConfig.Collections) == 0 {
		err = errors.New("Atleast one collection name should be provided")
		return
	}
	return
}

func NewMongoConfig() (mongoConfig *MongoConfig, err error) {
	var (
		fileB []byte
	)
	mongoConfig = &MongoConfig{}
	if fileB, err = ioutil.ReadFile(DefaultMongoConfigFile); err != nil {
		return
	}
	if err = json.Unmarshal(fileB, mongoConfig); err != nil {
		return
	}
	if mongoConfig.Host == "" {
		mongoConfig.Host = "localhost"
	}
	if mongoConfig.Port == "" {
		mongoConfig.Port = "27017"
	}

	return
}

//func (oplogCtx *Oplog) Watch() (err error) {
//	episodesStream, err := episodesCollection.Watch(context.TODO(), mongo.Pipeline{})
//	if err != nil {
//		panic(err)
//	}
//	return
//}
//

func (mongoConfig *MongoConfig) GetUrl() (url string) {
	if mongoConfig.Username != "" && mongoConfig.Password != "" {
		url = fmt.Sprintf("mongodb://%s:%s@%s:%s",
			mongoConfig.Username,
			mongoConfig.Password,
			mongoConfig.Host,
			mongoConfig.Port,
		)
		return
	}
	url = fmt.Sprintf("mongodb://%s:%s",
		mongoConfig.Host,
		mongoConfig.Port,
	)
	return
}

func (oplogCtx *Oplog) Connect() (err error) {
	var (
		client *mongo.Client
	)
	client, err = mongo.Connect(context.Background(), options.Client().ApplyURI(oplogCtx.mongoConfig.GetUrl()))
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(context.Background())
	if err = client.Ping(context.Background(), nil); err != nil {
		return
	}

	oplogCtx.db = client.Database(oplogCtx.mongoConfig.DbName)
	log.Println("Connected to database - ", oplogCtx.db.Name())

	for _, oplogCollection := range oplogCtx.oplogConfig.Collections {
		var (
			currCollection *mongo.Collection
		)
		currCollection = oplogCtx.db.Collection(oplogCollection.Name)
		oplogCtx.collections = append(oplogCtx.collections, currCollection)

		//log.Println(currCollection.InsertOne(context.TODO(), bson.M{"hello": "world"}))

		log.Println("Adding collections - ", currCollection.Name())
	}

	return
}

func (oplogCtx *Oplog) Run() (err error) {
	if err = oplogCtx.Connect(); err != nil {
		return
	}
	for {
		time.Sleep(5 * time.Second)
	}
	return
}
