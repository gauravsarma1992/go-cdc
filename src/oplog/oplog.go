package oplog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/gauravsarma1992/mongoreplay/filters"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultOplogConfigFile       = "./config/oplog_config.json"
	DefaultSourceMongoConfigFile = "./config/source_mongo_config.json"
	DefaultDestMongoFile         = "./config/dest_mongo_config.json"
)

type (
	Oplog struct {
		noOfWorkers uint8
		oplogConfig *OplogConfig

		sourceMongoConfig *MongoConfig
		destMongoConfig   *MongoConfig

		srcDb          *mongo.Database
		srcCollections map[string]*mongo.Collection

		dstDb          *mongo.Database
		dstCollections map[string]*mongo.Collection

		controllers []*Controller
		closeCh     chan bool
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
		noOfWorkers:    4,
		srcCollections: make(map[string]*mongo.Collection),
		dstCollections: make(map[string]*mongo.Collection),
		closeCh:        make(chan bool),
	}
	if oplogCtx.oplogConfig, err = NewOplogConfig(); err != nil {
		return
	}
	if oplogCtx.sourceMongoConfig, err = NewMongoConfig(DefaultSourceMongoConfigFile); err != nil {
		return
	}
	if oplogCtx.destMongoConfig, err = NewMongoConfig(DefaultDestMongoFile); err != nil {
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

func NewMongoConfig(fileName string) (sourceMongoConfig *MongoConfig, err error) {
	var (
		fileB []byte
	)
	sourceMongoConfig = &MongoConfig{}
	if fileB, err = ioutil.ReadFile(fileName); err != nil {
		return
	}
	if err = json.Unmarshal(fileB, sourceMongoConfig); err != nil {
		return
	}
	if sourceMongoConfig.Host == "" {
		sourceMongoConfig.Host = "localhost"
	}
	if sourceMongoConfig.Port == "" {
		sourceMongoConfig.Port = "27017"
	}

	return
}

func (sourceMongoConfig *MongoConfig) GetUrl() (url string) {
	if sourceMongoConfig.Username != "" && sourceMongoConfig.Password != "" {
		url = fmt.Sprintf("mongodb://%s:%s@%s:%s",
			sourceMongoConfig.Username,
			sourceMongoConfig.Password,
			sourceMongoConfig.Host,
			sourceMongoConfig.Port,
		)
		return
	}
	url = fmt.Sprintf("mongodb://%s:%s/dev/?replicaSet=dbrs",
		sourceMongoConfig.Host,
		sourceMongoConfig.Port,
	)
	return
}

func (oplogCtx *Oplog) connectToDb(mongoConfig *MongoConfig, collection map[string]*mongo.Collection) (db *mongo.Database, err error) {
	var (
		client *mongo.Client
	)
	client, err = mongo.Connect(context.Background(), options.Client().ApplyURI(mongoConfig.GetUrl()))
	if err != nil {
		panic(err)
	}
	if err = client.Ping(context.Background(), nil); err != nil {
		return
	}

	db = client.Database(mongoConfig.DbName)

	for _, oplogCollection := range oplogCtx.oplogConfig.Collections {
		var (
			currCollection *mongo.Collection
		)
		currCollection = db.Collection(oplogCollection.Name)
		collection[oplogCollection.Name] = currCollection
	}
	return
}

func (oplogCtx *Oplog) Connect() (err error) {
	if oplogCtx.srcDb, err = oplogCtx.connectToDb(oplogCtx.sourceMongoConfig, oplogCtx.srcCollections); err != nil {
		return
	}
	if oplogCtx.dstDb, err = oplogCtx.connectToDb(oplogCtx.destMongoConfig, oplogCtx.dstCollections); err != nil {
		return
	}
	for _, coll := range oplogCtx.dstCollections {
		coll.Drop(context.TODO())
	}
	return
}

func (oplogCtx *Oplog) Run() (err error) {
	if err = oplogCtx.Connect(); err != nil {
		return
	}
	for collName, _ := range oplogCtx.srcCollections {
		var (
			ctrlr *Controller
		)
		if ctrlr, err = NewController(oplogCtx.srcDb, oplogCtx.srcCollections[collName], oplogCtx.dstDb, oplogCtx.dstCollections[collName]); err != nil {
			log.Println(err)
			continue
		}
		go ctrlr.Run()
	}
	<-oplogCtx.closeCh
	return
}
