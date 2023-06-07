package mongoreplay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	ConfigFolder                 = os.Getenv("CONFIG_FOLDER")
	DefaultOplogConfigFile       = fmt.Sprintf("%s/oplog_config.json", ConfigFolder)
	DefaultSourceMongoConfigFile = fmt.Sprintf("%s/source_mongo_config.json", ConfigFolder)
	DefaultDestMongoFile         = fmt.Sprintf("%s/dest_mongo_config.json", ConfigFolder)
)

type (
	Oplog struct {
		Ctx        context.Context
		CancelFunc context.CancelFunc

		noOfWorkers uint8
		oplogConfig *OplogConfig

		sourceMongoConfig *MongoConfig
		destMongoConfig   *MongoConfig

		srcDb          *mongo.Database
		SrcCollections map[string]*OplogCollection

		dstDb          *mongo.Database
		DstCollections map[string]*OplogCollection

		controllers []*TailerManager
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
)

func New() (oplogCtx *Oplog, err error) {
	oplogCtx = &Oplog{
		noOfWorkers:    4,
		SrcCollections: make(map[string]*OplogCollection),
		DstCollections: make(map[string]*OplogCollection),
		closeCh:        make(chan bool),
	}
	oplogCtx.Ctx, oplogCtx.CancelFunc = context.WithCancel(context.Background())

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

func (oplogCtx *Oplog) connectToDb(mongoConfig *MongoConfig, collection map[string]*OplogCollection) (db *mongo.Database, err error) {
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
		oplogCollection.MongoCollection = currCollection
		oplogCollection.MongoDatabase = db

		collection[oplogCollection.Name] = &oplogCollection
	}
	return
}
func (oplogCtx *Oplog) Connect() (err error) {
	if oplogCtx.srcDb, err = oplogCtx.connectToDb(oplogCtx.sourceMongoConfig, oplogCtx.SrcCollections); err != nil {
		return
	}
	if oplogCtx.dstDb, err = oplogCtx.connectToDb(oplogCtx.destMongoConfig, oplogCtx.DstCollections); err != nil {
		return
	}
	for _, coll := range oplogCtx.DstCollections {
		coll.MongoCollection.Drop(context.TODO())
	}
	return
}

func (oplogCtx *Oplog) Run() (err error) {
	if err = oplogCtx.Connect(); err != nil {
		return
	}
	for collName := range oplogCtx.SrcCollections {
		var (
			controller *Controller
		)
		if controller, err = NewController(
			oplogCtx.Ctx,
			oplogCtx.SrcCollections[collName],
			oplogCtx.DstCollections[collName],
		); err != nil {
			log.Println(err)
			continue
		}
		go controller.Run()
	}
	<-oplogCtx.closeCh
	oplogCtx.CancelFunc()

	return
}
