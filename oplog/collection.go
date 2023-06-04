package oplog

import (
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type (
	OplogCollection struct {
		Name            string            `json:"name"`
		Filters         []Filter          `json:"filters"`
		MongoCollection *mongo.Collection `json:"mongo_collection"`
		MongoDatabase   *mongo.Database   `json:"mongo_database"`
	}
	Filter struct {
		FilterKey   string `json:"filter_key"`
		FilterValue string `json:"filter_value"`
		FilterType  string `json:"filter_type"`
	}
)

func (collection *OplogCollection) AddCollectionFilter(filters bson.M) (err error) {
	if len(collection.Filters) == 0 {
		log.Println("No filters found")
		return
	}
	for _, filter := range collection.Filters {
		filterKey := fmt.Sprintf("o.%s", filter.FilterKey)
		filters[filterKey] = bson.M{filter.FilterType: filter.FilterValue}
	}
	return
}

func (collection *OplogCollection) GetOplogFilter(resumeToken *ResumeTokenStore) (filters bson.M, err error) {
	var (
		ns string
	)
	ns = fmt.Sprintf("%s.%s", collection.MongoDatabase.Name(), collection.MongoCollection.Name())
	filters = bson.M{
		"ns": ns,
		"ts": bson.M{"$gte": resumeToken.Timestamp},
	}

	if err = collection.AddCollectionFilter(filters); err != nil {
		return
	}
	return
}
