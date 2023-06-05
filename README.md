# Mongoreplay 
CDC for MongoDB written in Golang

## Flow of copying data across collections
- Fetch the list of collections to be copied
- Revert if the collection is already created in the new DB
- Create new collection with the required indexes
- Note the current timestamp which can be used for the oplog tailing
- Start dumping the existing documents from the new collection
- Once the dumping is complete, start the oplog tailing
- Once the timestamp is nearby the current timestamp, the oplog tailing
can be shutdown

## Config files
All the config files are present in `src/oplog/config` folder
```
- source_mongo_config.json - For configuration of the source database
- dest_mongo_config.json - For configuration of the source database
- oplog_config.json - Define the collections needed to be exported and other oplog configurations
```

## To run the suite
```bash
make run # Runs the mongo container and the mongoreplay container
make test # Runs the test suite
make build # Builds the app 
```

## TODO (sorted based on descending priority)
- Proper shutdown of the concurrent contexts
- Check with multiple collections
- Track the status of the progress
