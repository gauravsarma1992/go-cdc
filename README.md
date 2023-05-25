# Mongoreplay 
CDC for MongoDB written in Golang

## Config files
All the config files are present in `src/oplog/config` folder
```
- source_mongo_config.json - For configuration of the source database
- dest_mongo_config.json - For configuration of the source database
- oplog_config.json - Define the collections needed to be exported and other oplog configurations
```

## To run the suite
```bash
./setup_and_run # Runs the mongo container and the mongoreplay container
./setup_and_test # Runs the test suite
```


## TODO (sorted based on descending priority)
- Resume using resume token
- Filtering based on collection attributes
- Reading data from snapshot in the beginning
- Proper shutdown of the concurrent contexts
