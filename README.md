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
./setup_and_run
./setup_and_test
```


## TODO
- Filtering based on collection attributes
- Reading data from snapshot in the beginning
- Resume using resume token
- Proper shutdown of the concurrent contexts
