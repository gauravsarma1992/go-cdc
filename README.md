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
make run # Runs the mongo container and the mongoreplay container
make test # Runs the test suite
make build # Builds the app 
```


## TODO (sorted based on descending priority)
- Proper shutdown of the concurrent contexts
- Changing package name to mongoreplay
- Check with multiple collections
- Track the status of the progress
