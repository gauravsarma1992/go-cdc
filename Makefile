
OBJ = oplog/*
CONFIG_FOLDER = ./config 

test: $(OBJ)
	CONFIG_FOLDER=../$(CONFIG_FOLDER) go test ./oplog -v

build: $(OBJ)
	CONFIG_FOLDER=$(CONFIG_FOLDER) go build -a -o bin/oplog ./run

run: $(OBJ)
	CONFIG_FOLDER=$(CONFIG_FOLDER) go run run/run.go
