
OBJ = mongoreplay/*
CONFIG_FOLDER = ./config 

debug_test: $(OBJ)
	CONFIG_FOLDER=../$(CONFIG_FOLDER) dlv test ./mongoreplay -v

test: $(OBJ)
	CONFIG_FOLDER=../$(CONFIG_FOLDER) go test ./mongoreplay -v

build: $(OBJ)
	CONFIG_FOLDER=$(CONFIG_FOLDER) go build -a -o bin/mongoreplay ./run

run: $(OBJ)
	CONFIG_FOLDER=$(CONFIG_FOLDER) go run run/run.go
