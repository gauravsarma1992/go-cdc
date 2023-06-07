
MONGOREPLAY_FOLDER = mongoreplay/*
SEED_FOLDER = seed/*
CONFIG_FOLDER = ./config 

debug_test: $(MONGOREPLAY_FOLDER)
	CONFIG_FOLDER=../$(CONFIG_FOLDER) dlv test ./mongoreplay -v

test: $(MONGOREPLAY_FOLDER)
	CONFIG_FOLDER=../$(CONFIG_FOLDER) go test ./mongoreplay -v

build: $(MONGOREPLAY_FOLDER)
	CONFIG_FOLDER=$(CONFIG_FOLDER) go build -a -o bin/mongoreplay ./run

run: $(MONGOREPLAY_FOLDER)
	CONFIG_FOLDER=$(CONFIG_FOLDER) go run run/run.go

seed: $(SEED_FOLDER)
	CONFIG_FOLDER=$(CONFIG_FOLDER) go run seed/seed.go
