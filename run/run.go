package main

import (
	"log"
	"os"

	"github.com/gauravsarma1992/mongoreplay/mongoreplay"
)

func main() {
	var (
		err      error
		oplogCtx *mongoreplay.Oplog
	)
	if oplogCtx, err = mongoreplay.New(); err != nil {
		log.Println(err)
		os.Exit(-1)
	}
	if err = oplogCtx.Run(); err != nil {
		log.Println(err)
		os.Exit(-1)
	}
}
