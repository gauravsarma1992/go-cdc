package main

import (
	"log"
	"os"

	"github.com/go-cdc/oplog"
)

func main() {
    var (
        err error
        oplogCtx *oplog.Oplog
    )
    if oplogCtx, err = oplog.New(); err != nil {
        log.Println(err)
        os.Exit(-1)
    }
    if err = oplogCtx.Run(); err != nil {
        log.Println(err)
        os.Exit(-1)
    }
}
