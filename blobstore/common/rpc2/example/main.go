package main

import (
	"flag"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

var (
	listenrpc = "localhost:9997"
	listenon  = []string{"localhost:9998", "localhost:9999"}

	mode = flag.String("mode", "server", "run mode")
)

func main() {
	flag.Parse()

	log.SetOutputLevel(log.Ldebug)
	if *mode == "server" {
		runServer()
	} else if *mode == "client" {
		runClient()
	} else if *mode == "stream" {
		runStream()
	}
}
