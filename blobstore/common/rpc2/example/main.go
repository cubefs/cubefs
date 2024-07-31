package main

import (
	"flag"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

var listenon = []string{"localhost:9998", "localhost:9999"}

var client = flag.Bool("client", false, "is client or server")

// main: go run main.go server.go client.go
func main() {
	flag.Parse()

	log.SetOutputLevel(log.Ldebug)
	if *client {
		runClient()
	} else {
		runServer()
	}
}
