package main

import (
	"flag"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

var listenon = []string{"localhost:9998", "localhost:9999"}

var mode = flag.String("mode", "server", "run mode")

// main: go run main.go server.go client.go
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
