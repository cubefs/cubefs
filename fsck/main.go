// Copyright 2020 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"

	"github.com/cubefs/cubefs/fsck/cmd"
	"github.com/cubefs/cubefs/util/log"
)

func main() {
	var err error
	_, err = log.InitLog("/tmp/cfs", "fsck", log.InfoLevel, nil, log.DefaultLogLeftSpaceLimitRatio)
	if err != nil {
		fmt.Printf("Failed to init log: %v\n", err)
		os.Exit(1)
	}
	defer log.LogFlush()

	go listenPprof()

	c := cmd.NewRootCmd()
	if err := c.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed: %v\n", err)
		os.Exit(1)
	}
}

func listenPprof() {

	mainMux := http.NewServeMux()
	mux := http.NewServeMux()
	mux.Handle("/debug/pprof", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	mux.Handle("/debug/", http.HandlerFunc(pprof.Index))
	mainHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, "/debug/") {
			mux.ServeHTTP(w, req)
		} else {
			http.DefaultServeMux.ServeHTTP(w, req)
		}
	})
	mainMux.Handle("/", mainHandler)
	http.ListenAndServe("127.0.0.1:0", mainMux)
}
