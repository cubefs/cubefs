package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"testing"
)

const (
	DefaultProfPort    = 10800
	DefaultPutDataStep = 10000
)

var (
	PutDataStep int
	ProfPort    int
)

type RaftTestConfig struct {
	name     string
	isLease  bool
	mode     RaftMode
	testFunc func(t *testing.T, testName string, isLease bool, mode RaftMode)
}

func init() {
	flag.IntVar(&PutDataStep, "put-data-step", DefaultPutDataStep, "put data step")
	flag.IntVar(&ProfPort, "prof-port", DefaultProfPort, "go pprof port")
	flag.VisitAll(func(i *flag.Flag) {
		output("Parsed tesing property: %v[%v]", i.Name, i.Value)
	})

	go func() {
		addr := fmt.Sprintf(":%v", ProfPort)
		output("go debug pprof listen [%v]", addr)
		_ = http.ListenAndServe(addr, nil)
	}()
}
