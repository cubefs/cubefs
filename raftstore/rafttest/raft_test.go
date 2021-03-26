package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
)

const (
	DefaultProfPort    = 10800
	DefaultPutDataStep = 10000
)

var (
	PutDataStep int
	ProfPort    int
)

func init() {
	flag.IntVar(&PutDataStep, "put-data-step", DefaultPutDataStep, "put data step")
	flag.IntVar(&ProfPort, "prof-port", DefaultProfPort, "go pprof port")
	flag.VisitAll(func(i *flag.Flag) {
		fmt.Printf("Parsed tesing property: %v[%v]\n", i.Name, i.Value)
	})

	go func() {
		addr := fmt.Sprintf(":%v", ProfPort)
		fmt.Printf("go debug pprof listen [%v]\n", addr)
		_ = http.ListenAndServe(addr, nil)
	}()
}
