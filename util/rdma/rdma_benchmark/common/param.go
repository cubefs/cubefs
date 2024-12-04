package common

import "C"
import (
	"flag"
	"fmt"
)

type Param struct {
	IoDeep int
	IoSize int
	Protocol string
	Ip string
	Port string
}

var GParam = Param{}

func init() {
	flag.IntVar(&GParam.IoDeep, "deep", 1, "io deep")
	flag.IntVar(&GParam.IoSize, "size", 4096, "io size")
	flag.StringVar(&GParam.Protocol, "protocol", "rdma", "rdma/tcp")
	flag.StringVar(&GParam.Ip, "ip", "127.0.0.1", "127.0.0.1")
	flag.StringVar(&GParam.Port, "port", "9000", "9000")
}

func ParseParam() {
	flag.Parse()
	fmt.Printf("param %v\n", GParam)
}
