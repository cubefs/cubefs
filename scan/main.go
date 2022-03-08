package main

import (
	"flag"
	"fmt"
	"github.com/cubefs/cubefs/scan/cmd"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/jacobsa/daemonize"
	syslog "log"
	"os"
	"path"
)

var (
	master          = flag.String("m", "", "input master")
	dataPartitionId = flag.Int("d", 0, "input dataPartitionId")
	extentId        = flag.Int("e", 0, "input extentId")
)

func main() {
	_, err := log.InitLog("/home/amy/log", "bad_crc", log.DebugLevel, nil)
	if err != nil {
		err = errors.NewErrorf("Init log dir fail: %v\n", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer log.LogFlush()

	outputFilePath := path.Join("/home/amy/log", "bad_crc", "output.log")
	outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		err = errors.NewErrorf("Open output file failed: %v\n", err)
		fmt.Println(err)
		daemonize.SignalOutcome(err)
		os.Exit(1)
	}
	defer func() {
		outputFile.Sync()
		outputFile.Close()
	}()
	syslog.SetOutput(outputFile)
	flag.Parse()
	scan := cmd.NewScan(*master)
	scan.ReadByPartitionIdAndExtentId(uint64(*dataPartitionId), uint64(*extentId))
}
