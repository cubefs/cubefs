package main

import (
	"fmt"
	"os"

	"github.com/cubefs/cubefs/deploy/cmd"
	"github.com/cubefs/cubefs/util/log"
)

func init() {
	cmd.RootCmd.Flags().BoolVarP(&cmd.Version, "version", "v", false, "show version information")
	cmd.RootCmd.AddCommand(cmd.ClusterCmd)
	cmd.RootCmd.AddCommand(cmd.StartCmd)
	cmd.RootCmd.AddCommand(cmd.StopCmd)
	cmd.RootCmd.AddCommand(cmd.RestartCmd)
}

func main() {
	_, err := log.InitLog("/tmp/cfs", "deploy", log.DebugLevel, nil, log.DefaultLogLeftSpaceLimit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		log.LogFlush()
		os.Exit(1)
	}

	err = cmd.RootCmd.Execute()
	defer log.LogFlush()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		log.LogFlush()
		os.Exit(1)
	}
}
