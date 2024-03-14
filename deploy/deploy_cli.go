package main

import (
	"fmt"
	"os"

	"github.com/cubefs/cubefs/deploy/cmd"
)

func init() {
	cmd.RootCmd.Flags().BoolVarP(&cmd.Version, "version", "v", false, "show version information")
	cmd.RootCmd.AddCommand(cmd.ClusterCmd)
	cmd.RootCmd.AddCommand(cmd.StartCmd)
	cmd.RootCmd.AddCommand(cmd.StopCmd)
	cmd.RootCmd.AddCommand(cmd.RestartCmd)
}

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
