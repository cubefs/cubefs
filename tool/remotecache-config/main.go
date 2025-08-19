package main

import (
	"fmt"
	"os"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/tool/remotecache-config/cmd"
	"github.com/cubefs/cubefs/util/log"
	"github.com/spf13/cobra"
)

func runCLI() (err error) {
	var cfg *cmd.Config
	if cfg, err = cmd.LoadConfig(); err != nil {
		return fmt.Errorf("load config %v", err)
	}
	cfsCli := setupCommands(cfg)
	return cfsCli.Execute()
}

func setupCommands(cfg *cmd.Config) *cobra.Command {
	mc := master.NewMasterClient(cfg.MasterAddr, false)
	mc.SetTimeout(cfg.Timeout)
	mc.SetClientIDKey(cfg.ClientIDKey)
	cfsRootCmd := cmd.NewRootCmd(mc)
	return cfsRootCmd.CFSCmd
}

func main() {
	var err error
	_, err = log.InitLog("/tmp/remotecahce-config", "cli", log.DebugLevel, nil, log.DefaultLogLeftSpaceLimitRatio)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
	proto.DumpVersion("cfs-cli")
	master.CliPrint = true
	if err = runCLI(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		log.LogError("Error:", err)
		log.LogFlush()
		os.Exit(1)
	}
	log.LogFlush()
}
