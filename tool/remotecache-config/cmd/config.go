package cmd

import (
	"encoding/json"
	"math"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

const (
	cmdConfigShort     = "Manage global config file"
	cmdConfigSetShort  = "set value of config file"
	cmdConfigInfoShort = "show info of config file"
)

type Config struct {
	MasterAddr  []string `json:"masterAddr"`
	Timeout     uint16   `json:"timeout"`
	ClientIDKey string   `json:"clientIDKey"`
}

var (
	defaultHomeDir, _ = os.UserHomeDir()
	defaultConfigName = ".remotecache-config.json"
	defaultConfigPath = path.Join(defaultHomeDir, defaultConfigName)
	defaultConfigData = []byte(`
{
  "masterAddr": [
    "master.cube.io"
  ],
  "timeout": 60
}
`)
	defaultConfigTimeout uint16 = 60
)

func LoadConfig() (*Config, error) {
	var err error
	var configData []byte
	if configData, err = os.ReadFile(defaultConfigPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if os.IsNotExist(err) {
		if err = os.WriteFile(defaultConfigPath, defaultConfigData, 0o600); err != nil {
			return nil, err
		}
		configData = defaultConfigData
	}
	config := &Config{}
	if err = json.Unmarshal(configData, config); err != nil {
		return nil, err
	}
	if config.Timeout == 0 {
		config.Timeout = defaultConfigTimeout
	}
	return config, nil
}

func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   CliResourceConfig,
		Short: cmdConfigShort,
	}
	cmd.AddCommand(newConfigSetCmd())
	cmd.AddCommand(newConfigInfoCmd())
	return cmd
}

func newConfigSetCmd() *cobra.Command {
	var optMasterHosts string
	var optTimeout string
	cmd := &cobra.Command{
		Use:   CliOpSet,
		Short: cmdConfigSetShort,
		Long:  `Set the config file`,
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				errout(err)
			}()
			tmp, _ := strconv.Atoi(optTimeout)
			if tmp > math.MaxUint16 {
				stdoutln("Please reset timeout. Input less than math.MaxUint16")
				return
			}
			timeOut := uint16(tmp)
			if optMasterHosts == "" {
				stdout("Please set addr. Input 'cfs-cli config set -h' for help.\n")
				return
			}

			if timeOut == 0 {
				stdout("timeOut %v is invalid.\n", timeOut)
				return
			}

			if err = setConfig(optMasterHosts, timeOut); err != nil {
				return
			}
			stdout("Config has been set successfully!\n")
		},
	}
	cmd.Flags().StringVar(&optMasterHosts, "addr", "",
		"Specify master address {HOST}:{PORT}[,{HOST}:{PORT}]")
	cmd.Flags().StringVar(&optTimeout, "timeout", "60", "Specify timeout for requests [Unit: s]")
	return cmd
}

func setConfig(masterHosts string, timeout uint16) (err error) {
	var config *Config
	if config, err = LoadConfig(); err != nil {
		return
	}
	hosts := strings.Split(masterHosts, ",")
	if masterHosts != "" && len(hosts) > 0 {
		config.MasterAddr = hosts
	}
	if timeout != 0 {
		config.Timeout = timeout
	}
	var configData []byte
	if configData, err = json.Marshal(config); err != nil {
		return
	}
	if err = os.WriteFile(defaultConfigPath, configData, 0o600); err != nil {
		return
	}
	return nil
}

func newConfigInfoCmd() *cobra.Command {
	var optFilterStatus string
	var optFilterWritable string
	cmd := &cobra.Command{
		Use:   CliOpInfo,
		Short: cmdConfigInfoShort,
		Run: func(cmd *cobra.Command, args []string) {
			config, err := LoadConfig()
			errout(err)
			printConfigInfo(config)
		},
	}
	cmd.Flags().StringVar(&optFilterWritable, "filter-writable", "", "Filter node writable status")
	cmd.Flags().StringVar(&optFilterStatus, "filter-status", "", "Filter node status [Active, Inactive]")
	return cmd
}

func printConfigInfo(config *Config) {
	stdout("Config info:\n")
	stdout("  Master  Address    : %v\n", config.MasterAddr)
	stdout("  Request Timeout [s]: %v\n", config.Timeout)
}
