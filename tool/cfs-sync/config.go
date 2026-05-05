package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type cliConfig struct {
	MasterAddr []string `json:"masterAddr"`
	Timeout    uint16   `json:"timeout"`
}

func loadCLIConfig() (*cliConfig, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return &cliConfig{}, nil
	}
	data, err := os.ReadFile(filepath.Join(home, ".cfs-cli.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return &cliConfig{}, nil
		}
		return nil, fmt.Errorf("read ~/.cfs-cli.json: %w", err)
	}
	cfg := &cliConfig{}
	if err = json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse ~/.cfs-cli.json: %w", err)
	}
	return cfg, nil
}

// resolveMasters returns the master addresses to use.
// The --master flag takes precedence over ~/.cfs-cli.json.
func resolveMasters(flagMaster string, cfg *cliConfig) ([]string, error) {
	if flagMaster != "" {
		return strings.Split(flagMaster, ","), nil
	}
	if len(cfg.MasterAddr) > 0 {
		return cfg.MasterAddr, nil
	}
	return nil, fmt.Errorf("no master address: set --master or configure ~/.cfs-cli.json")
}
