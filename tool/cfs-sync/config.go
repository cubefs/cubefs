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

	// RDMA client config (optional). Absent / RDMAEnable=false → SDK
	// uses pure TCP exactly as before; existing deployments need no
	// config change. Defaults below mirror FUSE mount options for
	// consistency across client surfaces.
	RDMAEnable           bool  `json:"rdmaEnable"`
	RDMAPortShift        int64 `json:"rdmaPortShift"`
	RDMANumSlots         int64 `json:"rdmaNumSlots"`
	RDMASlotSize         int64 `json:"rdmaSlotSize"`
	RDMAMaxConns         int64 `json:"rdmaMaxConns"`
	RDMAMinPayloadBytes  int64 `json:"rdmaMinPayloadBytes"`
	RDMABusySpinCount    int64 `json:"rdmaBusySpinCount"`
	RDMAYieldCount       int64 `json:"rdmaYieldCount"`
	RDMASleepThresholdUs int64 `json:"rdmaSleepThresholdUs"`
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
