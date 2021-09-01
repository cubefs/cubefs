package cutil

import (
	"encoding/json"
	"github.com/chubaofs/chubaofs/util/config"
)

type ConsoleConfig struct {
	Role             string   `json:"role"`
	Listen           string   `json:"listen"`
	LogDir           string   `json:"logDir"`
	LogLevel         string   `json:"logLevel"`
	MetaExporterPort string   `json:"metaExporterPort"`
	DataExporterPort string   `json:"dataExporterPort"`
	MasterAddr       []string `json:"masterAddr"`
	ObjectNodeDomain string   `json:"objectNodeDomain"`
	MonitorAddr      string   `json:"monitorAddr"`
	MonitorCluster   string   `json:"monitorCluster"`
}

func NewConsoleConfig(cfg *config.Config) (*ConsoleConfig, error) {
	c := &ConsoleConfig{}
	if err := json.Unmarshal(cfg.Raw, c); err != nil {
		return nil, err
	}
	return c, nil
}
