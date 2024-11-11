package main

import "os"

const (
	EnvLogFile       = "CFSAUTO_LOG_FILE"
	EnvCfsClientPath = "CFS_CLIENT_PATH"
)

var (
	defaultLogPath    = "/var/log/cfsauto.log"
	defaultClientPath = "/etc/cfs/cfs-client"
)

func getEnv(key, dft string) (val string) {
	val = os.Getenv(key)

	if val == "" {
		return dft
	}
	return val
}

// getCfsClientPath env
func getCfsClientPath() string {
	return getEnv(EnvCfsClientPath, defaultClientPath)
}
