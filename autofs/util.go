package main

import "os"

const EnvLogFile = "CFSAUTO_LOG_FILE"
const EnvCfsClientPath = "CFS_CLIENT_PATH"

const LogFile = "/var/log/cfsauto.log"

func getEnv(key, dft string) (val string) {
    val = os.Getenv(key)

    if val == "" {
        return dft
    }
    return val
}

// getCfsClientPath env
func getCfsClientPath() string {
    return getEnv(EnvCfsClientPath, "/etc/cfs/cfs-client")
}
