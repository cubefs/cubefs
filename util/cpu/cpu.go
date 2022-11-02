package cpu

import (
	"encoding/json"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/process"
	"io/ioutil"
	"os"
	"strconv"
)

const (
	//{"Config":{"Cpu":"8","Disk":53687091200,"Memory":34359738368},"DataDevice":"dm-5","host_ip":"11.5.80.6"}
	containerConfigFilePath = "/etc/container_config_info"
)

type Config struct {
	CPUCoreNumber string `json:"Cpu"`
	Disk          uint64 `json:"Disk"`
	Memory        uint64 `json:"Memory"`
}

type ContainerConfigInfo struct {
	Config     Config `json:"Config"`
	HostIp     string `json:"host_ip"`
	DataDevice string `json:"DataDevice"`
}

func GetProcessCPUUsage(pid int) (usage float64, err error) {
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return
	}
	usage, err = p.CPUPercent()
	if err != nil {
		return
	}
	return
}

func GetCPUCoreNumber() (n int) {
	var err error
	defer func() {
		if err != nil || n == 0 {
			n, _ = cpu.Counts(false)
		}
	}()

	if _, err = os.Stat(containerConfigFilePath); err != nil {
		return
	}

	//get cpu core number from container config file, if failed, get by cpu.Counts()
	fp, err := os.OpenFile(containerConfigFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		return
	}
	config := &ContainerConfigInfo{}
	if err = json.Unmarshal(data, config); err != nil {
		return
	}
	n, err = strconv.Atoi(config.Config.CPUCoreNumber)
	if err != nil {
		return
	}
	return
}
