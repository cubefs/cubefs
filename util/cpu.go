package util

import (
	"github.com/shirou/gopsutil/process"
)

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
