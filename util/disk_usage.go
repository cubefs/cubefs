package util

import (
	"github.com/shirou/gopsutil/disk"
)

func GetDiskTotal(path string) (total uint64, err error) {
	var (
		usageStat = new(disk.UsageStat)
	)

	if usageStat, err = disk.Usage(path); err != nil {
		return
	}

	total = usageStat.Total
	return
}
