package util

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

const MEMINFO = "/proc/meminfo"

func GetMemInfo() (total, used uint64, err error) {
	fp, err := os.Open(MEMINFO)
	if err != nil {
		return
	}
	defer fp.Close()
	var (
		val    uint64
		free   uint64
		buffer uint64
		cached uint64
	)
	scan := bufio.NewScanner(fp)
	for scan.Scan() {
		line := scan.Text()
		fields := strings.Split(line, ":")
		if len(fields) != 2 {
			continue
		}
		key := fields[0]
		value := strings.TrimSpace(fields[1])
		value = strings.Replace(value, " kB", "", -1)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}
		switch key {
		case "MemTotal":
			total = val * KB
		case "MemFree":
			free = val * KB
		case "Buffers":
			buffer = val * KB
		case "Cached":
			cached = val * KB
		}
	}
	used = total - free - buffer - cached
	return
}
