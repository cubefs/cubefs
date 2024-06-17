// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package loadutil

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/mem"
)

const PRO_MEM = "/proc/%d/status"

func GetMemoryUsedPercent() (used float64, err error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return
	}
	used = memInfo.UsedPercent
	return
}

func GetTotalMemory() (total uint64, err error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return
	}
	total = memInfo.Total
	return
}

func GetUsedMemory() (used uint64, err error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return
	}
	used = memInfo.Used
	return
}

func GetTotalSwapMemory() (total uint64, err error) {
	memInfo, err := mem.SwapMemory()
	if err != nil {
		return
	}
	total = memInfo.Total
	return
}

func IsEnableSwapMemory() (enable bool, err error) {
	total, err := GetTotalSwapMemory()
	if err != nil {
		return
	}
	enable = total != 0
	return
}

var procMemFile = fmt.Sprintf(PRO_MEM, os.Getpid())

func GetCurrentProcessMemory() (used uint64, err error) {
	fp, err := os.Open(procMemFile)
	if err != nil {
		return
	}
	defer fp.Close()
	scan := bufio.NewScanner(fp)
	for scan.Scan() {
		line := scan.Text()
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		value := strings.TrimPrefix(line, "VmRSS:")
		value = strings.TrimSpace(value)
		value = strings.TrimSuffix(value, " kB")
		used, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}
		used = used * 1024
		break
	}
	return
}

func GetGoMemStats() (heapInfo runtime.MemStats) {
	runtime.ReadMemStats(&heapInfo)
	return
}

func GetGoInUsedHeap() (size uint64) {
	size = GetGoMemStats().HeapInuse
	return
}
