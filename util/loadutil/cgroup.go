// Copyright 2026 The CubeFS Authors.
//
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
	"strconv"
	"strings"
)

// cgroup file paths. All reads return errors on non-Linux systems (e.g.
// macOS during development), causing callers to fall back to host metrics
// transparently — no build-tag split required.
const (
	cgroupV2Controllers = "/sys/fs/cgroup/cgroup.controllers"

	// cgroup v2 (unified hierarchy)
	cgroupV2MemMax      = "/sys/fs/cgroup/memory.max"
	cgroupV2MemCurrent  = "/sys/fs/cgroup/memory.current"
	cgroupV2CPUMax      = "/sys/fs/cgroup/cpu.max"
	cgroupV2CPUStat     = "/sys/fs/cgroup/cpu.stat"

	// cgroup v1 (legacy)
	cgroupV1MemLimit     = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	cgroupV1MemUsage     = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
	cgroupV1CPUQuota     = "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us"
	cgroupV1CPUPeriod    = "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us"
	cgroupV1CPUAcctUsage = "/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage"
)

// cgroupV2 reports whether the host uses cgroup v2 (unified hierarchy),
// detected by the presence of /sys/fs/cgroup/cgroup.controllers.
func cgroupV2() bool {
	_, err := os.Stat(cgroupV2Controllers)
	return err == nil
}

// readSingleIntFile reads the first whitespace-trimmed integer from path.
// The sentinel string "max" maps to -1 to signal "unlimited".
func readSingleIntFile(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return -1, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// GetContainerMemoryLimitBytes returns the cgroup memory limit in bytes.
// Returns (0, nil) when no limit is set (unlimited or not in a container).
func GetContainerMemoryLimitBytes() (uint64, error) {
	if cgroupV2() {
		v, err := readSingleIntFile(cgroupV2MemMax)
		if err != nil {
			return 0, err
		}
		if v < 0 { // "max" sentinel → unlimited
			return 0, nil
		}
		return uint64(v), nil
	}
	// cgroup v1: limit_in_bytes is set to a value close to MaxInt64 when
	// there is no explicit limit.
	v, err := readSingleIntFile(cgroupV1MemLimit)
	if err != nil {
		return 0, err
	}
	if v <= 0 || uint64(v) > 1<<40 { // > 1 TiB: treat as unlimited
		return 0, nil
	}
	return uint64(v), nil
}

// GetContainerMemoryUsageBytes returns the container's current memory usage
// in bytes.
func GetContainerMemoryUsageBytes() (uint64, error) {
	if cgroupV2() {
		v, err := readSingleIntFile(cgroupV2MemCurrent)
		if err != nil {
			return 0, err
		}
		return uint64(v), nil
	}
	v, err := readSingleIntFile(cgroupV1MemUsage)
	if err != nil {
		return 0, err
	}
	if v < 0 {
		return 0, nil
	}
	return uint64(v), nil
}

// GetContainerCPUCores returns the container's CPU quota expressed as a core
// count (quota/period). Returns (0, nil) when no CPU limit is set.
func GetContainerCPUCores() (float64, error) {
	if cgroupV2() {
		data, err := os.ReadFile(cgroupV2CPUMax)
		if err != nil {
			return 0, err
		}
		parts := strings.Fields(strings.TrimSpace(string(data)))
		if len(parts) < 2 || parts[0] == "max" {
			return 0, nil // unlimited
		}
		quota, err := strconv.ParseFloat(parts[0], 64)
		if err != nil {
			return 0, err
		}
		period, err := strconv.ParseFloat(parts[1], 64)
		if err != nil || period == 0 {
			return 0, err
		}
		return quota / period, nil
	}
	// cgroup v1
	quota, err := readSingleIntFile(cgroupV1CPUQuota)
	if err != nil || quota <= 0 { // -1 means unlimited
		return 0, err
	}
	period, err := readSingleIntFile(cgroupV1CPUPeriod)
	if err != nil || period <= 0 {
		return 0, err
	}
	return float64(quota) / float64(period), nil
}

// GetContainerCPUUsageMicros returns the container's cumulative CPU usage in
// microseconds. The value is monotonically increasing — callers must diff two
// readings over a known wall-clock interval to derive a utilisation percent.
func GetContainerCPUUsageMicros() (int64, error) {
	if cgroupV2() {
		f, err := os.Open(cgroupV2CPUStat)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "usage_usec ") {
				return strconv.ParseInt(strings.TrimPrefix(line, "usage_usec "), 10, 64)
			}
		}
		return 0, fmt.Errorf("usage_usec not found in %s", cgroupV2CPUStat)
	}
	// cgroup v1: cpuacct.usage is nanoseconds → convert to microseconds.
	v, err := readSingleIntFile(cgroupV1CPUAcctUsage)
	if err != nil {
		return 0, err
	}
	return v / 1000, nil
}
