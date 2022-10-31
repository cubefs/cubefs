package statinfo

import (
	"github.com/chubaofs/chubaofs/util/cpu"
	"github.com/chubaofs/chubaofs/util/memory"
	"github.com/chubaofs/chubaofs/util/unit"
	"os"
	"sync"
	"time"
)

const (
	CollectPointNumber = 60
)

type ProcessStatInfo struct {
	ProcessStartTime string
	MaxCPUUsage      float64
	MaxMemUsage      float64
	MaxMemUsedGB     float64
	CPUUsageList     []float64
	MemUsedGBList    []float64
	RWMutex          *sync.RWMutex
}

func NewProcessStatInfo() *ProcessStatInfo {
	var statInfo = &ProcessStatInfo{}
	statInfo.CPUUsageList = make([]float64, 0, CollectPointNumber)
	statInfo.MemUsedGBList = make([]float64, 0, CollectPointNumber)
	statInfo.RWMutex = new(sync.RWMutex)
	return statInfo
}

func (s *ProcessStatInfo) UpdateStatInfoSchedule() {
	t := time.NewTimer(time.Duration(0))
	defer t.Stop()

	for {
		select {
		case <-t.C:
			s.DoUpdateStatInfo()
			t.Reset(time.Minute * 1)
		}
	}
}

func (s *ProcessStatInfo) DoUpdateStatInfo() {
	pid := os.Getpid()
	s.UpdateMemoryInfo(pid)
	s.UpdateCPUUsageInfo(pid)
}

func (s *ProcessStatInfo) UpdateMemoryInfo(pid int) {
	var (
		memoryUsedGB float64
		memoryUsage  float64
	)
	memoryUsed, err := memory.GetProcessMemory(pid)
	if err != nil {
		return
	}
	memoryTotal, _, err := memory.GetMemInfo()
	if err != nil {
		return
	}
	//reserves a decimal fraction
	memoryUsage = unit.FixedPoint(float64(memoryUsed)/float64(memoryTotal)*100, 1)
	memoryUsedGB = unit.FixedPoint(float64(memoryUsed)/unit.GB, 1)
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	if len(s.MemUsedGBList) < CollectPointNumber {
		s.MemUsedGBList = append(s.MemUsedGBList, memoryUsedGB)
	} else {
		s.MemUsedGBList = append(s.MemUsedGBList[1:], memoryUsedGB)
	}
	if memoryUsedGB > s.MaxMemUsedGB {
		s.MaxMemUsedGB = memoryUsedGB
	}
	if memoryUsage > s.MaxMemUsage {
		s.MaxMemUsage = memoryUsage
	}
	return
}

func (s *ProcessStatInfo) UpdateCPUUsageInfo(pid int) {
	cpuUsage, err := cpu.GetProcessCPUUsage(pid)
	if err != nil {
		return
	}
	//reserves a decimal fraction
	cpuUsage = unit.FixedPoint(cpuUsage, 1)
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	if len(s.CPUUsageList) < CollectPointNumber {
		s.CPUUsageList = append(s.CPUUsageList, cpuUsage)
	} else {
		s.CPUUsageList = append(s.CPUUsageList[1:], cpuUsage)
	}
	if cpuUsage > s.MaxCPUUsage {
		s.MaxCPUUsage = cpuUsage
	}
	return
}

func (s *ProcessStatInfo) GetProcessCPUStatInfo() (cpuUsageList []float64, maxCPUUsage float64) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	cpuUsageList = s.CPUUsageList
	maxCPUUsage = s.MaxCPUUsage
	return
}

func (s *ProcessStatInfo) GetProcessMemoryStatInfo() (memoryUsedGBList []float64, maxMemoryUsedGB float64, maxMemoryUsagePercent float64) {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	memoryUsedGBList = s.MemUsedGBList
	maxMemoryUsedGB = s.MaxMemUsedGB
	maxMemoryUsagePercent = s.MaxMemUsage
	return
}
