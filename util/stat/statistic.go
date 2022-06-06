// Copyright 2022 The ChubaoFS Authors.
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

package stat

import "C"
import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	Stat_Module        = "mem_stat"
	FileNameDateFormat = "20060102150405"
	ShiftedExtension   = ".old"
	PRO_MEM            = "/proc/%d/status"

	F_OK                = 0
	MaxTimeoutLevel     = 3
	DefaultStatInterval = 60                // 60 seconds
	DefaultStatLogSize  = 200 * 1024 * 1024 // 200M
	DefaultHeadRoom     = 50 * 1024         // 50G
	MaxReservedDays     = 7 * 24 * time.Hour
)

var DefaultTimeOutUs = [3]uint32{100000, 500000, 1000000}

var re = regexp.MustCompile(`\([0-9]*\)`)

type ShiftedFile []os.FileInfo

func (f ShiftedFile) Less(i, j int) bool {
	return f[i].ModTime().Before(f[j].ModTime())
}

func (f ShiftedFile) Len() int {
	return len(f)
}

func (f ShiftedFile) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

type typeInfo struct {
	typeName  string
	allCount  uint32
	failCount uint32
	maxTime   time.Duration
	minTime   time.Duration
	allTimeUs time.Duration
	timeOut   [MaxTimeoutLevel]uint32
}

type Statistic struct {
	logDir        string
	logMaxSize    int64
	logBaseName   string
	pid           int
	lastClearTime time.Time
	timeOutUs     [MaxTimeoutLevel]uint32
	typeInfoMap   map[string]*typeInfo
	closeStat     bool
	useMutex      bool
	sync.Mutex
}

var gSt *Statistic = nil

func NewStatistic(dir, logModule string, logMaxSize int64, timeOutUs [MaxTimeoutLevel]uint32, useMutex bool) (*Statistic, error) {
	dir = path.Join(dir, logModule)
	fi, err := os.Stat(dir)
	if err != nil {
		os.MkdirAll(dir, 0755)
	} else {
		if !fi.IsDir() {
			return nil, errors.New(dir + " is not a directory")
		}
	}
	_ = os.Chmod(dir, 0766)
	logName := path.Join(dir, Stat_Module)
	st := &Statistic{
		logDir:        dir,
		logMaxSize:    logMaxSize,
		logBaseName:   logName,
		pid:           os.Getpid(),
		lastClearTime: time.Time{},
		timeOutUs:     [MaxTimeoutLevel]uint32{},
		typeInfoMap:   make(map[string]*typeInfo),
		closeStat:     false,
		useMutex:      useMutex,
		Mutex:         sync.Mutex{},
	}

	st.timeOutUs = timeOutUs
	gSt = st
	go st.flushScheduler()
	return st, nil
}

func (st *Statistic) flushScheduler() {
	lastStatTime := time.Now()
	statGap := time.Duration(DefaultStatInterval)
	for {
		if time.Since(lastStatTime) < (statGap * time.Second) {
			time.Sleep(1 * time.Second)
			continue
		}
		err := WriteStat()
		if err != nil {
			log.LogErrorf("WriteStat error: %v", err)
		}

		lastStatTime = time.Now()

		fs := syscall.Statfs_t{}
		if err := syscall.Statfs(st.logDir, &fs); err != nil {
			log.LogErrorf("Get fs stat failed, err: %v", err)
			continue
		}
		diskSpaceLeft := int64(fs.Bavail * uint64(fs.Bsize))
		diskSpaceLeft -= DefaultHeadRoom * 1024 * 1024
		removeLogFile(diskSpaceLeft, Stat_Module)
	}
}

func removeLogFile(diskSpaceLeft int64, module string) {
	fInfos, err := ioutil.ReadDir(gSt.logDir)
	if err != nil {
		log.LogErrorf("ReadDir failed, logDir: %s, err: %v", gSt.logDir, err)
		return
	}
	var needDelFiles ShiftedFile
	for _, info := range fInfos {
		if deleteFileFilter(info, diskSpaceLeft, module) {
			needDelFiles = append(needDelFiles, info)
		}
	}
	sort.Sort(needDelFiles)
	for _, info := range needDelFiles {
		if err = os.Remove(path.Join(gSt.logDir, info.Name())); err != nil {
			log.LogErrorf("Remove log file failed, logFileName: %s, err: %v", info.Name(), err)
			continue
		}
		diskSpaceLeft += info.Size()
		if diskSpaceLeft > 0 && time.Since(info.ModTime()) < MaxReservedDays {
			break
		}
	}
}

func deleteFileFilter(info os.FileInfo, diskSpaceLeft int64, module string) bool {
	if diskSpaceLeft <= 0 {
		return info.Mode().IsRegular() && strings.HasSuffix(info.Name(), ShiftedExtension) && strings.HasPrefix(info.Name(), module)
	}
	return time.Since(info.ModTime()) > MaxReservedDays && strings.HasSuffix(info.Name(), ShiftedExtension) && strings.HasPrefix(info.Name(), module)
}

func CloseStat() {
	if gSt == nil {
		return
	}

	gSt.closeStat = true
}

func BeginStat() (bgTime *time.Time) {
	bg := time.Now()
	return &bg
}

func EndStat(typeName string, err error, bgTime *time.Time, statCount uint32) error {
	if gSt == nil {
		return nil
	}

	if gSt.closeStat {
		return nil
	}

	if gSt.useMutex {
		gSt.Lock()
		defer gSt.Unlock()
	}

	if err != nil {
		newErrStr := string(re.ReplaceAll([]byte(err.Error()), []byte("(xxx)")))
		baseLen := len(typeName) + 2
		if len(newErrStr)+baseLen > 41 {
			typeName = typeName + "[" + newErrStr[:41-baseLen] + "]"
		} else {
			typeName = typeName + "[" + newErrStr + "]"
		}
	}

	return addStat(typeName, err, bgTime, statCount)
}

func WriteStat() error {
	if gSt == nil {
		return nil
	}

	if gSt.useMutex {
		gSt.Lock()
		defer gSt.Unlock()
	}

	logFileName := gSt.logBaseName + ".log"
	statFile, err := os.OpenFile(logFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.LogErrorf("OpenLogFile failed, logFileName: %s, err: %v\n", logFileName, err)
		return fmt.Errorf("OpenLogFile failed, logFileName %s\n", logFileName)
	}
	defer statFile.Close()

	statSpan := time.Since(gSt.lastClearTime) / 1e9
	ioStream := bufio.NewWriter(statFile)
	defer ioStream.Flush()

	fmt.Fprintf(ioStream, "\n===============  Statistic in %ds, %s  =====================\n",
		statSpan, time.Now().Format("2006-01-02 15:04:05"))

	if virt, res, err := GetProcessMemory(gSt.pid); err != nil {
		log.LogErrorf("Get process memory failed, err: %v", err)
		fmt.Fprintf(ioStream, "Get Mem Failed.\n")
	} else {
		fmt.Fprintf(ioStream, "Mem Used(kB): VIRT %-10d   RES %-10d\n", virt, res)
	}

	fmt.Fprintf(ioStream, "%-42s|%10s|%8s|%8s|%8s|%8s|%8s|%8s|%8s|\n",
		"", "TOTAL", "FAILED", "AVG(ms)", "MAX(ms)", "MIN(ms)",
		">"+strconv.Itoa(int(gSt.timeOutUs[0])/1000)+"ms",
		">"+strconv.Itoa(int(gSt.timeOutUs[1])/1000)+"ms",
		">"+strconv.Itoa(int(gSt.timeOutUs[2])/1000)+"ms")

	typeNames := make([]string, 0)
	for typeName := range gSt.typeInfoMap {
		typeNames = append(typeNames, typeName)
	}

	sort.Strings(typeNames)
	for _, typeName := range typeNames {
		typeInfo := gSt.typeInfoMap[typeName]
		avgUs := int32(0)
		if typeInfo.allCount > 0 {
			avgUs = int32(typeInfo.allTimeUs / time.Duration(typeInfo.allCount))
		}

		fmt.Fprintf(ioStream, "%-42s|%10d|%8d|%8.2f|%8.2f|%8.2f|%8d|%8d|%8d|\n",
			typeInfo.typeName, typeInfo.allCount, typeInfo.failCount,
			float32(avgUs)/1000, float32(typeInfo.maxTime)/1000, float32(typeInfo.minTime)/1000,
			typeInfo.timeOut[0], typeInfo.timeOut[1], typeInfo.timeOut[2])
	}

	fmt.Fprintf(ioStream, "-------------------------------------------------------------------"+
		"--------------------------------------------------\n")

	// clear stat
	gSt.lastClearTime = time.Now()
	gSt.typeInfoMap = make(map[string]*typeInfo)

	shiftFiles()

	return nil
}

func ClearStat() {
	if gSt == nil {
		return
	}

	if gSt.useMutex {
		gSt.Lock()
		defer gSt.Unlock()
	}

	gSt.lastClearTime = time.Now()
	gSt.typeInfoMap = make(map[string]*typeInfo)
}

func addStat(typeName string, err error, bgTime *time.Time, statCount uint32) error {
	if gSt == nil {
		return nil
	}

	if len(typeName) == 0 {
		return fmt.Errorf("AddStat fail, typeName %s\n", typeName)
	}

	if typeInfo, ok := gSt.typeInfoMap[typeName]; ok {
		typeInfo.allCount += statCount
		if err != nil {
			typeInfo.failCount += statCount
		}
		addTime(typeInfo, bgTime)
		return nil
	}

	typeInfo := &typeInfo{
		typeName:  typeName,
		allCount:  statCount,
		failCount: 0,
		maxTime:   0,
		minTime:   0,
		allTimeUs: 0,
		timeOut:   [3]uint32{},
	}

	if err != nil {
		typeInfo.failCount = statCount
	}

	gSt.typeInfoMap[typeName] = typeInfo
	addTime(typeInfo, bgTime)

	return nil
}

func addTime(typeInfo *typeInfo, bgTime *time.Time) {
	if bgTime == nil {
		return
	}

	timeCostUs := time.Since(*bgTime) / 1e3
	if timeCostUs == 0 {
		return
	}

	if timeCostUs >= time.Duration(gSt.timeOutUs[0]) && timeCostUs < time.Duration(gSt.timeOutUs[1]) {
		typeInfo.timeOut[0]++
	} else if timeCostUs >= time.Duration(gSt.timeOutUs[1]) && timeCostUs < time.Duration(gSt.timeOutUs[2]) {
		typeInfo.timeOut[1]++
	} else if timeCostUs > time.Duration(gSt.timeOutUs[2]) {
		typeInfo.timeOut[2]++
	}

	if timeCostUs > typeInfo.maxTime {
		typeInfo.maxTime = timeCostUs
	}
	if typeInfo.minTime == 0 || timeCostUs < typeInfo.minTime {
		typeInfo.minTime = timeCostUs
	}

	typeInfo.allTimeUs += timeCostUs
}

func shiftFiles() error {
	logFileName := gSt.logBaseName + ".log"
	fileInfo, err := os.Stat(logFileName)
	if err != nil {
		return err
	}

	if fileInfo.Size() < gSt.logMaxSize {
		return nil
	}

	if syscall.Access(logFileName, F_OK) == nil {
		logNewFileName := logFileName + "." + time.Now().Format(
			FileNameDateFormat) + ShiftedExtension
		if syscall.Rename(logFileName, logNewFileName) != nil {
			log.LogErrorf("RenameFile failed, logFileName: %s, logNewFileName: %s, err: %v\n",
				logFileName, logNewFileName, err)
			return fmt.Errorf("RenameFile failed, logFileName %s, logNewFileName %s\n",
				logFileName, logNewFileName)
		}
	}

	return nil
}

func StatBandWidth(typeName string, Size uint32) {
	EndStat(typeName+"[FLOW_KB]", nil, nil, Size/1024)
}

func GetMememory() (Virt, Res uint64, err error) {
	return GetProcessMemory(gSt.pid)
}

func GetProcessMemory(pid int) (Virt, Res uint64, err error) {
	proFileName := fmt.Sprintf(PRO_MEM, pid)
	fp, err := os.Open(proFileName)
	if err != nil {
		return
	}
	defer fp.Close()
	scan := bufio.NewScanner(fp)
	for scan.Scan() {
		line := scan.Text()
		fields := strings.Split(line, ":")
		key := fields[0]
		if key == "VmRSS" {
			value := strings.TrimSpace(fields[1])
			value = strings.Replace(value, " kB", "", -1)
			Res, err = strconv.ParseUint(value, 10, 64)
			if err != nil {
				return
			}
		} else if key == "VmSize" {
			value := strings.TrimSpace(fields[1])
			value = strings.Replace(value, " kB", "", -1)
			Virt, err = strconv.ParseUint(value, 10, 64)
			if err != nil {
				return
			}
		} else {
			continue
		}
	}
	return
}
