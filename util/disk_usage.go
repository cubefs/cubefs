package util

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/shirou/gopsutil/disk"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	_ "sync"
	"syscall"
	"time"
)

const (
	ReadOnly    = 1
	ReadWrite   = 2
	Unavailable = -1
)

type diskScore struct {
	path         string
	freeMemory   uint64
	partitionNum int
	score        float64
}

const (
	DiskStatusFile = ".diskStatus"
	DiskHangCnt    = 2
)

type FsCapMon struct {
	sync.RWMutex
	Path       string
	Total      float64
	Used       float64
	Available  float64
	Status     int8
	lastUpdate time.Time
}

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

func NewFsMon(path string) (d *FsCapMon) {
	d = new(FsCapMon)
	d.Path = path
	d.ComputeUsage()
	d.Status = ReadWrite
	d.lastUpdate = time.Now()
	return
}

func (d *FsCapMon) GetStatus() int8 {
	d.RLock()
	defer d.RUnlock()
	return d.Status
}

// Compute the disk usage
func (d *FsCapMon) ComputeUsage() (err error) {
	fs := syscall.Statfs_t{}
	err = syscall.Statfs(d.Path, &fs)
	if err != nil {
		return
	}

	d.Total = float64(fs.Blocks) * float64(fs.Bsize)
	d.Available = float64(fs.Bavail) * float64(fs.Bsize)
	d.Used = d.Total - d.Available
	log.LogDebugf("action[computeUsage] disk(%v) all(%v) available(%v) used(%v)", d.Path, d.Total, d.Available, d.Used)

	return
}

func (d *FsCapMon) isDiskErr(errMsg string) bool {
	if strings.Contains(errMsg, syscall.EIO.Error()) || strings.Contains(errMsg, syscall.EROFS.Error()) || strings.Contains(errMsg, "write disk hang") {
		return true
	}

	return false
}

func (d *FsCapMon) TriggerDiskError(err error) {
	if err == nil {
		return
	}
	if d.isDiskErr(err.Error()) {
		d.Status = Unavailable
	}

	return
}

func (d *FsCapMon) cleanDiskError() {
	if d.Status != ReadWrite {
		log.LogWarnf("clean disk(%s) status:%d--->%d", d.Path, d.Status, ReadWrite)
	}
	d.Status = ReadWrite
}

func (d *FsCapMon) updateCheckTick() {
	d.RLock()
	defer d.RUnlock()

	d.cleanDiskError()
	d.lastUpdate = time.Now()
}

func (d *FsCapMon) UpdateDiskTick() {
	var err error
	var fp *os.File
	defer func() {
		if err != nil {
			d.TriggerDiskError(err)
		}
	}()

	path := path.Join(d.Path, DiskStatusFile)
	fp, err = os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		return
	}
	defer fp.Close()
	data := []byte(DiskStatusFile)
	_, err = fp.WriteAt(data, 0)
	if err != nil {
		return
	}
	if err = fp.Sync(); err != nil {
		return
	}
	if _, err = fp.ReadAt(data, 0); err != nil {
		return
	}
	d.updateCheckTick()
	return
}

func (d *FsCapMon) CheckDiskStatus(interval time.Duration) {
	d.RLock()
	defer d.RUnlock()
	timeOutCnt := time.Since(d.lastUpdate) / interval
	// twice not update, will set unavailable, and alarm
	if timeOutCnt > DiskHangCnt {
		d.TriggerDiskError(fmt.Errorf("write disk hang, last update:%v, now:%v, cnt:%d", d.lastUpdate, time.Now(), timeOutCnt))
	}
}



// score = 1/(1+exp(1-free/allfree)) + 1/(1+exp(num/allnum))
func (ds *diskScore) computeScore(memory uint64, num int) {
	ds.score = 1/(1+math.Exp(1-float64(ds.freeMemory)/float64(memory))) + 1/(1+math.Exp(float64(ds.partitionNum)/float64(num)))
}

// select best dir by dirs , The reference parameters are the number of space remaining and partitions
func SelectDisk(dirs []string) (string, error) {

	result := make([]*diskScore, 0, len(dirs))

	var (
		sumMemory uint64
		sumCount  int
	)

	for _, path := range dirs {
		fs := syscall.Statfs_t{}
		if err := syscall.Statfs(path, &fs); err != nil {
			log.LogErrorf("statfs dir:[%s] has err:[%s]", path, err.Error())
			continue
		}
		freeMemory := fs.Bfree * uint64(fs.Bsize)

		dirs, _ := ioutil.ReadDir(path) //this only total all count in dir, so best to ensure the uniqueness of the directory

		if freeMemory < GB {
			log.LogWarnf("dir:[%s] not enough space:[%d] of disk so skip", path, freeMemory)
			continue
		}

		result = append(result, &diskScore{
			path:         path,
			freeMemory:   freeMemory,
			partitionNum: len(dirs),
		})

		sumMemory += freeMemory
		sumCount += len(dirs)
	}

	if len(result) == 0 {
		return "", fmt.Errorf("select disk got 0 result")
	}

	var max *diskScore

	for _, ds := range result {
		ds.computeScore(sumMemory, sumCount)
		if max == nil {
			max = ds
		}

		if max.score < ds.score {
			max = ds
		}
	}

	if max == nil {
		panic("impossibility")
	}

	return max.path, nil
}