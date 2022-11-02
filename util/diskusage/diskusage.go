package diskusage

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/unit"
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
	avail        uint64
	partitionNum int
	score        float64
}

const (
	DiskStatusFile           = ".diskStatus"
	DiskHangCnt              = 2
	MAXFsUsedFactor          = 0.6
	DefReservedSpaceMaxRatio = 0.05
)

type FsCapMon struct {
	sync.RWMutex
	Path          string
	IsRocksDBDisk bool
	ReservedSpace uint64
	Total         float64
	Used          float64
	Available     float64
	Status        int8
	MPCount       int
	lastUpdate    time.Time
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

func NewFsMon(path string, isRocksDBDisk bool, reservedSpace uint64) (d *FsCapMon) {
	d = new(FsCapMon)
	d.Path = path
	d.IsRocksDBDisk = isRocksDBDisk

	d.ComputeUsage()
	if isRocksDBDisk {
		reservedSpace = uint64(math.Min(float64(reservedSpace), d.Total*DefReservedSpaceMaxRatio))
	} else {
		reservedSpace = 0
	}
	d.ReservedSpace = reservedSpace

	d.Status = ReadWrite
	d.lastUpdate = time.Now()
	return
}

func (d *FsCapMon) GetStatus() int8 {
	d.RLock()
	defer d.RUnlock()
	return d.Status
}

func (d *FsCapMon) UpdateReversedSpace(space uint64) {
	d.RLock()
	defer d.RUnlock()
	if !d.IsRocksDBDisk {
		return
	}
	reservedSpace := uint64(math.Min(float64(space*unit.MB), d.Total*DefReservedSpaceMaxRatio))
	d.ReservedSpace = reservedSpace
	return
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
	d.MPCount = d.GetPartitionCount()
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

func (d *FsCapMon) GetPartitionCount() (mpCount int) {
	dirs, _ := ioutil.ReadDir(d.Path)
	for _, dir := range dirs {
		if strings.HasPrefix(dir.Name(), "partition_") {
			mpCount++
		}
	}
	return
}

// score = 1/(1+exp(1-free/allfree)) + 1/(1+exp(num/allnum))
func (ds *diskScore) computeScore(diskTotalAvail uint64, num int) {
	ds.score = 1/(1+math.Exp(1-float64(ds.avail)/float64(diskTotalAvail))) + 1/(1+math.Exp(float64(ds.partitionNum)/float64(num)))
}

// select best dir by dirs , The reference parameters are the number of space remaining and partitions
func SelectDisk(dirs []string) (string, error) {

	result := make([]*diskScore, 0, len(dirs))

	var (
		sumAvail uint64
		sumCount int
	)

	for _, dir := range dirs {
		fs := syscall.Statfs_t{}
		if err := syscall.Statfs(dir, &fs); err != nil {
			log.LogErrorf("statfs dir:[%s] has err:[%s]", dir, err.Error())
			continue
		}
		total := float64(fs.Blocks * uint64(fs.Bsize))
		avail := float64(fs.Bavail * uint64(fs.Bsize))
		if (total - avail) > total*MAXFsUsedFactor {
			log.LogWarnf("dir:[%s] not enough space:[%v] of disk so skip", dir, avail)
			continue
		}

		subDir, _ := ioutil.ReadDir(dir) //this only total all count in dir, so best to ensure the uniqueness of the directory
		result = append(result, &diskScore{
			path:         dir,
			avail:        uint64(avail),
			partitionNum: len(subDir),
		})

		sumAvail += uint64(avail)
		sumCount += len(subDir)
	}

	if len(result) == 0 {
		return "", fmt.Errorf("select disk got 0 result")
	}

	var max *diskScore
	for _, ds := range result {
		ds.computeScore(sumAvail, sumCount)
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
