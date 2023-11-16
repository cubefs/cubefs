// Copyright 2018 The CubeFS Authors.
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

package datanode

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/async"
	"github.com/cubefs/cubefs/util/concurrent"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/multirate"
	"github.com/cubefs/cubefs/util/statistics"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/shirou/gopsutil/load"
	"golang.org/x/time/rate"
)

var (
	// RegexpDataPartitionDir validates the directory name of a data partition.
	RegexpDataPartitionDir, _ = regexp.Compile("^datapartition_(\\d)+_(\\d)+$")
)

const (
	ExpiredPartitionPrefix = "expired_"

	LatestFlushTimeFile     = "LATEST_FLUSH"
	TempLatestFlushTimeFile = ".LATEST_FLUSH"
)

var (
	regexpDiskPath = regexp.MustCompile("^(/(\\w|-)+)+(:(\\d)+)?$")
)

type DiskPath struct {
	path     string
	reserved uint64
}

func (p *DiskPath) Path() string {
	return p.path
}

func (p *DiskPath) Reserved() uint64 {
	return p.reserved
}

func (p *DiskPath) SetReserved(reserved uint64) {
	p.reserved = reserved
}

func (p *DiskPath) String() string {
	return fmt.Sprintf("DiskPath(path=%v, reserved=%v)", p.path, p.reserved)
}

func ParseDiskPath(str string) (p *DiskPath, success bool) {
	if !regexpDiskPath.MatchString(str) {
		return
	}
	var parts = strings.Split(str, ":")
	p = &DiskPath{
		path: parts[0],
		reserved: func() uint64 {
			if len(parts) > 1 {
				var val, _ = strconv.ParseUint(parts[1], 10, 64)
				return val
			}
			return 0
		}(),
	}
	success = true
	return
}

type DiskConfig struct {
	Reserved                 uint64
	MaxErrCnt                int
	MaxFDLimit               uint64     // 触发强制FD淘汰策略的阈值
	ForceFDEvictRatio        unit.Ratio // 强制FD淘汰比例
	FixTinyDeleteRecordLimit uint64
	RepairTaskLimit          uint64
}

// Disk represents the structure of the disk
type Disk struct {
	sync.RWMutex
	Path        string
	ReadErrCnt  uint64 // number of read errors
	WriteErrCnt uint64 // number of write errors

	Total       uint64
	Used        uint64
	Available   uint64
	Unallocated uint64
	Allocated   uint64

	MaxErrCnt     int // maximum number of errors
	Status        int // disk status such as READONLY
	ReservedSpace uint64

	RejectWrite  bool
	partitionMap map[uint64]*DataPartition
	space        *SpaceManager

	// Parallel limit control
	fixTinyDeleteRecordLimit     uint64 // Limit for parallel fix tiny delete record tasks
	executingFixTinyDeleteRecord uint64 // Count of executing fix tiny delete record tasks
	repairTaskLimit              uint64 // Limit for parallel data repair tasks
	executingRepairTask          uint64 // Count of executing data repair tasks
	limitLock                    sync.Mutex

	// Runtime statistics
	fdCount           int64
	maxFDLimit        uint64
	forceEvictFDRatio unit.Ratio

	forceFlushFDParallelism uint64 // 控制Flush文件句柄的并发度

	latestFlushTimeOnInit int64 // Disk 实例初始化时加载到的该磁盘最近一次Flush数据的时间

	issueFixConcurrentLimiter *concurrent.Limiter // 修复服务器故障导致的不安全数据的并发限制器
	limiter                   *multirate.MultiLimiter

	monitorData  []*statistics.MonitorData
	interceptors storage.IOInterceptors

	// sfx compressible ssd attribute
	IsSfx              bool
	devName            string
	totalPhysicalSpace uint64 //total physical space .Byte
	freePhysicalSpace  uint64 //free physical space .Byte
	PhysicalUsedRatio  uint32 //physical space usage ratio
	CompressionRatio   uint32 //full disk compression ratio
}

type CheckExpired func(id uint64) bool

func OpenDisk(path string, config *DiskConfig, space *SpaceManager, parallelism int, limiter *multirate.MultiLimiter, expired CheckExpired) (d *Disk, err error) {
	_, err = os.Stat(path)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			d = nil
		}
	}()

	d = &Disk{
		Path:                      path,
		ReservedSpace:             config.Reserved,
		MaxErrCnt:                 config.MaxErrCnt,
		RejectWrite:               false,
		space:                     space,
		partitionMap:              make(map[uint64]*DataPartition),
		fixTinyDeleteRecordLimit:  config.FixTinyDeleteRecordLimit,
		repairTaskLimit:           config.RepairTaskLimit,
		fdCount:                   0,
		maxFDLimit:                config.MaxFDLimit,
		forceEvictFDRatio:         config.ForceFDEvictRatio,
		forceFlushFDParallelism:   DefaultForceFlushFDParallelismOnDisk,
		issueFixConcurrentLimiter: concurrent.NewLimiter(DefaultIssueFixConcurrencyOnDisk),
		limiter:                   limiter,
		monitorData:               statistics.InitMonitorData(statistics.ModelDataNode),
	}

	d.initInterceptors()

	d.IsSfx, d.devName = GetDevCheckSfx(d.Path)
	if !d.IsSfx {
		log.LogInfof("%s not on sfx csd\n", d.Path)
	}

	if err = d.computeUsage(); err != nil {
		return
	}
	d.updateSpaceInfo()
	if err = d.loadLatestFlushTime(); err != nil {
		return
	}

	if err = d.loadPartitions(parallelism, expired); err != nil {
		d = nil
		return
	}

	async.RunWorker(d.managementScheduler, func(i interface{}) {
		log.LogCriticalf("Disk %v: management scheduler occurred panic: %v\nCallstack:\n%v",
			path, i, string(debug.Stack()))
	})
	async.RunWorker(d.flushDeleteScheduler, func(i interface{}) {
		log.LogCriticalf("Disk %v: flush delete scheduler occurred panic: %v\nCallStack:\n%v",
			path, i, string(debug.Stack()))
	})
	async.RunWorker(d.crcComputationScheduler, func(i interface{}) {
		log.LogCriticalf("Disk %v: CRC computation scheduler occurred panic: %v\nCallStack:\n%v",
			path, i, string(debug.Stack()))
	})
	async.RunWorker(d.flushFPScheduler, func(i interface{}) {
		log.LogCriticalf("Disk %v: FD Flush scheduler occurred panic: %v\nCallStack:\n%v",
			path, i, string(debug.Stack()))
	})
	return
}

func (d *Disk) initInterceptors() {
	var (
		unifiedDiskPath = strings.Trim(strings.ReplaceAll(d.Path, "/", "_"), "_")

		tpKeyDiskIOWrite  = fmt.Sprintf("diskwrite_%s", unifiedDiskPath)
		tpKeyDiskIORead   = fmt.Sprintf("diskread_%s", unifiedDiskPath)
		tpKeyDiskIODelete = fmt.Sprintf("diskdelete_%s", unifiedDiskPath)
		tpKeyDiskIOSync   = fmt.Sprintf("disksync_%s", unifiedDiskPath)
	)
	const (
		ctxKeyExporterTPObject = byte(0)
		ctxKeyMonitorTPObject  = byte(1)
	)
	d.interceptors.Register(storage.IOWrite,
		storage.NewFuncInterceptor(
			func() (ctx context.Context, err error) {
				ctx = context.Background()
				ctx = context.WithValue(ctx, ctxKeyExporterTPObject, exporter.NewModuleTPUs(tpKeyDiskIOWrite))
				ctx = context.WithValue(ctx, ctxKeyMonitorTPObject, d.monitorData[proto.ActionDiskIOWrite].BeforeTp())
				return
			},
			func(ctx context.Context, n int64, err error) {
				d.triggerDiskError(err)
				ctx.Value(ctxKeyExporterTPObject).(exporter.TP).Set(nil)
				ctx.Value(ctxKeyMonitorTPObject).(*statistics.TpObject).AfterTp(uint64(n))
			}))
	d.interceptors.Register(storage.IORead,
		storage.NewFuncInterceptor(
			func() (ctx context.Context, err error) {
				ctx = context.Background()
				ctx = context.WithValue(ctx, ctxKeyExporterTPObject, exporter.NewModuleTPUs(tpKeyDiskIORead))
				ctx = context.WithValue(ctx, ctxKeyMonitorTPObject, d.monitorData[proto.ActionDiskIORead].BeforeTp())
				return
			},
			func(ctx context.Context, n int64, err error) {
				d.triggerDiskError(err)
				ctx.Value(ctxKeyExporterTPObject).(exporter.TP).Set(nil)
				ctx.Value(ctxKeyMonitorTPObject).(*statistics.TpObject).AfterTp(uint64(n))
			}))
	d.interceptors.Register(storage.IORemove,
		storage.NewFuncInterceptor(
			func() (ctx context.Context, err error) {
				ctx = context.Background()
				ctx = context.WithValue(ctx, ctxKeyExporterTPObject, exporter.NewModuleTPUs(tpKeyDiskIODelete))
				ctx = context.WithValue(ctx, ctxKeyMonitorTPObject, d.monitorData[proto.ActionDiskIODelete].BeforeTp())
				return
			},
			func(ctx context.Context, n int64, err error) {
				d.triggerDiskError(err)
				ctx.Value(ctxKeyExporterTPObject).(exporter.TP).Set(nil)
				ctx.Value(ctxKeyMonitorTPObject).(*statistics.TpObject).AfterTp(uint64(n))
			}))
	d.interceptors.Register(storage.IOSync,
		storage.NewFuncInterceptor(
			func() (ctx context.Context, err error) {
				ctx = context.Background()
				ctx = context.WithValue(ctx, ctxKeyExporterTPObject, exporter.NewModuleTPUs(tpKeyDiskIOSync))
				ctx = context.WithValue(ctx, ctxKeyMonitorTPObject, d.monitorData[proto.ActionDiskIOSync].BeforeTp())
				return
			},
			func(ctx context.Context, n int64, err error) {
				d.triggerDiskError(err)
				ctx.Value(ctxKeyExporterTPObject).(exporter.TP).Set(nil)
				ctx.Value(ctxKeyMonitorTPObject).(*statistics.TpObject).AfterTp(uint64(n))
			}))
}

func (d *Disk) SetForceFlushFDParallelism(parallelism uint64) {
	if parallelism <= 0 {
		parallelism = DefaultForceFlushFDParallelismOnDisk
	}
	if parallelism != d.forceFlushFDParallelism {
		if log.IsDebugEnabled() {
			log.LogDebugf("Disk %v: flush FD parallelism changed, prev %v, new %v", d.Path, d.forceFlushFDParallelism, parallelism)
		}
		d.forceFlushFDParallelism = parallelism
	}
}

func (d *Disk) IncreaseFDCount() {
	atomic.AddInt64(&d.fdCount, 1)
}

func (d *Disk) DecreaseFDCount() {
	atomic.AddInt64(&d.fdCount, -1)
}

// PartitionCount returns the number of partitions in the partition map.
func (d *Disk) PartitionCount() int {
	d.RLock()
	defer d.RUnlock()
	return len(d.partitionMap)
}

func (d *Disk) canRepairOnDisk() bool {
	d.limitLock.Lock()
	defer d.limitLock.Unlock()
	if d.repairTaskLimit <= 0 {
		return false
	}
	if d.executingRepairTask >= d.repairTaskLimit {
		return false
	}
	d.executingRepairTask++
	return true
}

func (d *Disk) finishRepairTask() {
	d.limitLock.Lock()
	defer d.limitLock.Unlock()
	if d.executingRepairTask > 0 {
		d.executingRepairTask--
	}
}

func (d *Disk) computeUsage() (err error) {
	if d.IsSfx {
		err = d.computeUsageOnSFXDevice()
		return
	}
	err = d.computeUsageOnStdDevice()
	return
}

// computeUsageOnSFXDevice computes the disk usage on SFX device
func (d *Disk) computeUsageOnSFXDevice() (err error) {
	if d.IsSfx {
		var dStatus sfxStatus
		dStatus, err = GetSfxStatus(d.devName)
		if err != nil {
			return
		}
		d.RLock()
		defer d.RUnlock()
		total := int64(dStatus.totalPhysicalCapability) - int64(d.ReservedSpace)
		if total <0 {
			total=0
		}
		d.Total=uint64(total)
		available := int64(dStatus.freePhysicalCapability) - int64(d.ReservedSpace)
		if available < 0 {
			available = 0
		}
		d.Available = uint64(available)
		used:= int64(dStatus.totalPhysicalCapability) - int64(dStatus.freePhysicalCapability)
		if used <0 {
			used=0
		}
		d.Used=uint64(used)

		allocatedSize := uint64(0)
		for _, dp := range d.partitionMap {
			allocatedSize += uint64(dp.Size())
		}
		atomic.StoreUint64(&d.Allocated, uint64(allocatedSize))
		unallocated := d.Total - allocatedSize
		if unallocated < 0 {
			unallocated = 0
		}
		d.Unallocated = unallocated
		d.RejectWrite = d.Available <= 0

		d.PhysicalUsedRatio = dStatus.physicalUsageRatio
		d.CompressionRatio = dStatus.compRatio
		log.LogDebugf("SfxDiskComputeUsage disk(%v) totalPhysicalSpace(%v) freePhysicalSpace(%v) PhysicalUsedRatio(%v) CompressionRatio(%v)",
			d.devName, d.Total, d.Available, d.PhysicalUsedRatio, d.CompressionRatio)
	}
	return
}

// computeUsageOnStdDevice computes the disk usage on standard device
func (d *Disk) computeUsageOnStdDevice() (err error) {
	fs := syscall.Statfs_t{}
	if err = syscall.Statfs(d.Path, &fs); err != nil {
		d.incReadErrCnt()
		return
	}
	d.RLock()
	defer d.RUnlock()
	//  total := math.Max(0, int64(fs.Blocks*uint64(fs.Bsize)- d.ReservedSpace))
	total := int64(fs.Blocks*uint64(fs.Bsize) - d.ReservedSpace)
	if total < 0 {
		total = 0
	}
	d.Total = uint64(total)
	//  available := math.Max(0, int64(fs.Bavail*uint64(fs.Bsize) - d.ReservedSpace))
	available := int64(fs.Bavail*uint64(fs.Bsize) - d.ReservedSpace)
	if available < 0 {
		available = 0
	}
	d.Available = uint64(available)

	used:= int64(fs.Blocks*fs.Bsize - fs.Bavail*fs.Bsize)
	if used <0 {
		used=0
	}
	d.Used=uint64(used)

	allocatedSize := int64(0)
	for _, dp := range d.partitionMap {
		allocatedSize += int64(dp.Size())
	}
	atomic.StoreUint64(&d.Allocated, uint64(allocatedSize))
	//  unallocated = math.Max(0, total - allocatedSize)
	unallocated := total - allocatedSize
	if unallocated < 0 {
		unallocated = 0
	}
	d.Unallocated = uint64(unallocated)

	d.RejectWrite = d.Available <= 0

	if log.IsDebugEnabled() {
		log.LogDebugf("Disk %v: computed usage: Capacity %v, Available %v, Used %v, Allocated %v, Unallocated %v",
			d.Path, d.Total, d.Available, d.Used, allocatedSize, unallocated)
	}

	return
}

func (d *Disk) incReadErrCnt() {
	atomic.AddUint64(&d.ReadErrCnt, 1)
}

func (d *Disk) incWriteErrCnt() {
	atomic.AddUint64(&d.WriteErrCnt, 1)
}

func (d *Disk) flushFPScheduler() {
	flushFDSecond := d.space.flushFDIntervalSec
	if flushFDSecond == 0 {
		flushFDSecond = DefaultForceFlushFDSecond
	}
	forceFlushFDTicker := time.NewTicker(time.Duration(flushFDSecond) * time.Second)
	defer func() {
		forceFlushFDTicker.Stop()
	}()
	for {
		select {
		case <-forceFlushFDTicker.C:
			if !gHasLoadDataPartition {
				continue
			}
			avg, err := load.Avg()
			if err != nil {
				log.LogErrorf("Disk %v: get host load value failed: %v", d.Path, err)
				continue
			}
			if avg.Load1 > 1000.0 {
				if log.IsWarnEnabled() {
					log.LogWarnf("Disk %v: skip flush FD: host load value larger than 1000", d.Path)
				}
				continue
			}
			d.RLock()
			var partitions = make([]*DataPartition, 0, len(d.partitionMap))
			for _, partition := range d.partitionMap {
				partitions = append(partitions, partition)
			}
			d.RUnlock()
			d.forcePersistPartitions(partitions)
			if flushFDSecond > 0 {
				forceFlushFDTicker.Reset(time.Duration(flushFDSecond) * time.Second)
			}
		}
		if d.maybeUpdateFlushFDInterval(flushFDSecond) {
			log.LogDebugf("action[startFlushFPScheduler] disk(%v) update ticker from(%v) to (%v)", d.Path, flushFDSecond, d.space.flushFDIntervalSec)
			oldFlushFDSecond := flushFDSecond
			flushFDSecond = d.space.flushFDIntervalSec
			if flushFDSecond > 0 {
				forceFlushFDTicker.Reset(time.Duration(flushFDSecond) * time.Second)
			} else {
				flushFDSecond = oldFlushFDSecond
			}
		}
	}
}

func (d *Disk) maybeUpdateFlushFDInterval(oldVal uint32) bool {
	if d.space.flushFDIntervalSec > 0 && oldVal != d.space.flushFDIntervalSec {
		return true
	}
	return false
}

func (d *Disk) managementScheduler() {
	var (
		updateSpaceInfoTicker        = time.NewTicker(5 * time.Second)
		checkStatusTicker            = time.NewTicker(time.Minute * 2)
		evictFDTicker                = time.NewTicker(time.Minute * 5)
		forceEvictFDTicker           = time.NewTicker(time.Second * 10)
		evictExtentDeleteCacheTicker = time.NewTicker(time.Minute * 10)
	)
	defer func() {
		updateSpaceInfoTicker.Stop()
		checkStatusTicker.Stop()
		evictFDTicker.Stop()
		forceEvictFDTicker.Stop()
		evictExtentDeleteCacheTicker.Stop()
	}()
	for {
		select {
		case <-updateSpaceInfoTicker.C:
			if err := d.computeUsage(); err != nil {
				log.LogErrorf("Disk %v: compute usage failed: %v", d.Path, err)
			}
			d.updateSpaceInfo()
		case <-checkStatusTicker.C:
			d.checkDiskStatus()
		case <-evictFDTicker.C:
			d.evictExpiredFileDescriptor()
		case <-forceEvictFDTicker.C:
			d.forceEvictFileDescriptor()
		case <-evictExtentDeleteCacheTicker.C:
			d.evictExpiredExtentDeleteCache()
		}
	}
}

func (d *Disk) flushDeleteScheduler() {
	var (
		flushDeleteTicker = time.NewTicker(time.Minute)
	)
	for {
		select {
		case <-flushDeleteTicker.C:
			if !gHasLoadDataPartition {
				continue
			}
			avg, err := load.Avg()
			if err != nil {
				log.LogErrorf("Disk %v: get host load value failed: %v", d.Path, err)
				continue
			}
			if avg.Load1 > 1000.0 {
				if log.IsWarnEnabled() {
					log.LogWarnf("Disk %v: skip flush delete: host load value larger than 1000", d.Path)
				}
				continue
			}
			d.flushDelete()
		}
	}
}

func (d *Disk) flushDelete() {
	d.RLock()
	var partitionCh = make(chan *DataPartition, len(d.partitionMap))
	for _, partition := range d.partitionMap {
		partitionCh <- partition
	}
	d.RUnlock()
	close(partitionCh)

	var wg = new(sync.WaitGroup)
	var worker = func() {
		defer wg.Done()
		var (
			err       error
			partition *DataPartition
		)
		for {
			if partition = <-partitionCh; partition == nil {
				return
			}
			if _, err = partition.FlushDelete(); err != nil {
				log.LogErrorf("Disk %v: DP(%v) flush delete failed: %v", d.Path, partition.partitionID, err)
				continue
			}
		}
	}

	for i := 0; i < DefaultDeletionConcurrencyOnDisk; i++ {
		wg.Add(1)
		async.RunWorker(worker)
	}
	wg.Wait()
}

func (d *Disk) crcComputationScheduler() {
	var timer = time.NewTimer(0)
	for {
		<-timer.C
		avg, err := load.Avg()
		if err != nil {
			log.LogErrorf("Disk %v: get host load value failed: %v", d.Path, err)
			timer.Reset(time.Minute)
			continue
		}
		if avg.Load1 > 1000.0 {
			if log.IsWarnEnabled() {
				log.LogWarnf("Disk %v: skip compute CRC: host load value larger than 1000", d.Path)
			}
			timer.Reset(time.Minute)
			continue
		}
		d.WalkPartitions(func(u uint64, partition *DataPartition) bool {
			partition.ExtentStore().AutoComputeExtentCrc()
			return true
		})
		timer.Reset(time.Minute)
	}
}

func (d *Disk) SetFixTinyDeleteRecordLimitOnDisk(value uint64) {
	d.limitLock.Lock()
	defer d.limitLock.Unlock()
	if d.fixTinyDeleteRecordLimit != value {
		log.LogInfof("action[updateTaskExecutionLimit] disk(%v) change fixTinyDeleteRecordLimit from(%v) to(%v)", d.Path, d.fixTinyDeleteRecordLimit, value)
		d.fixTinyDeleteRecordLimit = value
	}
}

func (d *Disk) SetRepairTaskLimitOnDisk(value uint64) {
	d.limitLock.Lock()
	defer d.limitLock.Unlock()
	if d.repairTaskLimit != value {
		log.LogInfof("action[updateTaskExecutionLimit] disk(%v) change repairTaskLimit from(%v) to(%v)", d.Path, d.repairTaskLimit, value)
		d.repairTaskLimit = value
	}
}

const (
	DiskStatusFile = ".diskStatus"
)

func (d *Disk) checkDiskStatus() {
	path := path.Join(d.Path, DiskStatusFile)
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		d.triggerDiskError(err)
		return
	}
	defer fp.Close()
	data := []byte(DiskStatusFile)
	_, err = fp.WriteAt(data, 0)
	if err != nil {
		d.triggerDiskError(err)
		return
	}
	if err = fp.Sync(); err != nil {
		d.triggerDiskError(err)
		return
	}
	if _, err = fp.ReadAt(data, 0); err != nil {
		d.triggerDiskError(err)
		return
	}
}

func (d *Disk) triggerDiskError(err error) {
	if err == nil {
		return
	}
	if IsDiskErr(err) {
		mesg := fmt.Sprintf("disk path %v error on %v", d.Path, LocalIP)
		exporter.Warning(mesg)
		log.LogErrorf(mesg)
		d.ForceExitRaftStore()
		d.Status = proto.Unavailable
	}
	return
}

func (d *Disk) updateSpaceInfo() {
	if d.Status == proto.Unavailable {
		mesg := fmt.Sprintf("disk path %v error on %v", d.Path, LocalIP)
		log.LogErrorf(mesg)
		exporter.Warning(mesg)
		d.ForceExitRaftStore()
	} else if d.Available <= 0 {
		d.Status = proto.ReadOnly
	} else {
		d.Status = proto.ReadWrite
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("Disk %v: updated space info: total(%v) available(%v) remain(%v) "+
			"restSize(%v) maxErrs(%v) readErrs(%v) writeErrs(%v) status(%v)", d.Path,
			d.Total, d.Available, d.Unallocated, d.ReservedSpace, d.MaxErrCnt, d.ReadErrCnt, d.WriteErrCnt, d.Status)
	}
	return
}

// AttachDataPartition adds a data partition to the partition map.
func (d *Disk) AttachDataPartition(dp *DataPartition) {
	d.Lock()
	d.partitionMap[dp.ID()] = dp
	d.Unlock()

	_ = d.computeUsage()
}

// DetachDataPartition removes a data partition from the partition map.
func (d *Disk) DetachDataPartition(dp *DataPartition) {
	d.Lock()
	delete(d.partitionMap, dp.ID())
	d.Unlock()

	_ = d.computeUsage()
}

// GetDataPartition returns the data partition based on the given partition ID.
func (d *Disk) GetDataPartition(partitionID uint64) (partition *DataPartition) {
	d.RLock()
	defer d.RUnlock()
	return d.partitionMap[partitionID]
}

func (d *Disk) ForceExitRaftStore() {
	partitionList := d.DataPartitionList()
	for _, partitionID := range partitionList {
		if partition := d.GetDataPartition(partitionID); partition != nil {
			partition.partitionStatus = proto.Unavailable
			partition.stopRaft()
		}
	}
}

// DataPartitionList returns a list of the data partitions
func (d *Disk) DataPartitionList() (partitionIDs []uint64) {
	d.Lock()
	defer d.Unlock()
	partitionIDs = make([]uint64, 0, len(d.partitionMap))
	for _, dp := range d.partitionMap {
		partitionIDs = append(partitionIDs, dp.ID())
	}
	return
}

func unmarshalPartitionName(name string) (partitionID uint64, partitionSize int, err error) {
	arr := strings.Split(name, "_")
	if len(arr) != 3 {
		err = fmt.Errorf("error DataPartition name(%v)", name)
		return
	}
	if partitionID, err = strconv.ParseUint(arr[1], 10, 64); err != nil {
		return
	}
	if partitionSize, err = strconv.Atoi(arr[2]); err != nil {
		return
	}
	return
}

func (d *Disk) isPartitionDir(filename string) (isPartitionDir bool) {
	isPartitionDir = RegexpDataPartitionDir.MatchString(filename)
	return
}

// RestorePartition reads the files stored on the local disk and restores the data partitions.
func (d *Disk) loadPartitions(parallelism int, expired CheckExpired) (err error) {

	var (
		partitionID uint64
		diskFp      *os.File
	)

	if diskFp, err = os.Open(d.Path); err != nil {
		return
	}

	var filenames []string
	if filenames, err = diskFp.Readdirnames(-1); err != nil {
		return
	}

	if parallelism < 1 {
		parallelism = 1
	}
	var loadWaitGroup = new(sync.WaitGroup)
	var filenameCh = make(chan string, parallelism)
	for i := 0; i < parallelism; i++ {
		loadWaitGroup.Add(1)
		go func() {
			defer loadWaitGroup.Done()
			var (
				filename  string
				partition *DataPartition
				loadErr   error
			)
			for {
				if filename = <-filenameCh; len(filename) == 0 {
					return
				}
				partitionFullPath := path.Join(d.Path, filename)
				startTime := time.Now()
				if partition, loadErr = d.loadPartition(partitionFullPath); loadErr != nil {
					msg := fmt.Sprintf("load partition(%v) failed: %v",
						partitionFullPath, loadErr)
					log.LogError(msg)
					exporter.Warning(msg)
					continue
				}
				log.LogInfof("DP(%v) load complete, elapsed %v",
					partitionFullPath, time.Since(startTime))
				d.AttachDataPartition(partition)
			}
		}()
	}
	go func() {
		for _, filename := range filenames {
			if !d.isPartitionDir(filename) {
				continue
			}

			if partitionID, _, err = unmarshalPartitionName(filename); err != nil {
				log.LogErrorf("action[RestorePartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
					filename, d.Path, err.Error())
				continue
			}
			if expired != nil && expired(partitionID) {
				if log.IsWarnEnabled() {
					log.LogWarnf("Disk %v: found expired DP %v (%v): rename it and you can delete it "+
						"manually", d.Path, partitionID, filename)
				}
				oldName := path.Join(d.Path, filename)
				newName := path.Join(d.Path, ExpiredPartitionPrefix+filename)
				_ = os.Rename(oldName, newName)
				continue
			}
			filenameCh <- filename
		}
		close(filenameCh)
	}()

	loadWaitGroup.Wait()
	return
}

// RestoreOnePartition restores the data partition.
func (d *Disk) RestoreOnePartition(partitionPath string) (err error) {
	var (
		partitionID uint64
		partition   *DataPartition
		dInfo       *DataNodeInfo
	)
	if len(partitionPath) == 0 {
		err = fmt.Errorf("action[RestoreOnePartition] partition path is empty")
		return
	}
	partitionFullPath := path.Join(d.Path, partitionPath)
	_, err = os.Stat(partitionFullPath)
	if err != nil {
		err = fmt.Errorf("action[RestoreOnePartition] read dir(%v) err(%v)", partitionFullPath, err)
		return
	}
	if !d.isPartitionDir(partitionPath) {
		err = fmt.Errorf("action[RestoreOnePartition] invalid partition path")
		return
	}

	if partitionID, _, err = unmarshalPartitionName(partitionPath); err != nil {
		err = fmt.Errorf("action[RestoreOnePartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
			partitionPath, d.Path, err.Error())
		return
	}

	dInfo, err = d.getPersistPartitionsFromMaster()
	if err != nil {
		return
	}
	if len(dInfo.PersistenceDataPartitions) == 0 {
		log.LogWarnf("action[RestoreOnePartition]: length of PersistenceDataPartitions is 0, ExpiredPartition check " +
			"without effect")
	}

	if isExpiredPartition(partitionID, dInfo.PersistenceDataPartitions) {
		log.LogErrorf("action[RestoreOnePartition]: find expired partition[%s], rename it and you can delete it "+
			"manually", partitionPath)
		newName := path.Join(d.Path, ExpiredPartitionPrefix+partitionPath)
		_ = os.Rename(partitionFullPath, newName)
		return
	}

	startTime := time.Now()
	if partition, err = d.loadPartition(partitionFullPath); err != nil {
		msg := fmt.Sprintf("load partition(%v) failed: %v",
			partitionFullPath, err)
		log.LogError(msg)
		exporter.Warning(msg)
		return
	}
	log.LogInfof("partition(%v) load complete cost(%v)",
		partitionFullPath, time.Since(startTime))
	d.AttachDataPartition(partition)
	d.space.AttachPartition(partition)
	return
}

func (d *Disk) WalkPartitions(f func(uint64, *DataPartition) bool) {
	if f == nil {
		return
	}
	d.Lock()
	var partitions = make([]*DataPartition, 0, len(d.partitionMap))
	for _, partition := range d.partitionMap {
		partitions = append(partitions, partition)
	}
	d.Unlock()
	for _, partition := range partitions {
		if !f(partition.ID(), partition) {
			break
		}
	}
}

func (d *Disk) AsyncLoadExtent(parallelism int) {

	if log.IsInfoEnabled() {
		log.LogInfof("Disk %v: storage lazy load start", d.Path)
	}
	var start = time.Now()
	var wg = new(sync.WaitGroup)
	var partitionCh = make(chan *DataPartition, parallelism)
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		async.RunWorker(func() {
			defer wg.Done()
			var dp *DataPartition
			for {
				if dp = <-partitionCh; dp == nil {
					return
				}
				dp.ExtentStore().Load()
			}
		})
	}
	wg.Add(1)
	async.RunWorker(func() {
		defer wg.Done()
		d.WalkPartitions(func(_ uint64, partition *DataPartition) bool {
			partitionCh <- partition
			return true
		})
		close(partitionCh)
	})
	wg.Wait()
	if log.IsInfoEnabled() {
		log.LogInfof("Disk %v: storage lazy load complete, elapsed %v", d.Path, time.Now().Sub(start))
	}
}

func (d *Disk) getPersistPartitionsFromMaster() (dInfo *DataNodeInfo, err error) {
	var dataNode *proto.DataNodeInfo
	var convert = func(node *proto.DataNodeInfo) *DataNodeInfo {
		result := &DataNodeInfo{}
		result.Addr = node.Addr
		result.PersistenceDataPartitions = node.PersistenceDataPartitions
		return result
	}
	for i := 0; i < 3; i++ {
		dataNode, err = MasterClient.NodeAPI().GetDataNode(d.space.dataNode.localServerAddr)
		if err != nil {
			log.LogErrorf("action[RestorePartition]: getDataNode error %v", err)
			continue
		}
		break
	}
	if err != nil {
		return
	}
	dInfo = convert(dataNode)
	return
}

func (d *Disk) AddSize(size uint64) {
	atomic.AddUint64(&d.Allocated, size)
}

func (d *Disk) getSelectWeight() float64 {
	return float64(atomic.LoadUint64(&d.Allocated)) / float64(d.Total)
}

// isExpiredPartition return whether one partition is expired
// if one partition does not exist in master, we decided that it is one expired partition
func isExpiredPartition(id uint64, partitions []uint64) bool {
	if len(partitions) == 0 {
		return true
	}

	for _, existId := range partitions {
		if existId == id {
			return false
		}
	}
	return true
}

func (d *Disk) canFinTinyDeleteRecord() bool {
	d.limitLock.Lock()
	defer d.limitLock.Unlock()
	if d.executingFixTinyDeleteRecord >= d.fixTinyDeleteRecordLimit {
		return false
	}
	d.executingFixTinyDeleteRecord++
	return true
}

func (d *Disk) finishFixTinyDeleteRecord() {
	d.limitLock.Lock()
	defer d.limitLock.Unlock()
	if d.executingFixTinyDeleteRecord > 0 {
		d.executingFixTinyDeleteRecord--
	}
}

func (d *Disk) evictExpiredFileDescriptor() {
	d.RLock()
	var partitions = make([]*DataPartition, 0, len(d.partitionMap))
	for _, partition := range d.partitionMap {
		partitions = append(partitions, partition)
	}
	d.RUnlock()

	for _, partition := range partitions {
		partition.EvictExpiredFileDescriptor()
	}
}

func (d *Disk) forceEvictFileDescriptor() {
	var count = atomic.LoadInt64(&d.fdCount)
	log.LogDebugf("action[forceEvictFileDescriptor] disk(%v) current FD count(%v)",
		d.Path, count)
	d.RLock()
	var partitions = make([]*DataPartition, 0, len(d.partitionMap))
	for _, partition := range d.partitionMap {
		partitions = append(partitions, partition)
	}
	d.RUnlock()
	for _, partition := range partitions {
		partition.ForceEvictFileDescriptor(d.forceEvictFDRatio)
	}
	log.LogDebugf("action[forceEvictFileDescriptor] disk(%v) evicted FD count [%v -> %v]",
		d.Path, count, atomic.LoadInt64(&d.fdCount))
}

func (d *Disk) evictExpiredExtentDeleteCache() {
	var expireTime uint64
	log.LogDebugf("action[evictExpiredExtentDeleteCache] disk(%v) evict start", d.Path)
	d.RLock()
	expireTime = d.space.normalExtentDeleteExpireTime
	var partitions = make([]*DataPartition, 0, len(d.partitionMap))
	for _, partition := range d.partitionMap {
		partitions = append(partitions, partition)
	}
	d.RUnlock()
	for _, partition := range partitions {
		partition.EvictExpiredExtentDeleteCache(int64(expireTime))
	}
	log.LogDebugf("action[evictExpiredExtentDeleteCache] disk(%v) evict end", d.Path)
}

func (d *Disk) forcePersistPartitions(partitions []*DataPartition) {
	if log.IsDebugEnabled() {
		log.LogDebugf("action[forcePersistPartitions] disk(%v) partition count(%v) begin",
			d.Path, len(partitions))
	}
	pChan := make(chan *DataPartition, len(partitions))
	for _, dp := range partitions {
		pChan <- dp
	}
	wg := new(sync.WaitGroup)
	var failedCount int64
	var parallelism = d.forceFlushFDParallelism
	if parallelism <= 0 {
		parallelism = DefaultForceFlushFDParallelismOnDisk
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("disk[%v] start to force persist partitions [parallelism: %v]", d.Path, parallelism)
	}
	var flushTime = time.Now()
	for i := uint64(0); i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case dp := <-pChan:
					if err := dp.Flush(); err != nil {
						err = errors.NewErrorf("[forcePersistPartitions]: persist all failed, partition=%d: %v", dp.config.PartitionID, err.Error())
						log.LogErrorf(err.Error())
						atomic.AddInt64(&failedCount, 1)
					}
				default:
					return
				}
			}
		}()
	}
	wg.Wait()
	close(pChan)
	if atomic.LoadInt64(&failedCount) == 0 {
		if err := d.persistLatestFlushTime(flushTime.Unix()); err != nil {
			log.LogErrorf("disk[%v] persist latest flush time failed: %v", d.Path, err)
		}
	}
	if log.IsWarnEnabled() {
		log.LogWarnf("disk[%v] flush partitions: %v/%v, elapsed %v",
			d.Path, len(partitions)-int(atomic.LoadInt64(&failedCount)), len(partitions), time.Now().Sub(flushTime))
	}
}

func (d *Disk) createFlushExtentsRater(parallelism uint64) *rate.Limiter {
	if parallelism <= 0 {
		parallelism = DefaultForceFlushFDParallelismOnDisk
	}
	var flushFDQps uint64
	if d.isSSDMediaType() {
		flushFDQps = parallelism * DefaultForceFlushDataSizeOnEachSSDDisk
	} else {
		flushFDQps = parallelism * DefaultForceFlushDataSizeOnEachHDDDisk
	}
	flushExtentsRater := rate.NewLimiter(rate.Limit(flushFDQps), int(flushFDQps))
	return flushExtentsRater
}

func (d *Disk) isSSDMediaType() bool {
	return d.space.dataNode != nil && strings.Contains(d.space.dataNode.zoneName, "ssd")
}

func (d *Disk) persistLatestFlushTime(unix int64) (err error) {

	tmpFilename := path.Join(d.Path, TempLatestFlushTimeFile)
	tmpFile, err := os.OpenFile(tmpFilename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFilename)
	}()
	if _, err = tmpFile.WriteString(fmt.Sprintf("%d", unix)); err != nil {
		return
	}
	if err = tmpFile.Sync(); err != nil {
		return
	}
	err = os.Rename(tmpFilename, path.Join(d.Path, LatestFlushTimeFile))
	log.LogInfof("Disk %v: persist latest flush time [unix: %v]", d.Path, unix)
	return
}

func (d *Disk) loadLatestFlushTime() (err error) {
	var filename = path.Join(d.Path, LatestFlushTimeFile)
	var (
		fileBytes []byte
	)
	if fileBytes, err = ioutil.ReadFile(filename); err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	if _, err = fmt.Sscanf(string(fileBytes), "%d", &d.latestFlushTimeOnInit); err != nil {
		err = nil
		return
	}
	return
}
