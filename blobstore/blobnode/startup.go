// Copyright 2022 The CubeFS Authors.
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

package blobnode

import (
	"context"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/disk"
	myos "github.com/cubefs/cubefs/blobstore/blobnode/sys"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/diskutil"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	TickInterval   = 1
	HeartbeatTicks = 30
	ExpiresTicks   = 60
	LostDiskCount  = 3
)

func NewService(conf Config) (svr *Service, err error) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "NewBlobNodeService")

	configInit(&conf)

	clusterMgrCli := cmapi.New(conf.Clustermgr)

	svr = &Service{
		ClusterMgrClient: clusterMgrCli,
		Disks:            make(map[proto.DiskID]core.DiskAPI),
		Conf:             &conf,
		closeCh:          make(chan struct{}),
	}
	svr.ctx, svr.cancel = context.WithCancel(context.Background())

	// start worker service
	if conf.StartMode == proto.ServiceNameWorker || conf.StartMode == defaultServiceBothBlobNodeWorker {
		startWorkerService(ctx, svr, conf)
	}

	// start blobndoe service
	if conf.StartMode == proto.ServiceNameBlobNode || conf.StartMode == defaultServiceBothBlobNodeWorker {
		err = startBlobnodeService(ctx, svr, conf)
	}

	return
}

// call by heartbeat single, or datafile read/write concurrence
func (s *Service) handleDiskIOError(ctx context.Context, diskID proto.DiskID, diskErr error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("start to handle broken diskID:%d diskErr: %v", diskID, diskErr)

	// limit once. May be used by callback func, when concurrently read/write shard in datafile.go.
	err := s.BrokenLimitPerDisk.Acquire(diskID)
	if err != nil {
		span.Warnf("There are too many the same request of broken diskID:%d", diskID)
		return
	}
	defer s.BrokenLimitPerDisk.Release(diskID)

	// 1: set disk broken in memory
	s.lock.RLock()
	ds, exist := s.Disks[diskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("such diskID:%d does exist", diskID)
		return
	}

	if !ds.IsWritable() {
		return
	}
	ds.SetStatus(proto.DiskStatusBroken)

	// 2: check lost disk
	if diskutil.IsLostDisk(ds.DiskInfo().Path) {
		lostCnt := 0
		diskStorages := s.copyDiskStorages(ctx)
		for _, dsAPI := range diskStorages {
			if diskutil.IsLostDisk(dsAPI.DiskInfo().Path) {
				lostCnt++
				span.Errorf("open diskId: %v, path: %v, disk lost", dsAPI.ID(), dsAPI.DiskInfo().Path)
				s.reportLostDisk(&s.Conf.HostInfo, dsAPI.DiskInfo().Path) // runtime check
			}
		}
		if lostCnt >= LostDiskCount {
			log.Fatalf("lost disk count:%d over threshold:%d", lostCnt, LostDiskCount)
		}
	}

	// 3: notify cluster mgr
	for {
		err = s.ClusterMgrClient.SetDisk(ctx, diskID, proto.DiskStatusBroken)
		// error is nil or already broken status
		if err == nil || rpc.DetectStatusCode(err) == bloberr.CodeChangeDiskStatusNotAllow {
			span.Infof("set diskID:%d broken success, err:%+v", diskID, err)
			break
		}
		span.Errorf("set diskID:%d broken failed: %+v", diskID, err)
		time.Sleep(3 * time.Second)
	}
	// After the repair is triggered, the handle can be safely removed
	go s.waitRepairAndClose(ctx, ds)

	span.Warnf("end to handle broken diskID:%d diskErr: %+v", diskID, diskErr)
}

func (s *Service) getGlobalConfig(ctx context.Context, key string) (val string, err error) {
	span := trace.SpanFromContext(ctx)

	type item struct {
		val      string
		expireAt time.Time
	}

	itemVal, exist := s.globalConfig.Load(key)
	if exist {
		if !itemVal.(item).expireAt.Before(time.Now()) {
			return itemVal.(item).val, nil
		}
	}

	limitKey := "config-" + key
	ret, err, _ := s.singleFlight.Do(limitKey, func() (interface{}, error) {
		getVal, err := s.ClusterMgrClient.GetConfig(ctx, key)
		if err != nil {
			span.Warnf("get config[%s] from clustermgr failed: %s", key, err)
			if exist {
				// still update expire time even when get config from cm failed9
				s.globalConfig.Store(key, item{val: itemVal.(item).val, expireAt: time.Now().Add(10 * time.Minute)})
				return itemVal.(item).val, nil
			}
			return "", err
		}

		// update when key value change only
		if !exist || getVal != itemVal.(item).val {
			s.globalConfig.Store(key, item{val: getVal, expireAt: time.Now().Add(10 * time.Minute)})
		}
		return getVal, nil
	})

	return ret.(string), err
}

func (s *Service) waitRepairAndClose(ctx context.Context, disk core.DiskAPI) {
	span := trace.SpanFromContextSafe(ctx)

	ticker := time.NewTicker(time.Duration(s.Conf.DiskStatusCheckIntervalSec) * time.Second)
	defer ticker.Stop()

	diskID := disk.ID()
	for {
		select {
		case <-s.closeCh:
			span.Warnf("service is closed. return")
			return
		case <-ticker.C:
		}

		info, err := s.ClusterMgrClient.DiskInfo(ctx, diskID)
		if err != nil {
			span.Errorf("Failed get clustermgr diskID:%d, err:%+v", diskID, err)
			continue
		}

		if info.Status >= proto.DiskStatusRepaired {
			span.Infof("diskID:%d path:%s status:%v", diskID, info.Path, info.Status)
			break
		}
	}

	// report OK, the bad disk is already being processed
	config := disk.GetConfig()
	s.reportOnlineDisk(&config.HostInfo, config.Path)

	// after the repair is finish, the handle can be safely removed. if delete disk at repairing, access will DiskNotFound
	span.Warnf("Delete diskID:%d from the map table of the service", diskID)

	s.lock.Lock()
	delete(s.Disks, disk.ID())
	s.lock.Unlock()

	disk.ResetChunks(ctx)

	span.Infof("diskID:%d will gc close", diskID)
}

func (s *Service) handleDiskDrop(ctx context.Context, ds core.DiskAPI) {
	diskID := ds.ID()
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("diskID:%d dropped, start check clean", diskID)

	// 1. set disk dropped in memory
	ds.SetStatus(proto.DiskStatusDropped)

	// 2. check all chunks is clean: chunk handler in memory, physics chunk files
	go func() {
		ticker := time.NewTicker(time.Duration(s.Conf.DiskStatusCheckIntervalSec) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-s.closeCh:
				span.Warn("service is closed. skip check disk drop")
				return
			case <-ticker.C:
			}

			if ds.IsCleanUp(ctx) {
				break // is clean, need to delete disk handler
			}
			// not clean, wait it, next check
		}

		// 3. physics chunks is already empty, destroy disk/chunks handlers
		span.Infof("diskID:%d dropped, will gc destroy resource", diskID)
		s.lock.Lock()
		delete(s.Disks, diskID)
		s.lock.Unlock()

		span.Debugf("diskID:%d dropped, end check clean", diskID)
	}()
}

func (s *Service) fixDiskConf(config *core.Config) {
	config.AllocDiskID = s.ClusterMgrClient.AllocDiskID
	config.NotifyCompacting = s.ClusterMgrClient.SetCompactChunk
	config.HandleIOError = s.handleDiskIOError
	config.GetGlobalConfig = s.getGlobalConfig

	// init configs
	config.RuntimeConfig = s.Conf.DiskConfig
	// init hostInfo
	config.HostInfo = s.Conf.HostInfo
	// init metaInfo
	config.MetaConfig = s.Conf.MetaConfig
}

func (s *Service) handleStartDiskError(ctx context.Context, allUniqDiskPathMap map[string]*cmapi.BlobNodeDiskInfo,
	diskPath string, diskID proto.DiskID, err error,
) {
	// 1. diskID is match: both bn and cm is same disk. [not normal:skip, EIO:skip, other error:fatal]
	// 2. diskID is not match, not register to cm: bn is new disk, cm is old disk: dont need judge disk status [EIO:skip, other error:fatal]
	span := trace.SpanFromContextSafe(ctx)
	diskInfo, found := allUniqDiskPathMap[diskPath]

	if !base.IsEIO(err) {
		span.Fatalf("disk error: foundInCluster:%t, diskID:%d, path:%s, err:%+v", found, diskID, diskPath, err)
	}

	// not found, old/new disk
	if !found {
		span.Fatalf("not found in cluster: foundInCluster:%t, diskID:%d, path:%s, err:%+v", found, diskID, diskPath, err)
	}

	// found old/new disk
	if diskID == diskInfo.DiskID || diskID == 0 {
		// status is not normal: repaired/broken/repairing/dropped
		// diskID==0, both bn and cm is same disk, disk status is not normal, skip
		// diskID==0, bn is new disk, cm is old repaired disk. generally, a newly replaced disk is a broken is almost non-existent, so ignore itss
		if diskInfo.Status != proto.DiskStatusNormal {
			span.Warnf("disk[id:%d,status:%d,path:%s] is not normal, err:%+v. skip init", diskInfo.DiskID, diskInfo.Status, diskPath, err)
			return
		}

		// set broken disk, may be already mark broken
		span.Errorf("open normal disk[%d:%s] failed, err:%+v. skip init", diskInfo.DiskID, diskPath, err)
		_err := s.ClusterMgrClient.SetDisk(ctx, diskInfo.DiskID, proto.DiskStatusBroken)
		if _err != nil && rpc.DetectStatusCode(_err) != bloberr.CodeChangeDiskStatusNotAllow {
			span.Fatalf("set disk[%d:%s] broken to cm failed: %s", diskInfo.DiskID, diskPath, _err)
		}
		return
	}
}

func (s *Service) registerNode(ctx context.Context, conf *Config) error {
	span := trace.SpanFromContextSafe(ctx)
	if err := core.CheckNodeConf(&conf.HostInfo); err != nil {
		return err
	}

	nodeToCm := cmapi.BlobNodeInfo{
		NodeInfo: cmapi.NodeInfo{
			ClusterID: conf.ClusterID,
			DiskType:  conf.DiskType,
			Idc:       conf.IDC,
			Rack:      conf.Rack,
			Host:      conf.Host,
			Role:      proto.NodeRoleBlobNode,
		},
	}

	nodeID, err := s.ClusterMgrClient.AddNode(ctx, &nodeToCm)
	if err != nil && rpc.DetectStatusCode(err) != http.StatusCreated {
		return err
	}

	conf.NodeID = nodeID // we update nodeID, which can be used in the subsequent process. e.g. to add disk
	span.Infof("add node success, nodeID=%d", nodeID)
	return nil
}

func setDefaultIOStat(dryRun bool) error {
	ios, err := flow.NewIOFlowStat("default", dryRun)
	if err != nil {
		return errors.New("init stat failed")
	}
	flow.SetupDefaultIOStat(ios)
	return nil
}

func readFormatInfo(ctx context.Context, diskRootPath string) (
	formatInfo *core.FormatInfo, err error,
) {
	span := trace.SpanFromContextSafe(ctx)
	_, err = os.ReadDir(diskRootPath)
	if err != nil {
		span.Errorf("read disk root path error:%s", diskRootPath)
		return nil, err
	}
	formatInfo, err = core.ReadFormatInfo(ctx, diskRootPath)
	if err != nil {
		if os.IsNotExist(err) {
			span.Warnf("format file not exist. must be first register")
			return new(core.FormatInfo), nil
		}
		return nil, err
	}

	return formatInfo, err
}

func isAllInConfig(ctx context.Context, registeredDisks []*cmapi.BlobNodeDiskInfo, conf *Config) bool {
	span := trace.SpanFromContextSafe(ctx)
	configDiskMap := make(map[string]struct{})
	for i := range conf.Disks {
		configDiskMap[conf.Disks[i].Path] = struct{}{}
	}
	// check all registered normal disks are in config
	for _, registeredDisk := range registeredDisks {
		if registeredDisk.Status != proto.DiskStatusNormal {
			continue
		}
		if _, ok := configDiskMap[registeredDisk.Path]; !ok {
			span.Errorf("disk registered to clustermgr, but is not in config: %v", registeredDisk.Path)
			return false
		}
	}
	return true
}

func startWorkerService(ctx context.Context, svr *Service, conf Config) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debug("start worker service...")

	node := cmapi.ServiceNode{
		ClusterID: uint64(conf.ClusterID),
		Name:      proto.ServiceNameWorker,
		Host:      conf.Host,
		Idc:       conf.IDC,
	}

	err := svr.ClusterMgrClient.RegisterService(ctx, node, TickInterval, HeartbeatTicks, ExpiresTicks)
	if err != nil {
		span.Fatalf("worker register to clusterMgr error:%+v", err)
	}

	svr.WorkerService, err = NewWorkerService(&conf.WorkerConfig, svr.ClusterMgrClient, conf.ClusterID, conf.IDC)
	if err != nil {
		span.Fatalf("Failed to new worker service, err: %v", err)
	}
}

func startBlobnodeService(ctx context.Context, svr *Service, conf Config) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debug("start blobnode service...")

	node := cmapi.ServiceNode{
		ClusterID: uint64(conf.ClusterID),
		Name:      proto.ServiceNameBlobNode,
		Host:      conf.Host,
		Idc:       conf.IDC,
	}
	if err = cmapi.LoadExtendCodemode(ctx, svr.ClusterMgrClient); err != nil {
		span.Fatalf("load extend codemode from clusterMgr error:%+v", err)
	}
	for _, ecmode := range codemode.GetECCodeModes() {
		if alignedSize := ecmode.Tactic().MinShardSize; alignedSize > 0 {
			client.NopdataSize(alignedSize)
		}
	}

	err = svr.ClusterMgrClient.RegisterService(ctx, node, TickInterval, HeartbeatTicks, ExpiresTicks)
	if err != nil {
		span.Fatalf("blobnode register to clusterMgr error:%+v", err)
	}

	if err = svr.registerNode(ctx, &conf); err != nil {
		span.Fatalf("fail to register node to clusterMgr, err:%+v", err)
	}

	registeredDisks, err := svr.ClusterMgrClient.ListHostDisk(ctx, conf.Host)
	if err != nil {
		span.Errorf("Failed ListDisk from clusterMgr. err:%+v", err)
		return err
	}
	span.Infof("registered disks: %v", registeredDisks)

	check := isAllInConfig(ctx, registeredDisks, &conf)
	if !check {
		span.Errorf("no all registered normal disk in config")
		return errors.New("registered disk not in config")
	}
	span.Infof("registered disks are all in config")

	svr.Conf = &conf
	svr.ChunkLimitPerVuid = keycount.New(1)
	svr.DiskLimitRegister = keycount.New(1)
	svr.InspectLimiterPerKey = keycount.New(1)
	svr.BrokenLimitPerDisk = keycount.New(1)

	switchMgr := taskswitch.NewSwitchMgr(svr.ClusterMgrClient)
	svr.inspectMgr, err = NewDataInspectMgr(svr, conf.InspectConf, switchMgr)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	lostCnt := int32(0)

	foundDiskIDInCluster := make(map[proto.DiskID]*cmapi.BlobNodeDiskInfo)
	foundDiskPathInCluster := make(map[string]*cmapi.BlobNodeDiskInfo)
	for _, disk := range registeredDisks {
		foundDiskIDInCluster[disk.DiskID] = disk
		// this id is uniq increasing, so we take the latest(maximum) diskID in the same path
		_, exist := foundDiskPathInCluster[disk.Path]
		if !exist || foundDiskPathInCluster[disk.Path].DiskID < disk.DiskID {
			foundDiskPathInCluster[disk.Path] = disk
		}
	}

	for _, diskConf := range conf.Disks {
		wg.Add(1)

		go func(diskConf core.Config) {
			var err error
			defer wg.Done()

			svr.fixDiskConf(&diskConf)

			if diskConf.MustMountPoint && !myos.IsMountPoint(diskConf.Path) {
				span.Errorf("Path is not mount point:%s. skip init", diskConf.Path)
				lost := atomic.AddInt32(&lostCnt, 1)
				svr.reportLostDisk(&diskConf.HostInfo, diskConf.Path)
				if lost >= LostDiskCount {
					log.Fatalf("lost disk count:%d over threshold:%d", lost, LostDiskCount)
				}
				return // skip
			}

			// read disk meta. get DiskID
			format, err := readFormatInfo(ctx, diskConf.Path)
			if err != nil {
				svr.handleStartDiskError(ctx, foundDiskPathInCluster, diskConf.Path, 0, err)
				return // skip
			}
			span.Debugf("local disk meta: %v", format)

			// found diskInfo store in cluster mgr
			var ds core.DiskAPI
			if format.DiskID == 0 {
				// new disk
				diskInfo, foundPathInCluster := foundDiskPathInCluster[diskConf.Path]
				span.Debugf("diskInfo:%v, foundInCluster:%t", diskInfo, foundPathInCluster)
				if foundPathInCluster && diskInfo.Status != proto.DiskStatusRepaired {
					// new disk, old disk is repairing
					span.Fatalf("disk[%d:%s] is not repaired, refuse replace disk", diskInfo.DiskID, diskConf.Path)
				}
			} else {
				// old disk
				diskInfo, foundIDInCluster := foundDiskIDInCluster[format.DiskID]
				if foundIDInCluster && diskConf.Path != diskInfo.Path {
					span.Fatalf("disk not match, format[%v], diskInfo[%v]", format, diskInfo)
				}
				if foundIDInCluster && diskInfo.Status != proto.DiskStatusNormal {
					// status is repaired/broken/repairing/dropped, skip
					span.Warnf("disk[%d:%s] is not normal, skip init", diskInfo.DiskID, diskConf.Path)
					return // skip
				}
			}

			ds, err = disk.NewDiskStorage(svr.ctx, diskConf)
			if err != nil {
				svr.handleStartDiskError(ctx, foundDiskPathInCluster, diskConf.Path, format.DiskID, err)
				return
			}

			// new disk, register to cm: not found in cm, or format.diskID==0 and old disk is repaired
			diskInfo, foundIDInCluster := foundDiskIDInCluster[format.DiskID]
			if format.DiskID == 0 || !foundIDInCluster {
				span.Warnf("diskInfo:%v not found in cm, will register to cm, nodeID:%d", diskInfo, conf.NodeID)
				dsInfo := ds.DiskInfo() // get nodeID to add disk
				err = svr.ClusterMgrClient.AddDisk(ctx, &dsInfo)
				if err != nil {
					span.Fatalf("Failed register disk: %v, err:%+v", dsInfo, err)
					return
				}
			}

			svr.lock.Lock()
			svr.Disks[ds.ID()] = ds
			svr.lock.Unlock()

			svr.reportOnlineDisk(&diskConf.HostInfo, diskConf.Path) // restart, normal disk
			span.Infof("Init disk storage, cluster:%d, diskID:%d", conf.ClusterID, ds.ID())
		}(diskConf)
	}
	wg.Wait()

	if err = setDefaultIOStat(conf.DiskConfig.IOStatFileDryRun); err != nil {
		span.Errorf("Failed set default iostat file, err:%v", err)
		return err
	}

	callBackFn := func(conf []byte) error {
		_, ctx := trace.StartSpanFromContext(ctx, "")
		c := Config{}
		if err = config.LoadData(&c, conf); err != nil {
			log.Errorf("reload fail to load config, err: %v", err)
			return err
		}
		// limit
		svr.changeLimit(ctx, c)
		// qos
		err := svr.changeQos(ctx, c)
		return err
	}
	config.Register(callBackFn)

	// background loop goroutines
	go svr.loopHeartbeatToClusterMgr()
	go svr.loopReportChunkInfoToClusterMgr()
	go svr.loopGcRubbishChunkFile()
	go svr.loopCleanExpiredStatFile()
	go svr.inspectMgr.loopDataInspect()

	return
}
