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
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/disk"
	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/diskutil"
	bloberr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/limit/keycount"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	TickInterval   = 1
	HeartbeatTicks = 30
	ExpiresTicks   = 60
)

func readFormatInfo(ctx context.Context, diskRootPath string) (
	formatInfo *core.FormatInfo, err error,
) {
	span := trace.SpanFromContextSafe(ctx)
	_, err = ioutil.ReadDir(diskRootPath)
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

func findDisk(disks []*bnapi.DiskInfo, clusterID proto.ClusterID, diskID proto.DiskID) (
	*bnapi.DiskInfo, bool) {
	for _, d := range disks {
		if d.ClusterID == clusterID && d.DiskID == diskID {
			return d, true
		}
	}
	return nil, false
}

func isAllInConfig(ctx context.Context, registeredDisks []*bnapi.DiskInfo, conf *Config) bool {
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

func (s *Service) handleDiskIOError(ctx context.Context, diskID proto.DiskID, diskErr error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("diskID:%v diskErr: %v", diskID, diskErr)

	// first: set disk broken in memory
	s.lock.RLock()
	ds, exist := s.Disks[diskID]
	s.lock.RUnlock()
	if !exist {
		span.Errorf("such disk(%v) does exist", diskID)
		return
	}

	if diskutil.IsLostDisk(ds.DiskInfo().Path) {
		lostCnt := 1
		diskStorages := s.copyDiskStorages(ctx)
		for _, dsAPI := range diskStorages {
			if dsAPI.ID() == diskID {
				continue
			}
			if diskutil.IsLostDisk(dsAPI.DiskInfo().Path) {
				lostCnt++
				span.Errorf("open diskId: %v, path: %v, disk lost", dsAPI.ID(), dsAPI.DiskInfo().Path)
			}
		}
		if lostCnt >= 3 {
			log.Fatalf("lost disk count: %v over threshold", lostCnt)
		}
	}

	ds.SetStatus(proto.DiskStatusBroken)

	_, _, shared := s.groupRun.Do(fmt.Sprintf("diskID:%d", diskID), func() (interface{}, error) {
		// second: notify cluster mgr
		for {
			err := s.ClusterMgrClient.SetDisk(ctx, diskID, proto.DiskStatusBroken)
			// error is nil or already broken status
			if err == nil || rpc.DetectStatusCode(err) == bloberr.CodeChangeDiskStatusNotAllow {
				span.Infof("set disk(%v) broken success, err:%v", diskID, err)
				break
			}
			span.Errorf("set disk(%v) broken failed: %v", diskID, err)
			time.Sleep(3 * time.Second)
		}

		// After the repair is triggered, the handle can be safely removed
		go s.waitRepairAndClose(ctx, ds)

		return nil, nil
	})

	span.Debugf("diskID:%v diskErr: %v, shared:%v", diskID, diskErr, shared)
}

func (s *Service) waitRepairAndClose(ctx context.Context, disk core.DiskAPI) {
	span := trace.SpanFromContextSafe(ctx)

	ticker := time.NewTicker(time.Duration(s.Conf.DiskStatusCheckIntervalSec) * time.Second)
	defer ticker.Stop()

	diskId := disk.ID()
	for {
		info, err := s.ClusterMgrClient.DiskInfo(ctx, diskId)
		if err != nil {
			span.Errorf("Failed get clustermgr diskinfo %v, err:%v", diskId, err)
			continue
		}
		if info.Status >= proto.DiskStatusRepairing {
			span.Infof("disk:%v path:%s status:%v", diskId, info.Path, info.Status)
			break
		}

		select {
		case <-s.closeCh:
			span.Warnf("service is closed. return")
			return
		case <-ticker.C:
		}
	}

	// after the repair is triggered, the handle can be safely removed

	span.Infof("Delete %v from the map table of the service", diskId)

	s.lock.Lock()
	delete(s.Disks, disk.ID())
	s.lock.Unlock()

	disk.ResetChunks(ctx)

	span.Infof("disk %v will gc close", diskId)
}

func setDefaultIOStat(dryRun bool) error {
	ios, err := flow.NewIOFlowStat("default", dryRun)
	if err != nil {
		return errors.New("init stat failed")
	}
	flow.SetupDefaultIOStat(ios)
	return nil
}

func (s *Service) fixDiskConf(config *core.Config) {
	config.AllocDiskID = s.ClusterMgrClient.AllocDiskID
	config.NotifyCompacting = s.ClusterMgrClient.SetCompactChunk
	config.HandleIOError = s.handleDiskIOError

	// init configs
	config.RuntimeConfig = s.Conf.DiskConfig
	// init hostInfo
	config.HostInfo = s.Conf.HostInfo
	// init metaInfo
	config.MetaConfig = s.Conf.MetaConfig
}

func NewService(conf Config) (svr *Service, err error) {
	span, ctx := trace.StartSpanFromContext(context.Background(), "NewBlobNodeService")

	configInit(&conf)

	clusterMgrCli := cmapi.New(conf.Clustermgr)
	node := cmapi.ServiceNode{
		ClusterID: uint64(conf.ClusterID),
		Name:      proto.ServiceNameBlobNode,
		Host:      conf.Host,
		Idc:       conf.IDC,
	}
	err = clusterMgrCli.RegisterService(ctx, node, TickInterval, HeartbeatTicks, ExpiresTicks)
	if err != nil {
		span.Fatalf("blobnode register to clusterMgr error:%v", err)
	}

	registeredDisks, err := clusterMgrCli.ListHostDisk(ctx, conf.Host)
	if err != nil {
		span.Errorf("Failed ListDisk from clusterMgr. err:%v", err)
		return nil, err
	}
	span.Infof("registered disks: %v", registeredDisks)

	check := isAllInConfig(ctx, registeredDisks, &conf)
	if !check {
		span.Errorf("no all registered normal disk in config")
		return nil, errors.New("registered disk not in config")
	}
	span.Infof("registered disks are all in config")

	svr = &Service{
		ClusterMgrClient: clusterMgrCli,
		Disks:            make(map[proto.DiskID]core.DiskAPI),
		Conf:             &conf,

		PutQpsLimitPerDisk:    keycount.New(conf.PutQpsLimitPerDisk),
		GetQpsLimitPerDisk:    keycount.NewBlockingKeyCountLimit(conf.GetQpsLimitPerDisk),
		GetQpsLimitPerKey:     keycount.NewBlockingKeyCountLimit(conf.GetQpsLimitPerKey),
		DeleteQpsLimitPerDisk: keycount.New(conf.DeleteQpsLimitPerDisk),
		DeleteQpsLimitPerKey:  keycount.NewBlockingKeyCountLimit(1),
		ChunkLimitPerVuid:     keycount.New(1),
		DiskLimitPerKey:       keycount.New(1),
		InspectLimiterPerKey:  keycount.New(1),

		closeCh: make(chan struct{}),
	}

	svr.ctx, svr.cancel = context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	errCh := make(chan error, len(conf.Disks))

	for _, diskConf := range conf.Disks {
		wg.Add(1)

		go func(diskConf core.Config) {
			var err error
			defer func() {
				errCh <- err
				wg.Done()
			}()

			svr.fixDiskConf(&diskConf)

			// read disk meta. get DiskID
			format, err := readFormatInfo(ctx, diskConf.Path)
			if err != nil {
				// todo: report to ums
				err = nil // skip
				span.Errorf("Failed read diskMeta:%s, err:%v. skip init", diskConf.Path, err)
				return
			}

			span.Debugf("local disk meta: %v", format)

			// found diskInfo store in cluster mgr
			diskInfo, foundInCluster := findDisk(registeredDisks, conf.ClusterID, format.DiskID)
			span.Debugf("diskInfo: %v, foundInCluster:%v", diskInfo, foundInCluster)

			nonNormal := foundInCluster && diskInfo.Status != proto.DiskStatusNormal
			if nonNormal {
				// todo: report to ums
				err = nil
				span.Warnf("disk(%v):path(%v) is not normal, skip init", format.DiskID, diskConf.Path)
				return
			}

			ds, err := disk.NewDiskStorage(svr.ctx, diskConf)
			if err != nil {
				span.Errorf("Failed Open DiskStorage. conf:%v, err:%v", diskConf, err)
				return
			}

			if !foundInCluster {
				span.Warnf("diskInfo:%v not found in clusterMgr, will register to cluster", diskInfo)
				diskInfo := ds.DiskInfo()
				err := clusterMgrCli.AddDisk(ctx, &diskInfo)
				if err != nil {
					span.Errorf("Failed register disk: %v, err:%v", diskInfo, err)
					return
				}
			}

			svr.lock.Lock()
			svr.Disks[ds.DiskID] = ds
			svr.lock.Unlock()

			span.Infof("Init disk storage, cluster:%v, diskID:%v", conf.ClusterID, format.DiskID)
		}(diskConf)
	}
	wg.Wait()

	close(errCh)
	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}

	if err = setDefaultIOStat(conf.DiskConfig.IOStatFileDryRun); err != nil {
		span.Errorf("Failed set default iostat file, err:%v", err)
		return nil, err
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

	svr.WorkerService, err = NewWorkerService(&conf.WorkerConfig, clusterMgrCli, conf.ClusterID, conf.IDC)
	if err != nil {
		span.Errorf("Failed to new worker service, err: %v", err)
		return
	}

	// background loop goroutines
	go svr.loopHeartbeatToClusterMgr()
	go svr.loopReportChunkInfoToClusterMgr()
	go svr.loopGcRubbishChunkFile()
	go svr.loopCleanExpiredStatFile()
	go svr.loopDataInspect()

	return
}
