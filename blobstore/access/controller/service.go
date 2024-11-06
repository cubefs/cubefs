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

package controller

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	_primaryDisk          = "_disk_"
	_primaryShardnodeDisk = "_sddisk_"
)

// HostIDC item of host with idc
type HostIDC struct {
	Host     string
	IDC      string
	Punished bool
}

// ServiceController support for both data node discovery and normal service discovery
type ServiceController interface {
	// GetServiceHost return an available service host
	GetServiceHost(ctx context.Context, name string) (host string, err error)
	// GetServiceHosts return all available service random sorted hosts
	GetServiceHosts(ctx context.Context, name string) (hosts []string, err error)
	// GetDiskHost return an disk's related data node host
	GetDiskHost(ctx context.Context, diskID proto.DiskID) (hostIDC *HostIDC, err error)
	// PunishService will punish an service host for an punishTimeSec interval
	PunishService(ctx context.Context, service, host string, punishTimeSec int)
	// PunishServiceWithThreshold will punish an service host for
	// an punishTimeSec interval if service failed times satisfied with threshold during some interval time
	PunishServiceWithThreshold(ctx context.Context, service, host string, punishTimeSec int)
	// PunishDisk will punish a disk host for an punishTimeSec interval
	PunishDisk(ctx context.Context, diskID proto.DiskID, punishTimeSec int)
	// PunishDiskWithThreshold will punish a disk host for
	// an punishTimeSec interval if disk host failed times satisfied with threshold
	PunishDiskWithThreshold(ctx context.Context, diskID proto.DiskID, punishTimeSec int)
	// GetShardnodeHost return shardnode host
	GetShardnodeHost(ctx context.Context, diskID proto.DiskID) (hostIDC *HostIDC, err error)
	// PunishShardnode will punish a shardnode disk host for an punishTimeSec interval
	PunishShardnode(ctx context.Context, diskID proto.DiskID, punishTimeSec int)
}

type (
	serviceList []*hostItem
	serviceMap  map[string]*atomic.Value
)

// hostItem represent a service or host item info
type hostItem struct {
	host string
	idc  string

	// punish time record the punish end time unix of host item
	punishTimeUnix int64
	// modify time record the last modify time unix of host item
	lastModifyTime int64
	// failedTimes record the service host failed times during some interval
	failedTimes uint32
}

func (h *hostItem) isPunish() bool {
	return time.Since(time.Unix(atomic.LoadInt64(&h.punishTimeUnix), 0)) < 0
}

// ServiceConfig service config
type ServiceConfig struct {
	ClusterID proto.ClusterID `json:"-"`
	IDC       string          `json:"-"`

	ServiceReloadSecs           int    `json:"service_reload_secs"`
	LoadDiskIntervalS           int    `json:"load_disk_interval_s"`
	DiskPunishThreshold         uint32 `json:"disk_punish_threshold"`
	DiskPunishValidIntervalS    int    `json:"disk_punish_valid_interval_s"`
	ServicePunishThreshold      uint32 `json:"service_punish_threshold"`
	ServicePunishValidIntervalS int    `json:"service_punish_valid_interval_s"`
}

type serviceControllerImpl struct {
	// allServices hold all disk/service host map, use for quickly find out
	allServices   sync.Map
	serviceHosts  serviceMap
	brokenDisks   sync.Map
	sdBrokenDisks sync.Map // shard node broken disks

	group        singleflight.Group
	serviceLocks map[string]*sync.RWMutex
	cmClient     clustermgr.APIAccess
	proxy        proxy.Cacher

	config ServiceConfig
}

// NewServiceController returns a service controller
func NewServiceController(cfg ServiceConfig,
	cmCli clustermgr.APIAccess, proxy proxy.Cacher, stopCh <-chan struct{},
) (ServiceController, error) {
	defaulter.IntegerLessOrEqual(&cfg.ServiceReloadSecs, 10)
	defaulter.IntegerLessOrEqual(&cfg.LoadDiskIntervalS, 300)
	defaulter.IntegerEqual(&cfg.DiskPunishThreshold, 3)
	defaulter.IntegerLessOrEqual(&cfg.DiskPunishValidIntervalS, 30)
	defaulter.IntegerEqual(&cfg.ServicePunishThreshold, 3)
	defaulter.IntegerLessOrEqual(&cfg.ServicePunishValidIntervalS, 30)

	controller := &serviceControllerImpl{
		serviceHosts: serviceMap{
			proto.ServiceNameProxy: &atomic.Value{},
		},
		cmClient: cmCli,
		proxy:    proxy,
		serviceLocks: map[string]*sync.RWMutex{
			proto.ServiceNameProxy: {},
		},
		config: cfg,
	}

	err := controller.load(cfg.ClusterID, cfg.IDC)
	if err != nil {
		return nil, errors.Base(err, "load service failed")
	}

	if stopCh == nil {
		return controller, nil
	}
	go func() {
		tick := time.NewTicker(time.Duration(cfg.ServiceReloadSecs) * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				if err := controller.load(cfg.ClusterID, cfg.IDC); err != nil {
					log.Warn("load timer error", err)
				}
			case <-stopCh:
				return
			}
		}
	}()
	go func() {
		tick := time.NewTicker(time.Duration(cfg.LoadDiskIntervalS) * time.Second)
		defer tick.Stop()
		for {
			controller.loadBrokenDisks()
			select {
			case <-tick.C:
			case <-stopCh:
				return
			}
		}
	}()
	return controller, nil
}

// load initial all service and service hosts
func (s *serviceControllerImpl) load(cid proto.ClusterID, idc string) error {
	span, ctx := trace.StartSpanFromContext(context.Background(), "access_cluster_service")
	span.Debug("service loader for cluster:", cid)

	serviceName := proto.ServiceNameProxy
	service, err := s.cmClient.GetService(ctx, clustermgr.GetServiceArgs{Name: serviceName})
	if err != nil {
		span.Warn("get service from cluster manager failed", err)
		return err
	}

	span.Debugf("found %d server nodes of %s in the cluster", len(service.Nodes), serviceName)
	hostItems := make(serviceList, 0, len(service.Nodes))
	for _, node := range service.Nodes {
		if node.Idc != idc {
			continue
		}
		hostItems = append(hostItems, &hostItem{idc: node.Idc, host: node.Host})
	}
	if len(hostItems) > 0 {
		for _, item := range hostItems {
			s.allServices.Store(s.getServiceKey(serviceName, item.host), item)
			span.Debugf("store node %+v", item)
		}
		s.serviceHosts[serviceName].Store(hostItems)
	}
	return nil
}

func (s *serviceControllerImpl) loadBrokenDisks() {
	_, ctx := trace.StartSpanFromContext(context.Background(), "access_cluster_load_disks")

	fnBlobnode := func(ctx context.Context, args *clustermgr.ListOptionArgs, diskMap map[proto.DiskID]struct{}) error {
		list, err := s.cmClient.ListDisk(ctx, args)
		if err != nil {
			return err
		}

		for _, disk := range list.Disks {
			diskMap[disk.DiskID] = struct{}{}
		}
		args.Marker = list.Marker
		return nil
	}

	fnShardnode := func(ctx context.Context, args *clustermgr.ListOptionArgs, diskMap map[proto.DiskID]struct{}) error {
		list, err := s.cmClient.ListShardNodeDisk(ctx, args)
		if err != nil {
			return err
		}

		for _, disk := range list.Disks {
			diskMap[disk.DiskID] = struct{}{}
		}
		args.Marker = list.Marker
		return nil
	}

	s.processBrokenDisks(ctx, fnBlobnode, &s.brokenDisks)
	s.processBrokenDisks(ctx, fnShardnode, &s.sdBrokenDisks)
}

func (s *serviceControllerImpl) processBrokenDisks(
	ctx context.Context,
	fn func(context.Context, *clustermgr.ListOptionArgs, map[proto.DiskID]struct{}) error,
	disks *sync.Map,
) {
	span := trace.SpanFromContextSafe(ctx)

	brokenDiskIDs := make(map[proto.DiskID]struct{})
	for _, st := range []proto.DiskStatus{proto.DiskStatusBroken, proto.DiskStatusRepairing} {
		span.Debugf("to load disks of cluster %d %s", s.config.ClusterID, st.String())

		args := &clustermgr.ListOptionArgs{Status: st, Count: 1 << 10}
		for {
			list, err := s.cmClient.ListDisk(ctx, args)
			err = fn(ctx, args, brokenDiskIDs)
			if err != nil {
				span.Errorf("load disks of cluster %d, err:%+v", s.config.ClusterID, err)
				return
			}
			if args.Marker = list.Marker; args.Marker <= proto.InvalidDiskID {
				break
			}
		}
	}

	// clean cached disks, ignore cases when concurrency getting disk.
	disks.Range(func(key, value interface{}) bool {
		disks.Delete(key)
		return true
	})
	if len(brokenDiskIDs) == 0 {
		return
	}
	span.Warnf("load disks of cluster %d broken %v", s.config.ClusterID, brokenDiskIDs)
	for diskID := range brokenDiskIDs {
		disks.Store(diskID, struct{}{})
	}
}

// GetServiceHost return an available service host
func (s *serviceControllerImpl) GetServiceHost(ctx context.Context, name string) (host string, err error) {
	serviceList, ok := s.serviceHosts[name].Load().(serviceList)
	if !ok {
		return "", errors.Newf("not found host of %s", name)
	}

	lock := s.getServiceLock(name)
	idx := 0

	var lastHost string
RETRY:

	lock.RLock()
	length := len(serviceList)
	if length == 0 {
		lock.RUnlock()

		span := trace.SpanFromContextSafe(ctx)
		if lastHost == "" {
			span.Errorf("no any service host of %s", name)
			return "", errors.Newf("no any host of %s", name)
		}
		span.Warnf("all host were punished of %s, return the last one %s", name, lastHost)
		return lastHost, nil
	}

	idx = rand.Intn(length)
	item := serviceList[idx]
	lastHost = item.host
	lock.RUnlock()

	if !item.isPunish() {
		return item.host, nil
	}

	lock.Lock()
	// double check
	v := serviceList[idx]
	// if serviceList[idx] still equal to item, then remove it
	if v == item {
		serviceList = append(serviceList[:idx], serviceList[idx+1:]...)
	}
	if len(serviceList) > 0 {
		s.serviceHosts[name].Store(serviceList)
	}
	lock.Unlock()

	goto RETRY
}

// GetServiceHosts return all available random-sorted hosts of service
func (s *serviceControllerImpl) GetServiceHosts(ctx context.Context, name string) (hosts []string, err error) {
	serviceList, ok := s.serviceHosts[name].Load().(serviceList)
	if !ok {
		return nil, errors.Newf("not found host of %s", name)
	}

	lock := s.getServiceLock(name)

	lock.RLock()
	length := len(serviceList)
	if length == 0 {
		lock.RUnlock()
		return nil, errors.Newf("no any host of %s", name)
	}

	hosts = make([]string, 0, length)
	for _, item := range serviceList {
		if !item.isPunish() {
			hosts = append(hosts, item.host)
		}
	}
	lock.RUnlock()

	if len(hosts) == 0 {
		return nil, errors.Newf("no available host of %s", name)
	}

	rand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})
	return hosts, nil
}

// GetDiskHost return an disk's related data node host
func (s *serviceControllerImpl) GetDiskHost(ctx context.Context, diskID proto.DiskID) (*HostIDC, error) {
	span := trace.SpanFromContextSafe(ctx)

	_, broken := s.brokenDisks.Load(diskID)

	v, ok := s.allServices.Load(s.getServiceKey(_primaryDisk, diskID))
	if ok {
		item := v.(*hostItem)
		return &HostIDC{
			Host:     item.host,
			IDC:      item.idc,
			Punished: broken || item.isPunish(),
		}, nil
	}
	ret, err, _ := s.group.Do("get-diskinfo-"+diskID.ToString(), func() (interface{}, error) {
		hosts, err := s.GetServiceHosts(ctx, proto.ServiceNameProxy)
		if err != nil {
			return nil, err
		}
		for _, host := range hosts {
			diskInfo, err := s.proxy.GetCacheDisk(ctx, host, &proxy.CacheDiskArgs{DiskID: diskID})
			if err != nil {
				span.Warnf("get disk %d from proxy %s error %s", diskID, host, err.Error())
				continue
			}
			return diskInfo, nil
		}
		return nil, errors.New("try all proxy failed")
	})
	if err != nil {
		span.Error("can't get disk host from proxy", err)
		return nil, errors.Base(err, "get disk info", diskID)
	}
	diskInfo := ret.(*clustermgr.BlobNodeDiskInfo)

	item := &hostItem{host: diskInfo.Host, idc: diskInfo.Idc}
	s.allServices.Store(s.getServiceKey(_primaryDisk, diskInfo.DiskID), item)
	return &HostIDC{
		Host:     item.host,
		IDC:      item.idc,
		Punished: broken || item.isPunish(),
	}, nil
}

func (s *serviceControllerImpl) GetShardnodeHost(ctx context.Context, diskID proto.DiskID) (hostIDC *HostIDC, err error) {
	span := trace.SpanFromContextSafe(ctx)

	_, broken := s.sdBrokenDisks.Load(diskID)
	v, ok := s.allServices.Load(s.getServiceKey(_primaryShardnodeDisk, diskID))
	if ok {
		item := v.(*hostItem)
		return &HostIDC{
			Host:     item.host,
			IDC:      item.idc,
			Punished: broken || item.isPunish(),
		}, nil
	}

	ret, err, _ := s.group.Do("get-shardnode-diskinfo-"+diskID.ToString(), func() (interface{}, error) {
		// todo: support proxy get disk host, next version
		info, err := s.cmClient.ShardNodeDiskInfo(ctx, diskID)
		if err != nil {
			return nil, err
		}

		return info, nil
	})
	if err != nil {
		span.Error("can't get shardnode disk host from cm", err)
		return nil, errors.Base(err, "get shardnode disk info", diskID)
	}
	diskInfo := ret.(*clustermgr.ShardNodeDiskInfo)

	item := &hostItem{host: diskInfo.Host, idc: diskInfo.Idc}
	s.allServices.Store(s.getServiceKey(_primaryShardnodeDisk, diskInfo.DiskID), item)
	return &HostIDC{
		Host:     item.host,
		IDC:      item.idc,
		Punished: broken || item.isPunish(),
	}, nil
}

// PunishService will punish an service host for an punishTimeSec interval
func (s *serviceControllerImpl) PunishService(ctx context.Context, service, host string, punishTimeSec int) {
	v, ok := s.allServices.Load(s.getServiceKey(service, host))
	if !ok {
		panic(fmt.Sprintf("can't find host in all services map, %s", s.getServiceKey(service, host)))
	}
	item := v.(*hostItem)

	// atomic set item's punish time unix
	atomic.StoreInt64(&item.punishTimeUnix, time.Now().Add(time.Duration(punishTimeSec)*time.Second).Unix())
}

// PunishDisk will punish a disk host for an punishTimeSec interval
func (s *serviceControllerImpl) PunishDisk(ctx context.Context, diskID proto.DiskID, punishTimeSec int) {
	s.PunishService(ctx, _primaryDisk, diskID.ToString(), punishTimeSec)
}

// PunishShardnode will punish a shardnode disk host for an punishTimeSec interval
func (s *serviceControllerImpl) PunishShardnode(ctx context.Context, diskID proto.DiskID, punishTimeSec int) {
	s.PunishService(ctx, _primaryShardnodeDisk, diskID.ToString(), punishTimeSec)
}

// PunishDiskWithThreshold will punish a disk host for
// an punishTimeSec interval if disk host failed times satisfied with threshold
func (s *serviceControllerImpl) PunishDiskWithThreshold(ctx context.Context, diskID proto.DiskID, punishTimeSec int) {
	s.punishWith(ctx, _primaryDisk, diskID.ToString(), punishTimeSec,
		s.config.DiskPunishThreshold, s.config.DiskPunishValidIntervalS)
}

// PunishServiceWithThreshold will punish an service host for
// an punishTimeSec interval if service failed times satisfied with threshold
func (s *serviceControllerImpl) PunishServiceWithThreshold(ctx context.Context, service, host string, punishTimeSec int) {
	s.punishWith(ctx, service, host, punishTimeSec, s.config.ServicePunishThreshold, s.config.ServicePunishValidIntervalS)
}

func (s *serviceControllerImpl) punishWith(ctx context.Context,
	primary, secondary string, punishTimeSec int,
	threshold uint32, interval int,
) {
	serviceKey := s.getServiceKey(primary, secondary)
	v, ok := s.allServices.Load(serviceKey)
	if !ok {
		panic(fmt.Sprintf("can't load host in all services map, %s", serviceKey))
	}
	item := v.(*hostItem)
	new := atomic.AddUint32(&item.failedTimes, 1)
	// failedTimes larger than threshold, then check the lastModifyTime
	if new >= threshold {
		if time.Since(time.Unix(atomic.LoadInt64(&item.lastModifyTime), 0)) < time.Duration(interval)*time.Second {
			s.PunishService(ctx, primary, secondary, punishTimeSec)
			return
		}
		atomic.AddUint32(&item.failedTimes, -(new - 1))
	}
	atomic.StoreInt64(&item.lastModifyTime, time.Now().Unix())
}

func (s *serviceControllerImpl) getServiceLock(name string) *sync.RWMutex {
	return s.serviceLocks[name]
}

func (s *serviceControllerImpl) getServiceKey(primary string, secondary any) string {
	return fmt.Sprintf("%s/%v", primary, secondary)
}
