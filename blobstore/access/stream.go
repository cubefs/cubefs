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

package access

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/hashicorp/consul/api"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/ec"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const (
	defaultMaxObjectSize int64 = 5 * (1 << 30) // 5GB

	// hystrix command define
	allocCommand = "alloc"
	rwCommand    = "rw"

	serviceProxy = proto.ServiceNameProxy
)

// StreamHandler stream http handler
type StreamHandler interface {
	// Alloc access interface /alloc
	//     required: size, file size
	//     optional: blobSize > 0, alloc with blobSize
	//               assignClusterID > 0, assign to alloc in this cluster certainly
	//               codeMode > 0, alloc in this codemode
	//     return: a location of file
	Alloc(ctx context.Context, size uint64, blobSize uint32,
		assignClusterID proto.ClusterID, codeMode codemode.CodeMode) (*access.Location, error)

	// PutAt access interface /putat, put one blob
	//     required: rc file reader
	//     required: clusterID VolumeID BlobID
	//     required: size, one blob size
	//     optional: hasherMap, computing hash
	PutAt(ctx context.Context, rc io.Reader,
		clusterID proto.ClusterID, vid proto.Vid, bid proto.BlobID, size int64, hasherMap access.HasherMap) error

	// Put put one object
	//     required: size, file size
	//     optional: hasher map to calculate hash.Hash
	Put(ctx context.Context, rc io.Reader, size int64, hasherMap access.HasherMap) (*access.Location, error)

	// Get read file
	//     required: location, readSize
	//     optional: offset(default is 0)
	//
	//     first return value is data transfer to copy data after argument checking
	//
	//  Read data shards firstly, if blob size is small or read few bytes
	//  then ec reconstruct-read, try to reconstruct from N+X to N+M
	//
	//  sorted N+X is, such as we use mode EC6P10L2, X=2 and Read from idc=2
	//  shards like this
	//              data N 6        |    parity M 10     | local L 2
	//        d1  d2  d3  d4  d5  d6  p1 .. p5  p6 .. p10  l1  l2
	//   idc   1   1   1   2   2   2     1         2        1   2
	//
	//sorted  d4  d5  d6  p6 .. p10  d1  d2  d3  p1 .. p5
	//read-1 [d4                p10]
	//read-2 [d4                p10  d1]
	//read-3 [d4                p10  d1  d2]
	//...
	//read-9 [d4                                       p5]
	//failed
	Get(ctx context.Context, w io.Writer, location access.Location, readSize, offset uint64) (func() error, error)

	// Delete delete all blobs in this location
	Delete(ctx context.Context, location *access.Location) error

	// Admin returns internal admin interface.
	Admin() interface{}
}

type streamAdmin struct {
	config     StreamConfig
	memPool    *resourcepool.MemPool
	controller controller.ClusterController
}

// StreamConfig access stream handler config
type StreamConfig struct {
	IDC string `json:"idc"`

	MaxBlobSize                uint32 `json:"max_blob_size"`
	DiskPunishIntervalS        int    `json:"disk_punish_interval_s"`
	DiskTimeoutPunishIntervalS int    `json:"disk_timeout_punish_interval_s"`
	ServicePunishIntervalS     int    `json:"service_punish_interval_s"`
	AllocRetryTimes            int    `json:"alloc_retry_times"`
	AllocRetryIntervalMS       int    `json:"alloc_retry_interval_ms"`
	EncoderEnableVerify        bool   `json:"encoder_enableverify"`
	EncoderConcurrency         int    `json:"encoder_concurrency"`
	MinReadShardsX             int    `json:"min_read_shards_x"`
	ShardCrcDisabled           bool   `json:"shard_crc_disabled"`

	MemPoolSizeClasses map[int]int `json:"mem_pool_size_classes"`

	// CodeModesPutQuorums
	// just for one AZ is down, cant write quorum in all AZs
	CodeModesPutQuorums map[codemode.CodeMode]int `json:"code_mode_put_quorums"`

	ClusterConfig  controller.ClusterConfig `json:"cluster_config"`
	BlobnodeConfig blobnode.Config          `json:"blobnode_config"`
	ProxyConfig    proxy.Config             `json:"proxy_config"`

	// hystrix command config
	AllocCommandConfig hystrix.CommandConfig `json:"alloc_command_config"`
	RWCommandConfig    hystrix.CommandConfig `json:"rw_command_config"`
}

// discard unhealthy volume
type discardVid struct {
	cid      proto.ClusterID
	codeMode codemode.CodeMode
	vid      proto.Vid
}

type blobIdent struct {
	cid proto.ClusterID
	vid proto.Vid
	bid proto.BlobID
}

func (id *blobIdent) String() string {
	return fmt.Sprintf("blob(%d %d %d)", id.cid, id.vid, id.bid)
}

// Handler stream handler
type Handler struct {
	memPool           *resourcepool.MemPool
	encoder           map[codemode.CodeMode]ec.Encoder
	clusterController controller.ClusterController

	blobnodeClient blobnode.StorageAPI
	proxyClient    proxy.Client

	allCodeModes  CodeModePairs
	maxObjectSize int64

	discardVidChan chan discardVid
	stopCh         <-chan struct{}

	StreamConfig
}

func confCheck(cfg *StreamConfig) {
	if cfg.IDC == "" {
		log.Fatal("idc config can not be null")
	}
	cfg.ClusterConfig.IDC = cfg.IDC

	if len(cfg.MemPoolSizeClasses) == 0 {
		cfg.MemPoolSizeClasses = getDefaultMempoolSize()
	}

	for mode, quorum := range cfg.CodeModesPutQuorums {
		tactic := mode.Tactic()
		if quorum < tactic.N+tactic.L+1 || quorum > mode.GetShardNum() {
			log.Fatalf("invalid put quorum(%d) in codemode(%d): %+v", quorum, mode, tactic)
		}
	}

	defaulter.Equal(&cfg.MaxBlobSize, defaultMaxBlobSize)
	defaulter.LessOrEqual(&cfg.DiskPunishIntervalS, defaultDiskPunishIntervalS)
	defaulter.LessOrEqual(&cfg.DiskTimeoutPunishIntervalS, defaultDiskPunishIntervalS/10)
	defaulter.LessOrEqual(&cfg.ServicePunishIntervalS, defaultServicePunishIntervalS)
	defaulter.LessOrEqual(&cfg.AllocRetryTimes, defaultAllocRetryTimes)
	if cfg.AllocRetryIntervalMS <= 100 {
		cfg.AllocRetryIntervalMS = defaultAllocRetryIntervalMS
	}
	defaulter.LessOrEqual(&cfg.EncoderConcurrency, defaultEncoderConcurrency)
	defaulter.LessOrEqual(&cfg.MinReadShardsX, defaultMinReadShardsX)

	defaulter.LessOrEqual(&cfg.ClusterConfig.CMClientConfig.Config.ClientTimeoutMs, defaultTimeoutClusterMgr)
	defaulter.LessOrEqual(&cfg.BlobnodeConfig.ClientTimeoutMs, defaultTimeoutBlobnode)
	defaulter.LessOrEqual(&cfg.ProxyConfig.ClientTimeoutMs, defaultTimeoutProxy)

	hc := cfg.AllocCommandConfig
	defaulter.LessOrEqual(&hc.Timeout, defaultAllocatorTimeout)
	defaulter.LessOrEqual(&hc.MaxConcurrentRequests, defaultAllocatorMaxConcurrentRequests)
	defaulter.LessOrEqual(&hc.RequestVolumeThreshold, defaultAllocatorRequestVolumeThreshold)
	defaulter.LessOrEqual(&hc.SleepWindow, defaultAllocatorSleepWindow)
	defaulter.LessOrEqual(&hc.ErrorPercentThreshold, defaultAllocatorErrorPercentThreshold)
	cfg.AllocCommandConfig = hc

	hc = cfg.RWCommandConfig
	defaulter.LessOrEqual(&hc.Timeout, defaultBlobnodeTimeout)
	defaulter.LessOrEqual(&hc.MaxConcurrentRequests, defaultBlobnodeMaxConcurrentRequests)
	defaulter.LessOrEqual(&hc.RequestVolumeThreshold, defaultBlobnodeRequestVolumeThreshold)
	defaulter.LessOrEqual(&hc.SleepWindow, defaultBlobnodeSleepWindow)
	defaulter.LessOrEqual(&hc.ErrorPercentThreshold, defaultBlobnodeErrorPercentThreshold)
	cfg.RWCommandConfig = hc
}

// NewStreamHandler returns a stream handler
func NewStreamHandler(cfg *StreamConfig, kvClient *api.Client, stopCh <-chan struct{}) StreamHandler {
	confCheck(cfg)

	clusterController, err := controller.NewClusterController(&cfg.ClusterConfig, kvClient)
	if err != nil {
		log.Fatalf("new cluster controller failed, err: %v", err)
	}

	handler := &Handler{
		memPool:           resourcepool.NewMemPool(cfg.MemPoolSizeClasses),
		clusterController: clusterController,

		blobnodeClient: blobnode.New(&cfg.BlobnodeConfig),
		proxyClient:    proxy.New(&cfg.ProxyConfig),

		maxObjectSize: defaultMaxObjectSize,
		StreamConfig:  *cfg,
	}

	rawCodeModePolicies, err := handler.clusterController.GetConfig(context.Background(), proto.CodeModeConfigKey)
	if err != nil {
		log.Fatal("get codemode policy from cluster manager failed, err: ", err)
	}
	codeModePolicies := make([]codemode.Policy, 0)
	err = json.Unmarshal([]byte(rawCodeModePolicies), &codeModePolicies)
	if err != nil {
		log.Fatal("json decode codemode policy failed, err: ", err)
	}
	if len(codeModePolicies) <= 0 {
		log.Fatal("invalid codemode policy raw: ", rawCodeModePolicies)
	}

	allCodeModes := make(CodeModePairs)
	encoders := make(map[codemode.CodeMode]ec.Encoder)
	maxSize := int64(0)
	for _, policy := range codeModePolicies {
		if policy.MaxSize > maxSize {
			maxSize = policy.MaxSize
		}
		codeMode := policy.ModeName.GetCodeMode()
		tactic := codeMode.Tactic()
		allCodeModes[codeMode] = CodeModePair{
			Policy: policy,
			Tactic: tactic,
		}
		encoder, err := ec.NewEncoder(ec.Config{
			CodeMode:     tactic,
			EnableVerify: cfg.EncoderEnableVerify,
			Concurrency:  cfg.EncoderConcurrency,
		})
		if err != nil {
			log.Fatalf("new encoder failed, err: %v", err)
		}
		encoders[codeMode] = encoder
	}
	handler.allCodeModes = allCodeModes
	handler.encoder = encoders
	if maxSize < handler.maxObjectSize {
		handler.maxObjectSize = maxSize
	}

	hystrix.ConfigureCommand(allocCommand, cfg.AllocCommandConfig)
	hystrix.ConfigureCommand(rwCommand, cfg.RWCommandConfig)

	handler.discardVidChan = make(chan discardVid, 8)
	handler.stopCh = stopCh
	handler.loopDiscardVids()
	return handler
}

// Delete delete all blobs in this location
func (h *Handler) Delete(ctx context.Context, location *access.Location) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("to delete %+v", location)
	return h.clearGarbage(ctx, location)
}

// Admin returns internal admin interface.
func (h *Handler) Admin() interface{} {
	return &streamAdmin{
		config:     h.StreamConfig,
		memPool:    h.memPool,
		controller: h.clusterController,
	}
}

func (h *Handler) sendRepairMsgBg(ctx context.Context, blob blobIdent, badIdxes []uint8) {
	go func() {
		h.sendRepairMsg(ctx, blob, badIdxes)
	}()
}

func (h *Handler) sendRepairMsg(ctx context.Context, blob blobIdent, badIdxes []uint8) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("to repair %s indexes(%+v)", blob.String(), badIdxes)

	clusterID := blob.cid
	serviceController, err := h.clusterController.GetServiceController(clusterID)
	if err != nil {
		span.Error(errors.Detail(err))
		return
	}
	reportUnhealth(clusterID, "repair.msg", "-", "-", "-")

	repairArgs := &proxy.ShardRepairArgs{
		ClusterID: clusterID,
		Bid:       blob.bid,
		Vid:       blob.vid,
		BadIdxes:  badIdxes[:],
		Reason:    "access-repair",
	}

	if err := retry.Timed(3, 200).On(func() error {
		host, err := serviceController.GetServiceHost(ctx, serviceProxy)
		if err != nil {
			span.Warn(err)
			return err
		}
		err = h.proxyClient.SendShardRepairMsg(ctx, host, repairArgs)
		if err != nil {
			if errorTimeout(err) || errorConnectionRefused(err) {
				serviceController.PunishServiceWithThreshold(ctx, serviceProxy, host, h.ServicePunishIntervalS)
			}
			span.Warnf("send to %s repair message(%+v) %s", host, repairArgs, err.Error())
			reportUnhealth(clusterID, "punish", serviceProxy, host, "failed")
			err = errors.Base(err, host)
		}
		return err
	}); err != nil {
		reportUnhealth(clusterID, "repair.msg", serviceProxy, "-", "failed")
		span.Errorf("send repair message(%+v) failed %s", repairArgs, errors.Detail(err))
		return
	}

	span.Infof("send repair message(%+v)", repairArgs)
}

func (h *Handler) clearGarbage(ctx context.Context, location *access.Location) error {
	span := trace.SpanFromContextSafe(ctx)
	serviceController, err := h.clusterController.GetServiceController(location.ClusterID)
	if err != nil {
		span.Error(errors.Detail(err))
		return errors.Base(err, "clear location:", *location)
	}

	blobs := location.Spread()
	deleteArgs := &proxy.DeleteArgs{
		ClusterID: location.ClusterID,
		Blobs:     make([]proxy.BlobDelete, 0, len(blobs)),
	}

	for _, blob := range blobs {
		deleteArgs.Blobs = append(deleteArgs.Blobs, proxy.BlobDelete{
			Bid: blob.Bid,
			Vid: blob.Vid,
		})
	}

	var logMsg interface{} = location
	if len(deleteArgs.Blobs) <= 20 {
		logMsg = deleteArgs
	}
	if err := retry.Timed(3, 200).On(func() error {
		host, err := serviceController.GetServiceHost(ctx, serviceProxy)
		if err != nil {
			span.Warn(err)
			return err
		}
		err = h.proxyClient.SendDeleteMsg(ctx, host, deleteArgs)
		if err != nil {
			if errorTimeout(err) || errorConnectionRefused(err) {
				serviceController.PunishServiceWithThreshold(ctx, serviceProxy, host, h.ServicePunishIntervalS)
			}
			span.Warnf("send to %s delete message(%+v) %s", host, logMsg, err.Error())
			reportUnhealth(location.ClusterID, "punish", serviceProxy, host, "failed")
			err = errors.Base(err, host)
		}
		return err
	}); err != nil {
		reportUnhealth(location.ClusterID, "delete.msg", serviceProxy, "-", "failed")
		span.Errorf("send delete message(%+v) failed %s", logMsg, errors.Detail(err))
		return errors.Base(err, "send delete message:", logMsg)
	}

	span.Infof("send delete message(%+v)", logMsg)
	return nil
}

// getVolume get volume info
func (h *Handler) getVolume(ctx context.Context, clusterID proto.ClusterID, vid proto.Vid, isCache bool) (*controller.VolumePhy, error) {
	volumeGetter, err := h.clusterController.GetVolumeGetter(clusterID)
	if err != nil {
		return nil, err
	}

	volume := volumeGetter.Get(ctx, vid, isCache)
	if volume == nil {
		return nil, errors.Newf("not found volume of (%d %d)", clusterID, vid)
	}

	return volume, nil
}

func (h *Handler) punishVolume(ctx context.Context, clusterID proto.ClusterID, vid proto.Vid, host, reason string) {
	reportUnhealth(clusterID, "punish", "volume", host, reason)
	if volumeGetter, err := h.clusterController.GetVolumeGetter(clusterID); err == nil {
		volumeGetter.Punish(ctx, vid, h.DiskPunishIntervalS)
	}
}

func (h *Handler) punishDisk(ctx context.Context, clusterID proto.ClusterID, diskID proto.DiskID, host, reason string) {
	reportUnhealth(clusterID, "punish", "disk", host, reason)
	if serviceController, err := h.clusterController.GetServiceController(clusterID); err == nil {
		serviceController.PunishDisk(ctx, diskID, h.DiskPunishIntervalS)
	}
}

func (h *Handler) punishDiskWith(ctx context.Context, clusterID proto.ClusterID, diskID proto.DiskID, host, reason string) {
	reportUnhealth(clusterID, "punish", "diskwith", host, reason)
	if serviceController, err := h.clusterController.GetServiceController(clusterID); err == nil {
		serviceController.PunishDiskWithThreshold(ctx, diskID, h.DiskTimeoutPunishIntervalS)
	}
}

// blobCount blobSize > 0 is certain
func blobCount(size uint64, blobSize uint32) uint64 {
	return (size + uint64(blobSize) - 1) / uint64(blobSize)
}

func minU64(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

func errorTimeout(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "Timeout") || strings.Contains(msg, "timeout")
}

func errorConnectionRefused(err error) bool {
	return strings.Contains(err.Error(), "connection refused")
}
