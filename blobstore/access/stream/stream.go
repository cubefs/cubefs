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

package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/afex/hystrix-go/hystrix"
	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/ec"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
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
		assignClusterID proto.ClusterID, codeMode codemode.CodeMode) (*proto.Location, error)

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
	Put(ctx context.Context, rc io.Reader, size int64, hasherMap access.HasherMap) (*proto.Location, error)

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
	Get(ctx context.Context, w io.Writer, location proto.Location, readSize, offset uint64) (func() error, error)

	// Delete delete all blobs in this location
	Delete(ctx context.Context, location *proto.Location) error

	// Admin returns internal admin interface.
	Admin() interface{}
	// GetBlob returns location
	GetBlob(ctx context.Context, args *access.GetBlobArgs) (*proto.Location, error)
	// DeleteBlob returns error
	DeleteBlob(ctx context.Context, args *access.DelBlobArgs) error
	// SealBlob returns error
	SealBlob(ctx context.Context, args *access.SealBlobArgs) error
	// CreateBlob returns location
	CreateBlob(ctx context.Context, args *access.CreateBlobArgs) (*proto.Location, error)
	// ListBlob returns blobs
	ListBlob(ctx context.Context, args *access.ListBlobArgs) (shardnode.ListBlobRet, error)
	// AllocSlice returns alloc blob
	AllocSlice(ctx context.Context, args *access.AllocSliceArgs) (shardnode.AllocSliceRet, error)
}

type StreamAdmin struct {
	Config     StreamConfig
	MemPool    *resourcepool.MemPool
	Controller controller.ClusterController
}

type ShardnodeConfig struct {
	shardnode.Config
}

// StreamConfig access stream handler config
type StreamConfig struct {
	IDC string `json:"idc"`

	MaxBlobSize                uint32 `json:"max_blob_size"`
	VolumePunishIntervalS      int    `json:"volume_punish_interval_s"`
	DiskPunishIntervalS        int    `json:"disk_punish_interval_s"`
	DiskTimeoutPunishIntervalS int    `json:"disk_timeout_punish_interval_s"`
	ServicePunishIntervalS     int    `json:"service_punish_interval_s"` // just service of proxy
	ShardnodePunishIntervalS   int    `json:"shardnode_punish_interval_s"`
	AllocRetryTimes            int    `json:"alloc_retry_times"`
	AllocRetryIntervalMS       int    `json:"alloc_retry_interval_ms"`
	EncoderEnableVerify        bool   `json:"encoder_enableverify"`
	EncoderConcurrency         int    `json:"encoder_concurrency"`
	MinReadShardsX             int    `json:"min_read_shards_x"`
	ReadDataOnlyTimeoutMS      int    `json:"read_data_only_timeout_ms"`
	ShardCrcWriteDisable       bool   `json:"shard_crc_write_disable"`
	ShardCrcReadEnable         bool   `json:"shard_crc_read_enable"`
	ShardnodeRetryTimes        int    `json:"shardnode_retry_times"`
	ShardnodeRetryIntervalMS   int    `json:"shardnode_retry_interval_ms"`

	LogSlowBaseTimeMS  int     `json:"log_slow_base_time_ms"`
	LogSlowBaseSpeedKB int     `json:"log_slow_base_speed_kb"`
	LogSlowTimeFator   float32 `json:"log_slow_time_fator"`

	MemPoolSizeClasses map[int]int `json:"mem_pool_size_classes"`

	// CodeModesPutQuorums
	// just for one AZ is down, cant write quorum in all AZs
	CodeModesPutQuorums map[codemode.CodeMode]int `json:"code_mode_put_quorums"`
	// CodeModesGetOrdered shards with volume unit order to
	// reduce reedsolom's inverted matrix cache
	//
	// EC24P8 in 1AZ, C(32, 8) = 10518300 matrix
	// Inverted matrix memory: (24 + 24*24 + 24*24*8) * 10518300 ~= 51 GB
	CodeModesGetOrdered map[codemode.CodeMode]bool `json:"code_mode_get_ordered"`

	ClusterConfig   controller.ClusterConfig `json:"cluster_config"`
	BlobnodeConfig  blobnode.Config          `json:"blobnode_config"`
	ProxyConfig     proxy.Config             `json:"proxy_config"`
	ShardnodeConfig *ShardnodeConfig         `json:"shardnode_config"`

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
	return fmt.Sprintf("blob(cid:%d vid:%d bid:%d)", id.cid, id.vid, id.bid)
}

// Handler stream handler
type Handler struct {
	memPool           *resourcepool.MemPool
	encoder           map[codemode.CodeMode]ec.Encoder
	clusterController controller.ClusterController
	groupRun          singleflight.Group

	blobnodeClient  blobnode.StorageAPI
	proxyClient     proxy.Client
	shardnodeClient shardnode.AccessAPI

	allCodeModes  CodeModePairs
	maxObjectSize int64

	discardVidChan chan discardVid
	stopCh         <-chan struct{}

	StreamConfig
}

func confCheck(cfg *StreamConfig) error {
	if cfg.IDC == "" {
		return errors.New("idc config can not be null")
	}
	if cfg.ClusterConfig.ConsulAgentAddr == "" && len(cfg.ClusterConfig.Clusters) == 0 {
		return errors.New("consul or clusters can not all be empty")
	}
	cfg.ClusterConfig.IDC = cfg.IDC

	if len(cfg.MemPoolSizeClasses) == 0 {
		cfg.MemPoolSizeClasses = getDefaultMempoolSize()
	}

	for mode, quorum := range cfg.CodeModesPutQuorums {
		tactic := mode.Tactic()
		if quorum < tactic.N+tactic.L+1 || quorum > mode.GetShardNum() {
			return errors.Newf("invalid put quorum(%d) in codemode(%d): %+v", quorum, mode, tactic)
		}
	}

	defaulter.Equal(&cfg.MaxBlobSize, defaultMaxBlobSize)
	defaulter.IntegerLessOrEqual(&cfg.VolumePunishIntervalS, 60)
	defaulter.LessOrEqual(&cfg.DiskPunishIntervalS, defaultDiskPunishIntervalS)
	defaulter.LessOrEqual(&cfg.DiskTimeoutPunishIntervalS, defaultDiskPunishIntervalS/10)
	defaulter.LessOrEqual(&cfg.ServicePunishIntervalS, defaultServicePunishIntervalS)
	defaulter.IntegerLessOrEqual(&cfg.ShardnodePunishIntervalS, 60)
	defaulter.LessOrEqual(&cfg.AllocRetryTimes, defaultAllocRetryTimes)
	defaulter.LessOrEqual(&cfg.AllocRetryIntervalMS, defaultAllocRetryIntervalMS)
	defaulter.LessOrEqual(&cfg.ShardnodeRetryTimes, defaultShardnodeRetryTimes)
	if cfg.ShardnodeRetryIntervalMS <= defaultShardnodeRetryIntervalMS {
		cfg.ShardnodeRetryIntervalMS = defaultShardnodeRetryIntervalMS
	}
	defaulter.LessOrEqual(&cfg.EncoderConcurrency, defaultEncoderConcurrency)
	defaulter.LessOrEqual(&cfg.MinReadShardsX, defaultMinReadShardsX)
	defaulter.LessOrEqual(&cfg.ReadDataOnlyTimeoutMS, 3*1000)

	defaulter.LessOrEqual(&cfg.LogSlowBaseTimeMS, 500)
	defaulter.Equal(&cfg.LogSlowBaseSpeedKB, 1<<10)
	defaulter.LessOrEqual(&cfg.LogSlowTimeFator, float32(2.0))

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

	return nil
}

// NewStreamHandler returns a stream handler
func NewStreamHandler(cfg *StreamConfig, stopCh <-chan struct{}) (h StreamHandler, e error) {
	if e = confCheck(cfg); e != nil {
		return nil, e
	}
	proxyClient := proxy.New(&cfg.ProxyConfig)
	clusterController, err := controller.NewClusterController(&cfg.ClusterConfig, proxyClient, stopCh)
	if err != nil {
		e = errors.Newf("new cluster controller failed, err: %v", err)
		return
	}

	handler := &Handler{
		memPool:           resourcepool.NewMemPool(cfg.MemPoolSizeClasses),
		clusterController: clusterController,

		blobnodeClient:  blobnode.New(&cfg.BlobnodeConfig),
		proxyClient:     proxyClient,
		shardnodeClient: shardnode.NewNonsupportShardnode(),

		maxObjectSize: defaultMaxObjectSize,
		StreamConfig:  *cfg,
	}
	if cfg.ShardnodeConfig != nil { // enable shard node
		// Do not use rpc retry, because the stream blob handles retries itself
		defaulter.LessOrEqual(&cfg.ShardnodeConfig.Config.Retry, int(1))
		handler.shardnodeClient = shardnode.New(cfg.ShardnodeConfig.Config)
	}

	rawCodeModePolicies, err := handler.clusterController.GetConfig(context.Background(), proto.CodeModeConfigKey)
	if err != nil {
		e = errors.Newf("get codemode policy from cluster manager failed, err: %+v", err)
		return
	}
	codeModePolicies := make([]codemode.Policy, 0)
	err = json.Unmarshal([]byte(rawCodeModePolicies), &codeModePolicies)
	if err != nil {
		e = errors.Newf("json decode codemode policy failed, err: %+v", err)
		return
	}
	if len(codeModePolicies) <= 0 {
		e = errors.Newf("invalid codemode policy raw: %s", rawCodeModePolicies)
		return
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
			e = errors.Newf("new encoder failed, err: %v", err)
			return
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
	return handler, nil
}

// Delete delete all blobs in this location
func (h *Handler) Delete(ctx context.Context, location *proto.Location) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("to delete %+v", location)
	return h.clearGarbage(ctx, location)
}

// Admin returns internal admin interface.
func (h *Handler) Admin() interface{} {
	return &StreamAdmin{
		Config:     h.StreamConfig,
		MemPool:    h.memPool,
		Controller: h.clusterController,
	}
}

func (h *Handler) sendRepairMsgBg(ctx context.Context, blob blobIdent, badIdxes []uint8) {
	ctx = trace.NewContextFromContext(ctx)
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
			reportUnhealth(clusterID, "repair.msg", serviceProxy, "-", "failed")
			span.Warn(err)
			return err
		}
		err = h.proxyClient.SendShardRepairMsg(ctx, host, repairArgs)
		if err != nil {
			if errorTimeout(err) || errorConnectionRefused(err) {
				serviceController.PunishServiceWithThreshold(ctx, serviceProxy, host, h.ServicePunishIntervalS)
				reportUnhealth(clusterID, "punish", serviceProxy, host, "failed")
			} else {
				reportUnhealth(clusterID, "repair.msg", serviceProxy, host, "failed")
			}
			span.Warnf("send to %s repair message(%+v) %s", host, repairArgs, err.Error())
			err = errors.Base(err, host)
		}
		return err
	}); err != nil {
		span.Errorf("send repair message(%+v) failed %s", repairArgs, errors.Detail(err))
		return
	}

	span.Infof("send repair message(%+v)", repairArgs)
}

func (h *Handler) clearGarbage(ctx context.Context, location *proto.Location) error {
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
			reportUnhealth(location.ClusterID, "delete.msg", serviceProxy, "-", "failed")
			span.Warn(err)
			return err
		}
		err = h.proxyClient.SendDeleteMsg(ctx, host, deleteArgs)
		if err != nil {
			if errorTimeout(err) || errorConnectionRefused(err) {
				serviceController.PunishServiceWithThreshold(ctx, serviceProxy, host, h.ServicePunishIntervalS)
				reportUnhealth(location.ClusterID, "punish", serviceProxy, host, "failed")
			} else {
				reportUnhealth(location.ClusterID, "delete.msg", serviceProxy, host, "failed")
			}
			span.Warnf("send to %s delete message(%+v) %s", host, logMsg, err.Error())
			err = errors.Base(err, host)
		}
		return err
	}); err != nil {
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

func (h *Handler) updateVolume(ctx context.Context, clusterID proto.ClusterID, vid proto.Vid) {
	volumeGetter, err := h.clusterController.GetVolumeGetter(clusterID)
	if err != nil {
		return
	}
	volumeGetter.Update(ctx, vid)
}

func (h *Handler) punishVolume(ctx context.Context, clusterID proto.ClusterID, vid proto.Vid, host, reason string) {
	reportUnhealth(clusterID, "punish", "volume", host, reason)
	if volumeGetter, err := h.clusterController.GetVolumeGetter(clusterID); err == nil {
		volumeGetter.Punish(ctx, vid, h.VolumePunishIntervalS)
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

func (h *Handler) punishShardnodeDisk(ctx context.Context, clusterID proto.ClusterID, diskID proto.DiskID, host, reason string) {
	reportUnhealth(clusterID, "punish", "shardnode", host, reason)
	if serviceController, err := h.clusterController.GetServiceController(clusterID); err == nil {
		serviceController.PunishShardnode(ctx, diskID, h.ShardnodePunishIntervalS)
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
