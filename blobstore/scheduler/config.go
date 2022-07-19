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

package scheduler

import (
	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/mongoutil"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/recordlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/scheduler/db"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

const (
	defaultTopologyUpdateIntervalMin  = 1
	defaultVolumeCacheUpdateIntervalS = 10
	defaultRetryHostsCnt              = 1
	defaultClientTimeoutMs            = int64(1000)
	defaultHostSyncIntervalMs         = int64(1000)
	defaultMongoTimeoutMs             = int64(3000)

	defaultBalanceDiskCntLimit = 100
	defaultMaxDiskFreeChunkCnt = int64(1024)
	defaultMinDiskFreeChunkCnt = int64(20)

	defaultInspectIntervalS  = 1
	defaultListVolIntervalMs = 10
	defaultListVolStep       = 100
	defaultInspectBatch      = 1000
	defaultInspectTimeoutMs  = 10000

	defaultTaskPoolSize             = 10
	defaultHandleBatchCnt           = 100
	defaultFailMsgConsumeIntervalMs = int64(10000)
	defaultDeleteLogChunkSize       = uint(29)
	defaultDeleteDelayH             = int64(72)
	defaultDeleteNoDelay            = int64(0)

	defaultTickInterval   = uint32(1)
	defaultHeartbeatTicks = uint32(30)
	defaultExpiresTicks   = uint32(60)

	defaultDatabase           = "scheduler"
	defaultOrphanedShardTable = "orphaned_shard_tbl"

	defaultShardRepairNormalTopic   = "shard_repair"
	defaultShardRepairPriorityTopic = "shard_repair_prior"
	defaultShardRepairFailedTopic   = "shard_repair_failed"

	defaultBlobDeleteNormalTopic = "blob_delete"
	defaultBlobDeleteFailedTopic = "blob_delete_failed"
)

var defaultWriteConfig = mongoutil.DefaultWriteConfig

// Config service config
type Config struct {
	cmd.Config

	ClusterID proto.ClusterID `json:"cluster_id"`
	Services  Services        `json:"services"`

	Database                   db.Config `json:"database"`
	TopologyUpdateIntervalMin  int       `json:"topology_update_interval_min"`
	VolumeCacheUpdateIntervalS int       `json:"volume_cache_update_interval_s"`
	FreeChunkCounterBuckets    []float64 `json:"free_chunk_counter_buckets"`

	ClusterMgr clustermgr.Config `json:"clustermgr"`
	Proxy      proxy.LbConfig    `json:"proxy"`
	Blobnode   blobnode.Config   `json:"blobnode"`
	Scheduler  rpc.Config        `json:"scheduler"`

	Balance       BalanceMgrConfig    `json:"balance"`
	DiskDrop      MigrateConfig       `json:"disk_drop"`
	DiskRepair    MigrateConfig       `json:"disk_repair"`
	ManualMigrate MigrateConfig       `json:"manual_migrate"`
	VolumeInspect VolumeInspectMgrCfg `json:"volume_inspect"`
	TaskLog       recordlog.Config    `json:"task_log"`

	Kafka       KafkaConfig       `json:"kafka"`
	ShardRepair ShardRepairConfig `json:"shard_repair"`
	BlobDelete  BlobDeleteConfig  `json:"blob_delete"`

	ServiceRegister ServiceRegisterConfig `json:"service_register"`
}

// ServiceRegisterConfig is service register info
type ServiceRegisterConfig struct {
	TickInterval   uint32 `json:"tick_interval"`
	HeartbeatTicks uint32 `json:"heartbeat_ticks"`
	ExpiresTicks   uint32 `json:"expires_ticks"`
	Idc            string `json:"idc"`
	Host           string `json:"host"`
}

// ShardRepairKafkaConfig is kafka config of shard repair
type ShardRepairKafkaConfig struct {
	BrokerList             []string    `json:"-"`
	FailMsgSenderTimeoutMs int64       `json:"-"`
	Normal                 TopicConfig `json:"normal"`
	Failed                 TopicConfig `json:"failed"`
	Priority               TopicConfig `json:"priority"`
}

// BlobDeleteKafkaConfig is kafka config of blob delete
type BlobDeleteKafkaConfig struct {
	BrokerList             []string    `json:"-"`
	FailMsgSenderTimeoutMs int64       `json:"-"`
	Normal                 TopicConfig `json:"normal"`
	Failed                 TopicConfig `json:"failed"`
}

// KafkaConfig kafka config
type KafkaConfig struct {
	BrokerList             []string               `json:"broker_list"`
	FailMsgSenderTimeoutMs int64                  `json:"fail_msg_sender_timeout_ms"`
	ShardRepair            ShardRepairKafkaConfig `json:"shard_repair"`
	BlobDelete             BlobDeleteKafkaConfig  `json:"blob_delete"`
}

// TopicConfig topic config
type TopicConfig struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

type Services struct {
	Leader  uint64            `json:"leader"`
	NodeID  uint64            `json:"node_id"`
	Members map[uint64]string `json:"members"`
}

func (c *Config) IsLeader() bool {
	return c.Services.Leader == c.Services.NodeID
}

func (c *Config) Leader() string {
	return c.Services.Members[c.Services.Leader]
}

func (c *Config) Follower() []string {
	var followers []string
	for k, v := range c.Services.Members {
		if k != c.Services.Leader {
			followers = append(followers, v)
		}
	}
	return followers
}

func (c *Config) fixServices() error {
	if len(c.Services.Members) < 1 {
		return errInvalidMembers
	}
	if _, ok := c.Services.Members[c.Services.Leader]; !ok {
		return errInvalidLeader
	}
	if _, ok := c.Services.Members[c.Services.NodeID]; !ok {
		return errInvalidNodeID
	}
	return nil
}

func (c *Config) fixConfig() (err error) {
	if c.ClusterID == 0 {
		return errIllegalClusterID
	}
	if err := c.fixServices(); err != nil {
		return err
	}
	defaulter.LessOrEqual(&c.TopologyUpdateIntervalMin, defaultTopologyUpdateIntervalMin)
	defaulter.LessOrEqual(&c.VolumeCacheUpdateIntervalS, defaultVolumeCacheUpdateIntervalS)
	defaulter.LessOrEqual(&c.TaskLog.ChunkBits, defaultDeleteLogChunkSize)
	c.fixClientConfig()
	c.fixDataBaseConfig()
	c.fixKafkaConfig()
	c.fixBalanceConfig()
	c.fixDiskDropConfig()
	c.fixDiskRepairConfig()
	c.fixManualMigrateConfig()
	c.fixInspectConfig()
	c.fixShardRepairConfig()
	c.fixBlobDeleteConfig()
	c.fixRegisterConfig()
	return nil
}

func (c *Config) fixClientConfig() {
	defaulter.LessOrEqual(&c.Proxy.ClientTimeoutMs, defaultClientTimeoutMs)
	defaulter.LessOrEqual(&c.Proxy.HostSyncIntervalMs, defaultHostSyncIntervalMs)
	defaulter.LessOrEqual(&c.Proxy.RetryHostsCnt, defaultRetryHostsCnt)
	defaulter.LessOrEqual(&c.Blobnode.ClientTimeoutMs, defaultClientTimeoutMs)
	defaulter.LessOrEqual(&c.Scheduler.ClientTimeoutMs, defaultClientTimeoutMs)
}

func (c *Config) fixDataBaseConfig() {
	if c.Database.Mongo.WriteConcern == nil {
		c.Database.Mongo.WriteConcern = &defaultWriteConfig
	}
	defaulter.LessOrEqual(&c.Database.Mongo.TimeoutMs, defaultMongoTimeoutMs)
	defaulter.Empty(&c.Database.DBName, defaultDatabase)
	defaulter.Empty(&c.Database.OrphanShardTable, defaultOrphanedShardTable)
}

func (c *Config) fixKafkaConfig() {
	defaulter.Empty(&c.Kafka.BlobDelete.Normal.Topic, defaultBlobDeleteNormalTopic)
	defaulter.Empty(&c.Kafka.BlobDelete.Failed.Topic, defaultBlobDeleteFailedTopic)
	defaulter.Empty(&c.Kafka.ShardRepair.Normal.Topic, defaultShardRepairNormalTopic)
	defaulter.Empty(&c.Kafka.ShardRepair.Priority.Topic, defaultShardRepairPriorityTopic)
	defaulter.Empty(&c.Kafka.ShardRepair.Failed.Topic, defaultShardRepairFailedTopic)
	defaulter.LessOrEqual(&c.Kafka.FailMsgSenderTimeoutMs, defaultClientTimeoutMs)
	c.BlobDelete.Kafka.FailMsgSenderTimeoutMs = c.Kafka.FailMsgSenderTimeoutMs
	c.ShardRepair.Kafka.FailMsgSenderTimeoutMs = c.Kafka.FailMsgSenderTimeoutMs
	c.Kafka.ShardRepair.BrokerList = c.Kafka.BrokerList
	c.Kafka.BlobDelete.BrokerList = c.Kafka.BrokerList
}

func (c *Config) fixBalanceConfig() {
	c.Balance.ClusterID = c.ClusterID
	defaulter.LessOrEqual(&c.Balance.BalanceDiskCntLimit, defaultBalanceDiskCntLimit)
	defaulter.LessOrEqual(&c.Balance.MaxDiskFreeChunkCnt, defaultMaxDiskFreeChunkCnt)
	defaulter.LessOrEqual(&c.Balance.MinDiskFreeChunkCnt, defaultMinDiskFreeChunkCnt)
	c.Balance.CheckAndFix()
}

func (c *Config) fixDiskDropConfig() {
	c.DiskDrop.ClusterID = c.ClusterID
	c.DiskDrop.CheckAndFix()
}

func (c *Config) fixDiskRepairConfig() {
	c.DiskRepair.ClusterID = c.ClusterID
	c.DiskRepair.CheckAndFix()
}

func (c *Config) fixManualMigrateConfig() {
	c.ManualMigrate.ClusterID = c.ClusterID
	c.ManualMigrate.CheckAndFix()
}

func (c *Config) fixInspectConfig() {
	defaulter.LessOrEqual(&c.VolumeInspect.TimeoutMs, defaultInspectTimeoutMs)
	defaulter.LessOrEqual(&c.VolumeInspect.ListVolStep, defaultListVolStep)
	defaulter.LessOrEqual(&c.VolumeInspect.ListVolIntervalMs, defaultListVolIntervalMs)
	defaulter.LessOrEqual(&c.VolumeInspect.InspectBatch, defaultInspectBatch)
	if c.VolumeInspect.InspectBatch < c.VolumeInspect.ListVolStep {
		c.VolumeInspect.InspectBatch = c.VolumeInspect.ListVolStep
	}
	defaulter.LessOrEqual(&c.VolumeInspect.InspectIntervalS, defaultInspectIntervalS)
}

func (c *Config) fixShardRepairConfig() {
	c.ShardRepair.ClusterID = c.ClusterID
	defaulter.LessOrEqual(&c.ShardRepair.TaskPoolSize, defaultTaskPoolSize)
	defaulter.LessOrEqual(&c.ShardRepair.NormalHandleBatchCnt, defaultHandleBatchCnt)
	defaulter.LessOrEqual(&c.ShardRepair.FailHandleBatchCnt, defaultHandleBatchCnt)
	defaulter.LessOrEqual(&c.ShardRepair.FailMsgConsumeIntervalMs, defaultFailMsgConsumeIntervalMs)
	c.ShardRepair.Kafka = c.Kafka.ShardRepair
}

func (c *Config) fixBlobDeleteConfig() {
	c.BlobDelete.ClusterID = c.ClusterID
	defaulter.LessOrEqual(&c.BlobDelete.TaskPoolSize, defaultTaskPoolSize)
	defaulter.LessOrEqual(&c.BlobDelete.NormalHandleBatchCnt, defaultHandleBatchCnt)
	defaulter.LessOrEqual(&c.BlobDelete.FailHandleBatchCnt, defaultHandleBatchCnt)
	defaulter.LessOrEqual(&c.BlobDelete.FailMsgConsumeIntervalMs, defaultFailMsgConsumeIntervalMs)
	defaulter.LessOrEqual(&c.BlobDelete.DeleteLog.ChunkBits, defaultDeleteLogChunkSize)
	defaulter.Equal(&c.BlobDelete.SafeDelayTimeH, defaultDeleteDelayH)
	defaulter.Less(&c.BlobDelete.SafeDelayTimeH, defaultDeleteNoDelay)
	c.BlobDelete.Kafka = c.Kafka.BlobDelete
}

func (c *Config) fixRegisterConfig() {
	defaulter.LessOrEqual(&c.ServiceRegister.TickInterval, defaultTickInterval)
	defaulter.LessOrEqual(&c.ServiceRegister.HeartbeatTicks, defaultHeartbeatTicks)
	defaulter.LessOrEqual(&c.ServiceRegister.ExpiresTicks, defaultExpiresTicks)
}
