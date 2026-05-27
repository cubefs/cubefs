// Copyright 2026 The CubeFS Authors.
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

// S3.2 — Soak 模式 checkpoint 存储。
//
// 设计要点：
//   - SoakStore 是接口，方便单测注入 mock；boltStore 是 bbolt-backed 默认实现。
//   - 键路径 "<taskID>/<stage>/<shardID>"：单 bucket 内可以容纳多 task × 多 stage
//     × 多 shard 的 checkpoint，不需要分桶/分库管理。
//   - 值为 SoakCheckpoint 的 JSON 序列化结果；HDR 直方图快照已经在外层做过
//     gzip+base64 编码，存储层不再二次处理，直接当字节数组持久化。
//   - 不存在 → Load 返回 (nil, nil)，与 ResumeFromCheckpoint 的"首次启动也能透明
//     调用"语义一致。
package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	bbolt "go.etcd.io/bbolt"
)

// SoakCheckpoint 是单个 (task, stage, shard) 三元组的 Soak 进度快照。
//
// 字段语义：
//   - TaskID/Stage/ShardID：三元组主键。
//   - ElapsedSec：相对该 stage 起跑点，已累计运行的秒数。重启恢复时，runner
//     按此值跳过已 ramp 完的段、计算剩余 runtime。
//   - OpsCompleted：已完成的 op 数（执行器自己累积），用于诊断和未来的进度展示。
//   - LastUpdateUnix：上一次写盘的 wall-clock 时间（秒，Unix epoch），主要供
//     运维诊断"上次心跳"用。
//   - RestartCount：当前 stage 已重启次数；用于 MaxRestartCount 上限判断。
//   - Snapshot：HDR 直方图快照（已经过 gzip+base64 编码）。Soak runner 自己不解
//     析其内容；只负责存取。允许 nil（首次 checkpoint 还没攒到样本时）。
type SoakCheckpoint struct {
	TaskID         string `json:"taskId"`
	Stage          string `json:"stage"`
	ShardID        int    `json:"shardId"`
	ElapsedSec     int    `json:"elapsedSec"`
	OpsCompleted   int64  `json:"opsCompleted"`
	LastUpdateUnix int64  `json:"lastUpdateUnix"`
	RestartCount   int    `json:"restartCount"`
	Snapshot       []byte `json:"snapshot,omitempty"`
}

// SoakStore 抽象 Soak checkpoint 的持久化层。所有方法都接受 ctx，但实现可以
// 视场景忽略（bbolt 自身没有可中断的事务 API）。
//
// Load 在 key 不存在时返回 (nil, nil) —— 这是 ResumeFromCheckpoint 的"首跑也透
// 明调用"约定。任何其它错误（解码失败、bucket 缺失、ctx 取消）应原样上抛。
type SoakStore interface {
	Save(ctx context.Context, cp SoakCheckpoint) error
	Load(ctx context.Context, taskID, stage string, shardID int) (*SoakCheckpoint, error)
	Delete(ctx context.Context, taskID, stage string, shardID int) error
	ListByTask(ctx context.Context, taskID string) ([]SoakCheckpoint, error)
}

// soakCheckpointBucket 是 bbolt 中用于 Soak checkpoint 的 bucket 名称。
// 与 syncnode/bolt 包内的其它 bucket 共存于同一 DB 文件即可；bbolt 自身按
// bucket 名隔离，不需要额外的 schema 协调。
var soakCheckpointBucket = []byte("soak_checkpoints")

// soakKey 拼接 (taskID, stage, shardID) 三元组到 bbolt 的扁平字节键。格式
// "<taskID>/<stage>/<shardID>"，便于 ListByTask 用 Cursor.Seek 做前缀扫描。
//
// taskID / stage 不允许包含 "/"。当前所有调用方都是 syncnode 自身生成 ID，
// 不存在该字符，故无需 escape。如果未来引入用户传入的 stage 名再考虑转义。
func soakKey(taskID, stage string, shardID int) []byte {
	return []byte(fmt.Sprintf("%s/%s/%d", taskID, stage, shardID))
}

// soakTaskPrefix 返回 ListByTask 用的前缀（不含 trailing shard 部分）。
func soakTaskPrefix(taskID string) []byte {
	return []byte(taskID + "/")
}

// NewBoltSoakStore 用已经打开的 *bbolt.DB 构造一个 SoakStore。函数会在内部
// 单次 Update 事务中确保 bucket 存在；后续 Save/Load 调用零分配地复用。
//
// 调用方负责 db 的生命周期（执行器不会 Close 它）。这与 syncnode/bolt/db.go
// 的设计一致：db 由 syncnode server 持有，多个 store 共用同一句柄。
func NewBoltSoakStore(db *bbolt.DB) (SoakStore, error) {
	if db == nil {
		return nil, errors.New("soak: nil bbolt.DB")
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(soakCheckpointBucket)
		return e
	}); err != nil {
		return nil, fmt.Errorf("create soak bucket: %w", err)
	}
	return &boltSoakStore{db: db}, nil
}

// boltSoakStore 是 SoakStore 的 bbolt 实现。
type boltSoakStore struct {
	db *bbolt.DB
}

// Save 序列化并写入。若 cp.LastUpdateUnix 为 0，调用方应已经填好（runner 会填），
// 这里不再二次戳时间戳，避免 Save 与 Load 看到的值不一致。
func (s *boltSoakStore) Save(ctx context.Context, cp SoakCheckpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cp.TaskID == "" || cp.Stage == "" {
		return errors.New("soak: TaskID and Stage required")
	}
	raw, err := json.Marshal(&cp)
	if err != nil {
		return fmt.Errorf("marshal soak checkpoint: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(soakCheckpointBucket)
		if b == nil {
			return fmt.Errorf("soak: bucket %q missing", string(soakCheckpointBucket))
		}
		return b.Put(soakKey(cp.TaskID, cp.Stage, cp.ShardID), raw)
	})
}

// Load 返回 (nil, nil) 表示 key 不存在；任何其它错误原样上抛。
func (s *boltSoakStore) Load(ctx context.Context, taskID, stage string, shardID int) (*SoakCheckpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out *SoakCheckpoint
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(soakCheckpointBucket)
		if b == nil {
			return fmt.Errorf("soak: bucket %q missing", string(soakCheckpointBucket))
		}
		raw := b.Get(soakKey(taskID, stage, shardID))
		if raw == nil {
			return nil
		}
		cp := SoakCheckpoint{}
		if err := json.Unmarshal(raw, &cp); err != nil {
			return fmt.Errorf("unmarshal soak checkpoint: %w", err)
		}
		out = &cp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Delete 删除指定 (task, stage, shard) 的 checkpoint。删除不存在的 key 不报错
// —— Soak runner 在完成正常退出时会调用 Delete 清理残留 checkpoint，幂等更友好。
func (s *boltSoakStore) Delete(ctx context.Context, taskID, stage string, shardID int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(soakCheckpointBucket)
		if b == nil {
			return fmt.Errorf("soak: bucket %q missing", string(soakCheckpointBucket))
		}
		return b.Delete(soakKey(taskID, stage, shardID))
	})
}

// ListByTask 扫描所有以 "<taskID>/" 开头的 key，返回这些 SoakCheckpoint。
// 顺序按 bbolt 的字节序（即 stage 字典序 → shardID 字典序）。
func (s *boltSoakStore) ListByTask(ctx context.Context, taskID string) ([]SoakCheckpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]SoakCheckpoint, 0)
	prefix := soakTaskPrefix(taskID)
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(soakCheckpointBucket)
		if b == nil {
			return fmt.Errorf("soak: bucket %q missing", string(soakCheckpointBucket))
		}
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
			cp := SoakCheckpoint{}
			if err := json.Unmarshal(v, &cp); err != nil {
				return fmt.Errorf("unmarshal soak checkpoint at %q: %w", string(k), err)
			}
			out = append(out, cp)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// hasPrefix is a small allocation-free []byte HasPrefix helper. Avoids
// pulling in bytes.HasPrefix for a single callsite.
func hasPrefix(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i := range prefix {
		if b[i] != prefix[i] {
			return false
		}
	}
	return true
}
