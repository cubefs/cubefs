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

package cacher

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const keyDiskConcurrency = "disk"

type expiryDisk struct {
	clustermgr.BlobNodeDiskInfo
	CreateAt time.Time `json:"__create_at__,omitempty"`

	expiration time.Duration `json:"-"`
}

func (v *expiryDisk) SetExpiration(exp time.Duration) {
	v.expiration = exp
}

func (v *expiryDisk) Expired() bool {
	return v.expiration > 0 && time.Since(v.CreateAt) > v.expiration
}

func encodeDisk(v *expiryDisk) ([]byte, error) {
	return json.Marshal(v)
}

func decodeDisk(data []byte) (valueExpired, error) {
	disk := new(expiryDisk)
	err := json.Unmarshal(data, &disk)
	return disk, err
}

func (c *cacher) GetDisk(ctx context.Context, args *proxy.CacheDiskArgs) (*clustermgr.BlobNodeDiskInfo, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("try to get disk %+v", args)

	id := args.DiskID
	if !args.Flush { // read cache
		if disk := c.getDisk(span, id); disk != nil {
			return &disk.BlobNodeDiskInfo, nil
		}
	}

	st := time.Now()
	err := c.cmConcurrency.Acquire(keyDiskConcurrency)
	span.AppendTrackLog("wait", st, err)
	if err != nil {
		return nil, err
	}
	defer c.cmConcurrency.Release(keyDiskConcurrency)

	st = time.Now()
	val, err, _ := c.singleRun.Do(diskvKeyDisk(id), func() (any, error) {
		return c.cmClient.DiskInfo(ctx, id)
	})
	span.AppendTrackLog("cm", st, err)
	if err != nil {
		c.diskReport("clustermgr", "miss")
		span.Error("get disk info from clustermgr failed", errors.Detail(err))
		return nil, err
	}
	diskInfo, ok := val.(*clustermgr.BlobNodeDiskInfo)
	if !ok {
		return nil, errors.New("error convert to disk struct after singleflight")
	}
	c.diskReport("clustermgr", "hit")

	disk := new(expiryDisk)
	disk.BlobNodeDiskInfo = *diskInfo
	disk.CreateAt = time.Now()
	disk.expiration = interleaveExpiration(c.config.DiskExpirationS)
	c.diskCache.Set(id, disk)

	span, _ = trace.StartSpanFromContextWithTraceID(context.Background(), "diskkv", span.TraceID())
	go func() {
		key := diskvKeyDisk(id)
		fullPath := c.DiskvFilename(key)
		if data, err := encodeDisk(disk); err == nil {
			if err := c.diskv.Write(key, data); err != nil {
				span.Warnf("write diskv on path:%s data:<%s> error:%s", fullPath, string(data), err.Error())
			} else {
				span.Infof("write diskv on path:%s disk:<%s>", fullPath, string(data))
			}
		} else {
			span.Warnf("encode disk_id:%d disk:%+v error:%s", id, disk, err.Error())
		}

		select {
		case c.syncChan <- struct{}{}:
		default:
		}
	}()

	return &disk.BlobNodeDiskInfo, nil
}

func (c *cacher) getDisk(span trace.Span, id proto.DiskID) *expiryDisk {
	if val := c.getCachedValue(span, id, diskvKeyDisk(id), c.config.DiskExpirationS,
		c.diskCache, decodeDisk, c.diskReport); val != nil {
		if value, ok := val.(*expiryDisk); ok {
			return value
		}
	}
	return nil
}
