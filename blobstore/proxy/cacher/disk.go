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
	"math/rand"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const keyDiskConcurrency = "disk"

type expiryDisk struct {
	blobnode.DiskInfo
	ExpiryAt int64 `json:"expiry,omitempty"` // seconds
}

func (v *expiryDisk) Expired() bool {
	return v.ExpiryAt > 0 && time.Now().Unix() >= v.ExpiryAt
}

func encodeDisk(v *expiryDisk) ([]byte, error) {
	return json.Marshal(v)
}

func decodeDisk(data []byte) (*expiryDisk, error) {
	disk := new(expiryDisk)
	err := json.Unmarshal(data, &disk)
	return disk, err
}

func (c *cacher) GetDisk(ctx context.Context, args *proxy.CacheDiskArgs) (*blobnode.DiskInfo, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("try to get disk %+v", args)

	id := args.DiskID
	if !args.Flush { // read cache
		if disk := c.getDisk(span, id); disk != nil {
			return &disk.DiskInfo, nil
		}
	}

	err := c.cmConcurrency.Acquire(keyDiskConcurrency)
	if err != nil {
		return nil, err
	}
	defer c.cmConcurrency.Release(keyDiskConcurrency)

	val, err, _ := c.singleRun.Do("disk-"+id.ToString(), func() (interface{}, error) {
		return c.cmClient.DiskInfo(ctx, id)
	})
	if err != nil {
		c.diskReport("clustermgr", "miss")
		span.Error("get disk info from clustermgr failed", errors.Detail(err))
		return nil, err
	}
	diskInfo, ok := val.(*blobnode.DiskInfo)
	if !ok {
		return nil, errors.New("error convert to disk struct after singleflight")
	}
	c.diskReport("clustermgr", "hit")

	disk := new(expiryDisk)
	disk.DiskInfo = *diskInfo
	if expire := c.config.DiskExpirationS; expire > 0 {
		expiration := rand.Intn(expire) + expire
		disk.ExpiryAt = time.Now().Add(time.Second * time.Duration(expiration)).Unix()
	}
	c.diskCache.Set(id, disk)

	go func() {
		if data, err := encodeDisk(disk); err == nil {
			if err := c.diskv.Write(diskvKeyDisk(id), data); err != nil {
				span.Warnf("write to diskv disk_id:%d data:%s error:%s", id, string(data), err.Error())
			}
		} else {
			span.Warnf("encode disk_id:%d disk:%v error:%s", id, disk, err.Error())
		}
	}()

	return &disk.DiskInfo, nil
}

func (c *cacher) getDisk(span trace.Span, id proto.DiskID) *expiryDisk {
	if val := c.diskCache.Get(id); val != nil {
		c.diskReport("memcache", "hit")
		if disk, ok := val.(*expiryDisk); ok {
			if !disk.Expired() {
				span.Debug("hits at memory cache disk", id)
				return disk
			}
			c.diskReport("memcache", "expired")
		}
	} else {
		c.diskReport("memcache", "miss")
	}

	data, err := c.diskv.Read(diskvKeyDisk(id))
	if err != nil {
		c.diskReport("diskv", "miss")
		span.Warnf("read from diskv disk_id:%d error:%s", id, err.Error())
		return nil
	}
	disk, err := decodeDisk(data)
	if err != nil {
		c.diskReport("diskv", "error")
		span.Warnf("decode diskv disk_id:%d data:%s error:%s", id, string(data), err.Error())
		return nil
	}

	if !disk.Expired() {
		c.diskReport("diskv", "hit")
		c.diskCache.Set(id, disk)
		span.Debug("hits at diskv cache, set back to memory cache disk", id)
		return disk
	}

	c.diskReport("diskv", "expired")
	span.Debugf("expired at diskv disk_id:%d expiryat:%d", id, disk.ExpiryAt)
	return nil
}
