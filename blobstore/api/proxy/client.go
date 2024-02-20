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

package proxy

import (
	"context"
	"fmt"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type Config struct {
	rpc.Config
}

type client struct {
	rpc.Client
}

type Client interface {
	Cacher
}

func New(cfg *Config) Client {
	return &client{rpc.NewClient(&cfg.Config)}
}

func (c *client) GetCacheVolume(ctx context.Context, host string, args *clustermgr.CacheVolumeArgs) (volume *clustermgr.VersionVolume, err error) {
	volume = new(clustermgr.VersionVolume)
	url := fmt.Sprintf("%s/cache/volume/%d?flush=%v&version=%d", host, args.Vid, args.Flush, args.Version)
	err = c.GetWith(ctx, url, &volume)
	return
}

func (c *client) GetCacheDisk(ctx context.Context, host string, args *clustermgr.CacheDiskArgs) (disk *blobnode.DiskInfo, err error) {
	disk = new(blobnode.DiskInfo)
	url := fmt.Sprintf("%s/cache/disk/%d?flush=%v", host, args.DiskID, args.Flush)
	err = c.GetWith(ctx, url, &disk)
	return
}

func (c *client) Erase(ctx context.Context, host string, key string) error {
	resp, err := c.Delete(ctx, fmt.Sprintf("%s/cache/erase/%s", host, key))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return rpc.ParseData(resp, nil)
}
