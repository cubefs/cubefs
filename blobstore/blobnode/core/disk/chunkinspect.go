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

package disk

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func listPhyDiskChunkFile(ctx context.Context, dataPath string) (cis map[bnapi.ChunkId]struct{}, err error) {
	span := trace.SpanFromContextSafe(ctx)

	dir, err := ioutil.ReadDir(dataPath)
	if err != nil {
		span.Errorf("Failed read dir, path:%s, err:%v", dataPath, err)
		return nil, err
	}

	cis = make(map[bnapi.ChunkId]struct{})
	for i := range dir {
		if dir[i].IsDir() {
			span.Warnf("%s/%s is dir.", dataPath, dir[i].Name())
			continue
		}

		id, err := bnapi.DecodeChunk(dir[i].Name())
		if err != nil {
			span.Errorf("decode %v to chunkID failed: %v", dir[i].Name(), err)
			continue
		}

		cis[id] = struct{}{}
	}
	return
}

func (ds *DiskStorage) GcRubbishChunk(ctx context.Context) (
	mayBeLost []bnapi.ChunkId, err error,
) {
	span := trace.SpanFromContextSafe(ctx)

	chunkMetas, err := ds.ListChunks(ctx)
	if err != nil {
		span.Errorf("%s list chunks failed: %v", ds.MetaPath, err)
		return
	}

	chunkIdDataMap, err := listPhyDiskChunkFile(ctx, ds.DataPath)
	if err != nil {
		span.Errorf("Failed list chunk file, path:%s, err:%v", ds.DataPath, err)
		return
	}

	chunkIdMetaMap := make(map[bnapi.ChunkId]struct{})

	// Important: Confirm whether there is suspicious data loss
	for _, vm := range chunkMetas {
		id := vm.ChunkId
		chunkIdMetaMap[id] = struct{}{}
		if _, exist := chunkIdDataMap[id]; exist {
			continue
		}
		lost, err := ds.maybeChunkLost(ctx, id, vm)
		if err != nil || !lost {
			continue
		}
		mayBeLost = append(mayBeLost, id)
	}

	// Important: Is it really data junk?
	for id := range chunkIdDataMap {
		if _, exist := chunkIdMetaMap[id]; exist {
			continue
		}
		_err := ds.maybeCleanRubbishChunk(ctx, id)
		if _err != nil {
			span.Errorf("failed clean rubbish. err:%v", err)
		}
	}

	return
}

func (ds *DiskStorage) maybeChunkLost(ctx context.Context, id bnapi.ChunkId, meta core.VuidMeta) (lost bool, err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("chunk:%s (meta:%v) maybe lost, will confirm.", id, meta)

	if meta.Status != bnapi.ChunkStatusNormal {
		span.Debugf("chunk:%s, meta:%v", id, meta)
		return false, nil
	}

	curMeta, err := ds.SuperBlock.ReadChunk(ctx, id)
	if os.IsNotExist(err) {
		// maybe vuid been clean(released)
		span.Warnf("maybe chunk(%s) have been clean", id)
		return false, nil
	}

	if err != nil {
		span.Errorf("read chunk(%v) failed: %v", id, err)
		return false, err
	}

	span.Warnf("chunk:%s, current meta:%v", id, curMeta)

	return true, nil
}

func (ds *DiskStorage) maybeCleanRubbishChunk(ctx context.Context, id bnapi.ChunkId) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	// set io type
	ctx = bnapi.SetIoType(ctx, bnapi.InternalIO)

	span.Debugf("chunk:%s maybe rubbish, will confirm", id)

	vuid, createTime := id.VolumeUnitId(), id.UnixTime()

	if ds.ChunkLimitPerKey.Acquire(vuid) != nil {
		return fmt.Errorf("%s lock failed", id)
	}
	defer ds.ChunkLimitPerKey.Release(vuid)

	cs, found := ds.GetChunkStorage(vuid)
	if found {
		meta := cs.VuidMeta()
		if meta.Compacting {
			span.Warnf("%s still in compacting, skip", id)
			return nil
		}
		span.Warnf("chunk:%s", id)
	}

	span.Warnf("need to further confirm whether %s is safe", id)

	meta, err := ds.SuperBlock.ReadChunk(ctx, id)
	if err == nil {
		span.Warnf("read chunk:%s meta:%v. skip", id, meta)
		return nil
	}

	if !os.IsNotExist(err) {
		span.Errorf("failed read chunk:%s. err:%v. skip", id, err)
		return err
	}

	span.Debugf("chunk:%s, err:<%v>. really junk data", id, err)

	ctime := time.Unix(0, int64(createTime))
	createTimeThreshold := time.Duration(ds.Conf.ChunkGcCreateTimeProtectionM) * time.Minute

	if time.Since(ctime) < createTimeThreshold {
		span.Debugf("%s still in ctime protection", id)
		return nil
	}

	chunkDataFile := filepath.Join(ds.DataPath, id.String())
	stat, err := os.Stat(chunkDataFile)
	if err != nil {
		span.Errorf("failed stat %s, err:%v", id, err)
		return nil
	}

	mtime := stat.ModTime()
	modifyTimeThreshold := time.Duration(ds.Conf.ChunkGcModifyTimeProtectionM) * time.Minute

	if time.Since(mtime) < modifyTimeThreshold {
		span.Debugf("%s still in mtime protection", id)
		return nil
	}

	err = ds.cleanChunk(ctx, id, true)
	if err != nil {
		span.Errorf("failed clean chunk %s, err:%v", id, err)
		return
	}

	span.Warnf("clean chunk %s success. mv to trash.", id)

	return nil
}
