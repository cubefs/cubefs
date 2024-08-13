// Copyright 2024 The CubeFS Authors.
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

package catalog

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"sort"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (c *CatalogMgr) AllocSpaceID(ctx context.Context) (proto.SpaceID, error) {
	_, spaceID, err := c.scopeMgr.Alloc(ctx, spaceIDScopeName, 1)
	if err != nil {
		return 0, errors.Info(err, "CatalogMgr.AllocSpaceID failed")
	}
	return proto.SpaceID(spaceID), nil
}

func (c *CatalogMgr) GetSpaceInfoByName(ctx context.Context, name string) (*clustermgr.Space, error) {
	space := c.allSpaces.getSpaceByName(name)
	if space == nil {
		return nil, apierrors.ErrSpaceNotFound
	}
	var spaceInfo clustermgr.Space
	// need to copy before return, or the higher level may change the space info by the pointer
	space.withRLocked(func() error {
		spaceInfo = *space.info
		return nil
	})
	return &(spaceInfo), nil
}

func (c *CatalogMgr) GetSpaceInfoByID(ctx context.Context, spaceID proto.SpaceID) (*clustermgr.Space, error) {
	space := c.allSpaces.getSpaceByID(spaceID)
	if space == nil {
		return nil, apierrors.ErrSpaceNotFound
	}
	var spaceInfo clustermgr.Space
	// need to copy before return, or the higher level may change the space info by the pointer
	space.withRLocked(func() error {
		spaceInfo = *space.info
		return nil
	})
	return &(spaceInfo), nil
}

func (c *CatalogMgr) ListSpaceInfo(ctx context.Context, args *clustermgr.ListSpaceArgs) (ret []*clustermgr.Space, err error) {
	span := trace.SpanFromContextSafe(ctx)
	if args.Count > defaultListMaxCount {
		args.Count = defaultListMaxCount
	}
	records, err := c.catalogTbl.ListSpace(args.Count, args.Marker)
	if err != nil {
		span.Errorf("list space failed:%v", err)
		return nil, errors.Info(err, "catalogMgr list space failed").Detail(err)
	}
	if len(records) == 0 {
		return
	}

	for _, record := range records {
		space := spaceRecordToSpaceInfo(record)
		if space == nil {
			return nil, apierrors.ErrSpaceNotFound
		}
		ret = append(ret, space)
	}

	return ret, nil
}

func (c *CatalogMgr) CreateSpace(ctx context.Context, args *clustermgr.CreateSpaceArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	spaceInfo, err := c.GetSpaceInfoByName(ctx, args.Name)
	if err != nil && err != apierrors.ErrSpaceNotFound {
		return errors.Info(err, "get space err")
	}
	if spaceInfo != nil {
		return apierrors.ErrExist
	}
	if err = c.validateSpaceInfo(ctx, args); err != nil {
		return err
	}
	if !c.IsShardInitDone(ctx) {
		return apierrors.ErrShardInitNotDone
	}

	spaceID, err := c.AllocSpaceID(ctx)
	if err != nil {
		return errors.Info(err, "alloc space id failed")
	}
	spaceInfo = &clustermgr.Space{
		SpaceID:    spaceID,
		Name:       args.Name,
		Status:     proto.SpaceStatusNormal,
		FieldMetas: args.FieldMetas,
		AccessKey:  makeKey(),
		SecretKey:  makeKey(),
	}
	data, err := json.Marshal(spaceInfo)
	if err != nil {
		return errors.Info(apierrors.ErrUnexpected, "json marshal failed, space info: ", spaceInfo)
	}
	proposeInfo := base.EncodeProposeInfo(c.GetModuleName(), OperTypeCreateSpace, data, base.ProposeContext{ReqID: span.TraceID()})
	err = c.raftServer.Propose(ctx, proposeInfo)
	if err != nil {
		return errors.Info(apierrors.ErrRaftPropose, "raft propose failed: ", err)
	}
	return nil
}

func (c *CatalogMgr) IsShardInitDone(ctx context.Context) bool {
	return atomic.LoadInt32(&c.initShardDone) == initShardDone
}

func (c *CatalogMgr) applyCreateSpace(ctx context.Context, args *clustermgr.Space) error {
	span := trace.SpanFromContextSafe(ctx)

	// concurrent double check
	space := c.allSpaces.getSpaceByName(args.Name)
	if space != nil {
		return nil
	}

	// alloc field id, start with 1
	sort.Slice(args.FieldMetas, func(i, j int) bool {
		return args.FieldMetas[i].Name < args.FieldMetas[j].Name
	})
	for index, filedMeta := range args.FieldMetas {
		filedMeta.ID = proto.FieldID(index + 1)
	}

	record := spaceInfoToSpaceRecord(args)
	err := c.catalogTbl.CreateSpace(record)
	if err != nil {
		span.Error("CatalogMgr.applyCreateSpace failed: ", err)
		return errors.Info(err, "CatalogMgr.applyCreateSpace failed").Detail(err)
	}
	si := &spaceItem{spaceID: args.SpaceID, info: args}
	c.allSpaces.putSpace(si)

	return nil
}

func (c *CatalogMgr) validateSpaceInfo(ctx context.Context, args *clustermgr.CreateSpaceArgs) error {
	if args.Name == "" {
		return apierrors.ErrIllegalArguments
	}
	filedNameMap := make(map[string]struct{})
	for _, filedMeta := range args.FieldMetas {
		if filedMeta.Name == "" {
			return apierrors.ErrIllegalArguments
		}
		if _, ok := filedNameMap[filedMeta.Name]; ok {
			return apierrors.ErrIllegalArguments
		}
		if !filedMeta.FieldType.IsValid() || !filedMeta.IndexOption.IsValid() {
			return apierrors.ErrIllegalArguments
		}
		filedNameMap[filedMeta.Name] = struct{}{}
	}
	return nil
}

func spaceInfoToSpaceRecord(info *clustermgr.Space) *catalogdb.SpaceInfoRecord {
	return &catalogdb.SpaceInfoRecord{
		Version:    catalogdb.SpaceInfoVersionNormal,
		SpaceID:    info.SpaceID,
		Name:       info.Name,
		Status:     info.Status,
		FieldMetas: info.FieldMetas,
		AccessKey:  info.AccessKey,
		SecretKey:  info.SecretKey,
	}
}

func spaceRecordToSpaceInfo(record *catalogdb.SpaceInfoRecord) *clustermgr.Space {
	return &clustermgr.Space{
		SpaceID:    record.SpaceID,
		Name:       record.Name,
		Status:     record.Status,
		FieldMetas: record.FieldMetas,
		AccessKey:  record.AccessKey,
		SecretKey:  record.SecretKey,
	}
}

func makeKey() string {
	var b [30]byte
	io.ReadFull(rand.Reader, b[:])
	return base64.URLEncoding.EncodeToString(b[:])
}
