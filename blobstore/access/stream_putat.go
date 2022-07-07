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
	"io"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/ec"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// PutAt access interface /putat, put one blob
//     required: rc file reader
//     required: clusterID VolumeID BlobID
//     required: size, one blob size
//     optional: hasherMap, computing hash
func (h *Handler) PutAt(ctx context.Context, rc io.Reader,
	clusterID proto.ClusterID, vid proto.Vid, bid proto.BlobID, size int64,
	hasherMap access.HasherMap) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("putat request cluster:%d vid:%d bid:%d size:%d hashes:b(%b)",
		clusterID, vid, bid, size, hasherMap.ToHashAlgorithm())

	if len(hasherMap) > 0 {
		rc = io.TeeReader(rc, hasherMap.ToWriter())
	}

	volume, err := h.getVolume(ctx, clusterID, vid, true)
	if err != nil {
		return err
	}
	encoder := h.encoder[volume.CodeMode]

	readSize := int(size)
	st := time.Now()
	buffer, err := ec.NewBuffer(readSize, volume.CodeMode.Tactic(), h.memPool)
	if err != nil {
		return err
	}

	putTime := new(timeReadWrite)
	putTime.IncA(time.Since(st))
	defer func() {
		buffer.Release()
		span.AppendRPCTrackLog([]string{putTime.String()})
	}()

	shards, err := encoder.Split(buffer.ECDataBuf)
	if err != nil {
		return err
	}

	startRead := time.Now()
	n, err := io.ReadFull(rc, buffer.DataBuf)
	putTime.IncR(time.Since(startRead))
	if err != nil && err != io.EOF {
		span.Info("read blob data from request body", err)
		return errcode.ErrAccessReadRequestBody
	}
	if n != readSize {
		span.Infof("want to read %d, but %d", readSize, n)
		return errcode.ErrAccessReadRequestBody
	}

	if err = encoder.Encode(shards); err != nil {
		return err
	}

	blobident := blobIdent{clusterID, vid, bid}
	span.Debug("to write", blobident)

	takeoverBuffer := buffer
	buffer = nil
	startWrite := time.Now()
	err = h.writeToBlobnodesWithHystrix(ctx, blobident, shards, func() {
		takeoverBuffer.Release()
	})
	putTime.IncW(time.Since(startWrite))
	if err != nil {
		return err
	}

	span.Debugf("putat done cluster:%d vid:%d bid:%d size:%d", clusterID, vid, bid, size)
	return nil
}
