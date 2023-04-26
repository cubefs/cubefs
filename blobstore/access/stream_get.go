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
	"fmt"
	"io"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/afex/hystrix-go/hystrix"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/ec"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

var (
	errNeedReconstructRead = errors.New("need to reconstruct read")
	errCanceledReadShard   = errors.New("canceled read shard")
	errPunishedDisk        = errors.New("punished disk")
)

type blobGetArgs struct {
	Cid      proto.ClusterID
	Vid      proto.Vid
	Bid      proto.BlobID
	CodeMode codemode.CodeMode
	BlobSize uint64
	Offset   uint64
	ReadSize uint64

	ShardSize     int
	ShardOffset   int
	ShardReadSize int
}

func (blob *blobGetArgs) ID() string {
	return fmt.Sprintf("blob(cid:%d vid:%d bid:%d)", blob.Cid, blob.Vid, blob.Bid)
}

type shardData struct {
	index  int
	status bool
	buffer []byte
}

type sortedVuid struct {
	index  int
	vuid   proto.Vuid
	diskID proto.DiskID
	host   string
}

func (vuid *sortedVuid) ID() string {
	return fmt.Sprintf("blobnode(vuid:%d disk:%d host:%s) ecidx(%02d)",
		vuid.vuid, vuid.diskID, vuid.host, vuid.index)
}

type pipeBuffer struct {
	err    error
	blob   blobGetArgs
	shards [][]byte
}

// Get read file
//     required: location, readSize
//     optional: offset(default is 0)
//
//     first return value is data transfer to copy data after argument checking
//
//  Read data shards firstly, if blob size is small or read few bytes
//  then ec reconstruct-read, try to reconstruct from N+X to N+M
//  Just read essential bytes in each shard when reconstruct-read.
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
func (h *Handler) Get(ctx context.Context, w io.Writer, location access.Location, readSize, offset uint64) (func() error, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("get request cluster:%d size:%d offset:%d", location.ClusterID, readSize, offset)

	blobs, err := genLocationBlobs(&location, readSize, offset)
	if err != nil {
		span.Info("illegal argument", err)
		return func() error { return nil }, errcode.ErrIllegalArguments
	}
	if len(blobs) == 0 {
		return func() error { return nil }, nil
	}

	clusterID := location.ClusterID
	var serviceController controller.ServiceController
	if err = retry.Timed(3, 200).On(func() error {
		sc, err := h.clusterController.GetServiceController(clusterID)
		if err != nil {
			return err
		}
		serviceController = sc
		return nil
	}); err != nil {
		span.Error("get service", errors.Detail(err))
		return func() error { return nil }, err
	}

	return func() error {
		getTime := new(timeReadWrite)
		defer func() {
			span.AppendRPCTrackLog([]string{getTime.String()})
		}()

		// try to read data shard only,
		//   if blobsize is small: all data is in the first shard, cos shards aligned by MinShardSize.
		//   read few bytes: read bytes less than quarter of blobsize, like Range:[0-1].
		if len(blobs) == 1 {
			blob := blobs[0]
			if int(blob.BlobSize) <= blob.ShardSize || blob.ReadSize < blob.BlobSize/4 {
				span.Debugf("read data shard only %s readsize:%d blobsize:%d shardsize:%d",
					blob.ID(), blob.ReadSize, blob.BlobSize, blob.ShardSize)

				err := h.getDataShardOnly(ctx, getTime, w, serviceController, blob)
				if err != errNeedReconstructRead {
					if err != nil {
						span.Error("read data shard only", err)
						reportDownload(clusterID, "Direct", "error")
					} else {
						reportDownload(clusterID, "Direct", "-")
					}
					return err
				}
				span.Info("read data shard only failed", err)
			}
		}

		// data stream flow:
		// client <--copy-- pipeline <--swap-- readBlob <--copy-- blobnode
		//
		// Alloc N+M shard buffers here, and release after written to client.
		// Replace not-empty buffers in readBlob, need release old-buffers in that function.
		closeCh := make(chan struct{})
		pipeline := func() <-chan pipeBuffer {
			ch := make(chan pipeBuffer, 1)
			go func() {
				defer close(ch)

				var blobVolume *controller.VolumePhy
				var sortedVuids []sortedVuid
				tactic := location.CodeMode.Tactic()
				for _, blob := range blobs {
					var err error
					if blobVolume == nil || blobVolume.Vid != blob.Vid {
						blobVolume, err = h.getVolume(ctx, clusterID, blob.Vid, true)
						if err != nil {
							span.Error("get volume", err)
							ch <- pipeBuffer{err: err}
							return
						}

						// do not use local shards
						sortedVuids = genSortedVuidByIDC(ctx, serviceController, h.IDC, blobVolume.Units[:tactic.N+tactic.M])
						span.Debugf("to read %s with read-shard-x:%d active-shard-n:%d of data-n:%d party-n:%d",
							blob.ID(), h.MinReadShardsX, len(sortedVuids), tactic.N, tactic.M)
						if len(sortedVuids) < tactic.N {
							err = fmt.Errorf("broken %s", blob.ID())
							span.Error(err)
							ch <- pipeBuffer{err: err}
							return
						}
					}

					st := time.Now()
					shards := make([][]byte, tactic.N+tactic.M)
					for ii := range shards {
						buf, _ := h.memPool.Alloc(blob.ShardSize)
						shards[ii] = buf
					}
					getTime.IncA(time.Since(st))

					err = h.readOneBlob(ctx, getTime, serviceController, blob, sortedVuids, shards)
					if err != nil {
						span.Error("read one blob", blob.ID(), err)
						for _, buf := range shards {
							h.memPool.Put(buf)
						}
						ch <- pipeBuffer{err: err}
						return
					}

					select {
					case <-closeCh:
						for _, buf := range shards {
							h.memPool.Put(buf)
						}
						return
					case ch <- pipeBuffer{blob: blob, shards: shards}:
					}
				}
			}()

			return ch
		}()

		var err error
		for line := range pipeline {
			if line.err != nil {
				err = line.err
				break
			}

			startWrite := time.Now()

			idx := 0
			off := line.blob.Offset
			toReadSize := line.blob.ReadSize
			for toReadSize > 0 {
				buf := line.shards[idx]
				l := uint64(len(buf))
				if off >= l {
					idx++
					off -= l
					continue
				}

				toRead := minU64(toReadSize, l-off)
				if _, e := w.Write(buf[off : off+toRead]); e != nil {
					err = errors.Info(e, "write to response")
					break
				}
				idx++
				off = 0
				toReadSize -= toRead
			}

			getTime.IncW(time.Since(startWrite))

			for _, buf := range line.shards {
				h.memPool.Put(buf)
			}
			if err != nil {
				close(closeCh)
				break
			}
		}

		// release buffer in pipeline if fail to write client
		go func() {
			for line := range pipeline {
				for _, buf := range line.shards {
					h.memPool.Put(buf)
				}
			}
		}()

		if err != nil {
			reportDownload(clusterID, "EC", "error")
			span.Error("get request error", err)
			return err
		}
		reportDownload(clusterID, "EC", "-")
		return nil
	}, nil
}

// 1. try to min-read shards bytes
// 2. if failed try to read next shard to reconstruct
// 3. write the the right offset bytes to writer
// 4. Just read essential bytes if the data is a segment of one shard.
func (h *Handler) readOneBlob(ctx context.Context, getTime *timeReadWrite,
	serviceController controller.ServiceController,
	blob blobGetArgs, sortedVuids []sortedVuid, shards [][]byte) error {
	span := trace.SpanFromContextSafe(ctx)

	tactic := blob.CodeMode.Tactic()
	sizes, err := ec.GetBufferSizes(int(blob.BlobSize), tactic)
	if err != nil {
		return err
	}
	empties := emptyDataShardIndexes(sizes)

	dataN, dataParityN := tactic.N, tactic.N+tactic.M
	minShardsRead := dataN + h.MinReadShardsX
	if minShardsRead > len(sortedVuids) {
		minShardsRead = len(sortedVuids)
	}
	shardSize, shardOffset, shardReadSize := blob.ShardSize, blob.ShardOffset, blob.ShardReadSize

	stopChan := make(chan struct{})
	nextChan := make(chan struct{}, len(sortedVuids))
	shardPipe := func() <-chan shardData {
		ch := make(chan shardData)
		go func() {
			wg := new(sync.WaitGroup)
			defer func() {
				wg.Wait()
				close(ch)
			}()

			for _, vuid := range sortedVuids[:minShardsRead] {
				if _, ok := empties[vuid.index]; !ok {
					wg.Add(1)
					go func(vuid sortedVuid) {
						ch <- h.readOneShard(ctx, serviceController, blob, vuid, stopChan)
						wg.Done()
					}(vuid)
				}
			}

			for _, vuid := range sortedVuids[minShardsRead:] {
				if _, ok := empties[vuid.index]; ok {
					continue
				}

				select {
				case <-stopChan:
					return
				case <-nextChan:
				}

				wg.Add(1)
				go func(vuid sortedVuid) {
					ch <- h.readOneShard(ctx, serviceController, blob, vuid, stopChan)
					wg.Done()
				}(vuid)
			}
		}()

		return ch
	}()

	received := make(map[int]bool, minShardsRead)
	for idx := range empties {
		received[idx] = true
		h.memPool.Zero(shards[idx])
	}

	startRead := time.Now()
	reconstructed := false
	for shard := range shardPipe {
		// swap shard buffer
		if shard.status {
			buf := shards[shard.index]
			shards[shard.index] = shard.buffer
			h.memPool.Put(buf)
		}

		received[shard.index] = shard.status
		if len(received) < dataN {
			continue
		}

		// bad data index
		badIdx := make([]int, 0, 8)
		for i := 0; i < dataN; i++ {
			if succ, ok := received[i]; !ok || !succ {
				badIdx = append(badIdx, i)
			}
		}
		if len(badIdx) == 0 {
			reconstructed = true
			close(stopChan)
			break
		}

		// update bad parity index
		for i := dataN; i < dataParityN; i++ {
			if succ, ok := received[i]; !ok || !succ {
				badIdx = append(badIdx, i)
			}
		}

		badShards := 0
		for _, succ := range received {
			if !succ {
				badShards++
			}
		}
		// it will not wait all the shards, cos has no enough shards to reconstruct
		if badShards > dataParityN-dataN {
			span.Infof("%s bad(%d) has no enough to reconstruct", blob.ID(), badShards)
			close(stopChan)
			break
		}

		// has bad shards, but have enough shards to reconstruct
		if len(received) >= dataN+badShards {
			var err error
			if shardReadSize < shardSize {
				span.Debugf("bid(%d) ready to segment ec reconstruct data", blob.Bid)
				reportDownload(blob.Cid, "EC", "segment")
				segments := make([][]byte, len(shards))
				for idx := range shards {
					segments[idx] = shards[idx][shardOffset : shardOffset+shardReadSize]
				}
				err = h.encoder[blob.CodeMode].ReconstructData(segments, badIdx)
			} else {
				span.Debugf("bid(%d) ready to ec reconstruct data", blob.Bid)
				err = h.encoder[blob.CodeMode].ReconstructData(shards, badIdx)
			}
			if err == nil {
				reconstructed = true
				close(stopChan)
				break
			}
			span.Errorf("%s ec reconstruct data error:%s", blob.ID(), err.Error())
		}

		if len(received) >= len(sortedVuids) {
			close(stopChan)
			break
		}
		nextChan <- struct{}{}
	}
	getTime.IncR(time.Since(startRead))

	// release buffer of delayed shards
	go func() {
		for shard := range shardPipe {
			if shard.status {
				h.memPool.Put(shard.buffer)
			}
		}
	}()

	if reconstructed {
		return nil
	}
	return fmt.Errorf("broken %s", blob.ID())
}

func (h *Handler) readOneShard(ctx context.Context, serviceController controller.ServiceController,
	blob blobGetArgs, vuid sortedVuid, stopChan <-chan struct{}) shardData {
	clusterID, vid := blob.Cid, blob.Vid
	shardOffset, shardReadSize := blob.ShardOffset, blob.ShardReadSize
	span := trace.SpanFromContextSafe(ctx)
	shardResult := shardData{
		index:  vuid.index,
		status: false,
	}

	args := blobnode.RangeGetShardArgs{
		GetShardArgs: blobnode.GetShardArgs{
			DiskID: vuid.diskID,
			Vuid:   vuid.vuid,
			Bid:    blob.Bid,
		},
		Offset: int64(shardOffset),
		Size:   int64(shardReadSize),
	}

	var (
		err  error
		body io.ReadCloser
	)
	if hErr := hystrix.Do(rwCommand, func() error {
		body, err = h.getOneShardFromHost(ctx, serviceController, vuid.host, vuid.diskID, args,
			vuid.index, clusterID, vid, 3, stopChan)
		if err != nil && (errorTimeout(err) || rpc.DetectStatusCode(err) == errcode.CodeOverload) {
			return err
		}
		return nil
	}, nil); hErr != nil {
		span.Warnf("hystrix: read %s on %s: %s", blob.ID(), vuid.ID(), hErr.Error())
		return shardResult
	}

	if err != nil {
		if err == errPunishedDisk || err == errCanceledReadShard {
			span.Warnf("read %s on %s: %s", blob.ID(), vuid.ID(), err.Error())
			return shardResult
		}
		span.Warnf("read %s on %s: %s", blob.ID(), vuid.ID(), errors.Detail(err))
		return shardResult
	}
	defer body.Close()

	buf, err := h.memPool.Alloc(blob.ShardSize)
	if err != nil {
		span.Warn(err)
		return shardResult
	}

	_, err = io.ReadFull(body, buf[shardOffset:shardOffset+shardReadSize])
	if err != nil {
		h.memPool.Put(buf)
		span.Warnf("read %s on %s: %s", blob.ID(), vuid.ID(), err.Error())
		return shardResult
	}

	shardResult.status = true
	shardResult.buffer = buf
	return shardResult
}

func (h *Handler) getDataShardOnly(ctx context.Context, getTime *timeReadWrite,
	w io.Writer, serviceController controller.ServiceController, blob blobGetArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	if blob.ReadSize == 0 {
		return nil
	}

	blobVolume, err := h.getVolume(ctx, blob.Cid, blob.Vid, true)
	if err != nil {
		return err
	}
	tactic := blobVolume.CodeMode.Tactic()

	from, to := int(blob.Offset), int(blob.Offset+blob.ReadSize)
	buffer, err := ec.NewRangeBuffer(int(blob.BlobSize), from, to, tactic, h.memPool)
	if err != nil {
		return err
	}
	defer buffer.Release()

	shardSize := buffer.ShardSize
	firstShardIdx := int(blob.Offset) / shardSize
	shardOffset := int(blob.Offset) % shardSize

	startRead := time.Now()
	remainSize := blob.ReadSize
	bufOffset := 0
	for i, shard := range blobVolume.Units[firstShardIdx:tactic.N] {
		if remainSize <= 0 {
			break
		}

		toReadSize := minU64(remainSize, uint64(shardSize-shardOffset))
		args := blobnode.RangeGetShardArgs{
			GetShardArgs: blobnode.GetShardArgs{
				DiskID: shard.DiskID,
				Vuid:   shard.Vuid,
				Bid:    blob.Bid,
			},
			Offset: int64(shardOffset),
			Size:   int64(toReadSize),
		}

		body, err := h.getOneShardFromHost(ctx, serviceController, shard.Host, shard.DiskID, args,
			firstShardIdx+i, blob.Cid, blob.Vid, 1, nil)
		if err != nil {
			span.Warnf("read %s on blobnode(vuid:%d disk:%d host:%s) ecidx(%02d): %s", blob.ID(),
				shard.Vuid, shard.DiskID, shard.Host, firstShardIdx+i, errors.Detail(err))
			return errNeedReconstructRead
		}
		defer body.Close()

		buf := buffer.DataBuf[bufOffset : bufOffset+int(toReadSize)]
		_, err = io.ReadFull(body, buf)
		if err != nil {
			span.Warn(err)
			return errNeedReconstructRead
		}

		// reset next shard offset
		shardOffset = 0
		remainSize -= toReadSize
		bufOffset += int(toReadSize)
	}
	getTime.IncR(time.Since(startRead))

	if remainSize > 0 {
		return fmt.Errorf("no enough data to read %d", remainSize)
	}

	startWrite := time.Now()
	if _, err := w.Write(buffer.DataBuf[:int(blob.ReadSize)]); err != nil {
		getTime.IncW(time.Since(startWrite))
		return errors.Info(err, "write to response")
	}
	getTime.IncW(time.Since(startWrite))

	return nil
}

// getOneShardFromHost get body of one shard
func (h *Handler) getOneShardFromHost(ctx context.Context, serviceController controller.ServiceController,
	host string, diskID proto.DiskID, args blobnode.RangeGetShardArgs, // get shard param with host diskid
	index int, clusterID proto.ClusterID, vid proto.Vid, // param to update volume cache
	attempts int, cancelChan <-chan struct{}, // do not retry again if cancelChan was closed
) (io.ReadCloser, error) {
	span := trace.SpanFromContextSafe(ctx)

	// skip punished disk
	if diskHost, err := serviceController.GetDiskHost(ctx, diskID); err != nil {
		return nil, err
	} else if diskHost.Punished {
		return nil, errPunishedDisk
	}

	var (
		rbody io.ReadCloser
		rerr  error
	)
	rerr = retry.ExponentialBackoff(attempts, 200).RuptOn(func() (bool, error) {
		if cancelChan != nil {
			select {
			case <-cancelChan:
				return true, errCanceledReadShard
			default:
			}
		}

		// new child span to get from blobnode, we should finish it here.
		spanChild, ctxChild := trace.StartSpanFromContextWithTraceID(
			context.Background(), "GetFromBlobnode", span.TraceID())
		defer spanChild.Finish()

		body, _, err := h.blobnodeClient.RangeGetShard(ctxChild, host, &args)
		if err == nil {
			rbody = body
			return true, nil
		}

		code := rpc.DetectStatusCode(err)
		switch code {
		case errcode.CodeOverload:
			return true, err

		// EIO and Readonly error, then we need to punish disk in local and no need to retry
		case errcode.CodeDiskBroken, errcode.CodeVUIDReadonly:
			h.punishDisk(ctx, clusterID, diskID, host, "BrokenOrRO")
			span.Warnf("punish disk:%d on:%s cos:blobnode/%d", diskID, host, code)
			return true, fmt.Errorf("punished disk (%d %s)", diskID, host)

		// vuid not found means the reflection between vuid and diskID has change,
		// should refresh the blob volume cache
		case errcode.CodeDiskNotFound, errcode.CodeVuidNotFound:
			span.Infof("volume info outdated disk %d on host %s", diskID, host)

			latestVolume, e := h.getVolume(ctx, clusterID, vid, false)
			if e != nil {
				span.Warnf("update volume info with no cache %d %d err: %s", clusterID, vid, e)
				return false, err
			}
			newUnit := latestVolume.Units[index]

			newDiskID := newUnit.DiskID
			if newDiskID != diskID {
				hi, e := serviceController.GetDiskHost(ctx, newDiskID)
				if e == nil && !hi.Punished {
					span.Infof("update disk %d %d %d -> %d", clusterID, vid, diskID, newDiskID)

					host = hi.Host
					diskID = newDiskID
					args.GetShardArgs.DiskID = diskID
					args.GetShardArgs.Vuid = newUnit.Vuid
					return false, err
				}
			}

			h.punishDiskWith(ctx, clusterID, diskID, host, "NotFound")
			span.Warnf("punish threshold disk:%d cos:blobnode/%d", diskID, code)
		}

		// do not retry on timeout then punish threshold this disk
		if errorTimeout(err) {
			h.punishDiskWith(ctx, clusterID, diskID, host, "Timeout")
			return true, err
		}
		if errorConnectionRefused(err) {
			return true, err
		}
		span.Debugf("read from disk:%d blobnode/%s", diskID, err.Error())

		err = errors.Base(err, fmt.Sprintf("get shard on (disk:%d host:%s)", diskID, host))
		return false, err
	})

	return rbody, rerr
}

func genLocationBlobs(location *access.Location, readSize uint64, offset uint64) ([]blobGetArgs, error) {
	if readSize > location.Size || offset > location.Size || offset+readSize > location.Size {
		return nil, fmt.Errorf("FileSize:%d ReadSize:%d Offset:%d", location.Size, readSize, offset)
	}

	blobSize := uint64(location.BlobSize)
	if blobSize <= 0 {
		return nil, fmt.Errorf("BlobSize:%d", blobSize)
	}

	remainSize := readSize
	firstBlobIdx := offset / blobSize
	blobOffset := offset % blobSize

	tactic := location.CodeMode.Tactic()

	idx := uint64(0)
	blobs := make([]blobGetArgs, 0, 1+(readSize+blobOffset)/blobSize)
	for _, blob := range location.Blobs {
		currBlobID := blob.MinBid

		for ii := uint32(0); ii < blob.Count; ii++ {
			if remainSize <= 0 {
				return blobs, nil
			}

			if idx >= firstBlobIdx {
				toReadSize := minU64(remainSize, blobSize-blobOffset)
				if toReadSize > 0 {
					// update the last blob size
					fixedBlobSize := minU64(location.Size-idx*blobSize, blobSize)

					sizes, _ := ec.GetBufferSizes(int(fixedBlobSize), tactic)
					shardSize := sizes.ShardSize
					shardOffset, shardReadSize := shardSegment(shardSize, int(blobOffset), int(toReadSize))

					blobs = append(blobs, blobGetArgs{
						Cid:      location.ClusterID,
						Vid:      blob.Vid,
						Bid:      currBlobID,
						CodeMode: location.CodeMode,
						BlobSize: fixedBlobSize,
						Offset:   blobOffset,
						ReadSize: toReadSize,

						ShardSize:     shardSize,
						ShardOffset:   shardOffset,
						ShardReadSize: shardReadSize,
					})
				}

				// reset next blob offset
				blobOffset = 0
				remainSize -= toReadSize
			}

			currBlobID++
			idx++
		}
	}

	if remainSize > 0 {
		return nil, fmt.Errorf("no enough data to read %d", remainSize)
	}

	return blobs, nil
}

func genSortedVuidByIDC(ctx context.Context, serviceController controller.ServiceController, idc string,
	vuidPhys []controller.Unit) []sortedVuid {
	span := trace.SpanFromContextSafe(ctx)

	vuids := make([]sortedVuid, 0, len(vuidPhys))
	sortMap := make(map[int][]sortedVuid)

	for idx, phy := range vuidPhys {
		var hostIDC *controller.HostIDC
		if err := retry.ExponentialBackoff(2, 100).On(func() error {
			hi, e := serviceController.GetDiskHost(context.Background(), phy.DiskID)
			if e != nil {
				return e
			}
			hostIDC = hi
			return nil
		}); err != nil {
			span.Warnf("no host of disk(%d %d) %s", phy.Vuid, phy.DiskID, err.Error())
			continue
		}

		dis := distance(idc, hostIDC.IDC, hostIDC.Punished)
		if _, ok := sortMap[dis]; !ok {
			sortMap[dis] = make([]sortedVuid, 0, 8)
		}
		sortMap[dis] = append(sortMap[dis], sortedVuid{
			index:  idx,
			vuid:   phy.Vuid,
			diskID: phy.DiskID,
			host:   phy.Host,
		})
	}

	keys := make([]int, 0, len(sortMap))
	for dis := range sortMap {
		keys = append(keys, dis)
	}
	sort.Ints(keys)

	for _, dis := range keys {
		ids := sortMap[dis]
		rand.Shuffle(len(ids), func(i, j int) {
			ids[i], ids[j] = ids[j], ids[i]
		})
		vuids = append(vuids, ids...)
		if dis > 1 {
			span.Debugf("distance: %d punished vuids: %+v", dis, ids)
		}
	}

	return vuids
}

func distance(idc1, idc2 string, punished bool) int {
	if punished {
		if idc1 == idc2 {
			return 2
		}
		return 3
	}
	if idc1 == idc2 {
		return 0
	}
	return 1
}

func emptyDataShardIndexes(sizes ec.BufferSizes) map[int]struct{} {
	firstEmptyIdx := (sizes.DataSize + sizes.ShardSize - 1) / sizes.ShardSize
	n := sizes.ECDataSize / sizes.ShardSize
	if firstEmptyIdx >= n {
		return make(map[int]struct{})
	}

	set := make(map[int]struct{}, n-firstEmptyIdx)
	for i := firstEmptyIdx; i < n; i++ {
		set[i] = struct{}{}
	}

	return set
}

func shardSegment(shardSize, blobOffset, blobReadSize int) (shardOffset, shardReadSize int) {
	shardOffset = blobOffset % shardSize
	if lastOffset := shardOffset + blobReadSize; lastOffset > shardSize {
		shardOffset, shardReadSize = 0, shardSize
	} else {
		shardReadSize = blobReadSize
	}
	return
}
