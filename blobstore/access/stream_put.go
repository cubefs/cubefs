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
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/afex/hystrix-go/hystrix"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/ec"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

// TODO: To Be Continue
//  put empty shard to blobnode if file has been aligned.

// Put put one object
//     required: size, file size
//     optional: hasher map to calculate hash.Hash
func (h *Handler) Put(ctx context.Context, rc io.Reader, size int64,
	hasherMap access.HasherMap) (*access.Location, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("put request size:%d hashes:b(%b)", size, hasherMap.ToHashAlgorithm())

	if size <= 0 {
		return nil, errcode.ErrIllegalArguments
	}
	if size > h.maxObjectSize {
		span.Info("exceed max object size", h.maxObjectSize)
		return nil, errcode.ErrAccessExceedSize
	}

	// 1.make hasher
	if len(hasherMap) > 0 {
		rc = io.TeeReader(rc, hasherMap.ToWriter())
	}

	// 2.choose cluster and alloc volume from allocator
	selectedCodeMode := h.allCodeModes.SelectCodeMode(size)
	span.Debugf("select codemode %d", selectedCodeMode)

	blobSize := atomic.LoadUint32(&h.MaxBlobSize)
	clusterID, blobs, err := h.allocFromAllocatorWithHystrix(ctx, selectedCodeMode, uint64(size), blobSize, 0)
	if err != nil {
		span.Error("alloc failed", errors.Detail(err))
		return nil, err
	}
	span.Debugf("allocated from %d %+v", clusterID, blobs)

	// 3.read body and split, alloc from mem pool;ec encode and put into data node
	limitReader := io.LimitReader(rc, int64(size))
	location := &access.Location{
		ClusterID: clusterID,
		CodeMode:  selectedCodeMode,
		Size:      uint64(size),
		BlobSize:  blobSize,
		Blobs:     blobs,
	}

	uploadSucc := false
	defer func() {
		if !uploadSucc {
			span.Infof("put failed clean location %+v", location)
			if err := h.clearGarbage(ctx, location); err != nil {
				span.Warn(errors.Detail(err))
			}
		}
	}()

	var buffer *ec.Buffer
	putTime := new(timeReadWrite)
	defer func() {
		// release ec buffer which have not takeover
		buffer.Release()
		span.AppendRPCTrackLog([]string{putTime.String()})
	}()

	// concurrent buffer in per request
	const concurrence = 4
	ready := make(chan struct{}, concurrence)
	for range [concurrence]struct{}{} {
		ready <- struct{}{}
	}

	encoder := h.encoder[selectedCodeMode]
	tactic := selectedCodeMode.Tactic()
	for _, blob := range location.Spread() {
		vid, bid, bsize := blob.Vid, blob.Bid, int(blob.Size)

		// new an empty ec buffer for per blob
		var err error
		st := time.Now()
		buffer, err = ec.NewBuffer(bsize, tactic, h.memPool)
		putTime.IncA(time.Since(st))
		if err != nil {
			return nil, err
		}

		readBuff := buffer.DataBuf[:bsize]
		shards, err := encoder.Split(buffer.ECDataBuf)
		if err != nil {
			return nil, err
		}

		startRead := time.Now()
		n, err := io.ReadFull(limitReader, readBuff)
		putTime.IncR(time.Since(startRead))
		if err != nil && err != io.EOF {
			span.Infof("read blob data failed want:%d read:%d %s", bsize, n, err.Error())
			return nil, errcode.ErrAccessReadRequestBody
		}
		if n != bsize {
			span.Infof("read blob less data want:%d but:%d", bsize, n)
			return nil, errcode.ErrAccessReadRequestBody
		}

		// ec encode
		if err = encoder.Encode(shards); err != nil {
			return nil, err
		}

		blobident := blobIdent{clusterID, vid, bid}
		span.Debug("to write", blobident)

		// takeover the buffer, release to pool in function writeToBlobnodes
		takeoverBuffer := buffer
		buffer = nil
		<-ready
		startWrite := time.Now()
		err = h.writeToBlobnodesWithHystrix(ctx, blobident, shards, func() {
			takeoverBuffer.Release()
			ready <- struct{}{}
		})
		putTime.IncW(time.Since(startWrite))
		if err != nil {
			return nil, errors.Info(err, "write to blobnode failed")
		}
	}

	uploadSucc = true
	return location, nil
}

func (h *Handler) writeToBlobnodesWithHystrix(ctx context.Context,
	blob blobIdent, shards [][]byte, callback func()) error {
	safe := make(chan struct{}, 1)
	err := hystrix.Do(rwCommand, func() error {
		safe <- struct{}{}
		return h.writeToBlobnodes(ctx, blob, shards, callback)
	}, nil)

	select {
	case <-safe:
	default:
		callback() // callback if fused by hystrix
	}
	return err
}

type shardPutStatus struct {
	index  int
	status bool
}

// writeToBlobnodes write shards to blobnodes.
// takeover ec buffer release by callback.
// return if had quorum successful shards, then wait all shards in background.
func (h *Handler) writeToBlobnodes(ctx context.Context,
	blob blobIdent, shards [][]byte, callback func()) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	clusterID, vid, bid := blob.cid, blob.vid, blob.bid

	wg := &sync.WaitGroup{}
	defer func() {
		// waiting all shards done in background
		go func() {
			wg.Wait()
			callback()
		}()
	}()

	volume, err := h.getVolume(ctx, clusterID, vid, true)
	if err != nil {
		return
	}
	serviceController, err := h.clusterController.GetServiceController(clusterID)
	if err != nil {
		return
	}

	statusCh := make(chan shardPutStatus, len(volume.Units))
	tactic := volume.CodeMode.Tactic()
	putQuorum := uint32(tactic.PutQuorum)
	if num, ok := h.CodeModesPutQuorums[volume.CodeMode]; ok && num <= tactic.N+tactic.M {
		putQuorum = uint32(num)
	}

	// writtenNum ONLY apply on data and partiy shards
	// TODO: count N and M in each AZ,
	//    decision ec data is recoverable or not.
	maxWrittenIndex := tactic.N + tactic.M
	writtenNum := uint32(0)

	wg.Add(len(volume.Units))
	for i, unitI := range volume.Units {
		index, unit := i, unitI

		go func() {
			status := shardPutStatus{index: index}
			defer func() {
				statusCh <- status
				wg.Done()
			}()

			diskID := unit.DiskID
			args := &blobnode.PutShardArgs{
				DiskID: diskID,
				Vuid:   unit.Vuid,
				Bid:    bid,
				Size:   int64(len(shards[index])),
				Type:   blobnode.NormalIO,
			}

			crcDisabled := h.ShardCrcDisabled
			var crcOrigin uint32
			if !crcDisabled {
				crcOrigin = crc32.ChecksumIEEE(shards[index])
			}

			// new child span to write to blobnode, we should finish it here.
			spanChild, ctxChild := trace.StartSpanFromContextWithTraceID(
				context.Background(), "WriteToBlobnode", span.TraceID())
			defer spanChild.Finish()

		RETRY:
			hostInfo, err := serviceController.GetDiskHost(ctxChild, diskID)
			if err != nil {
				span.Error("get disk host failed", errors.Detail(err))
				return
			}
			// punished disk, ignore and return
			if hostInfo.Punished {
				span.Infof("ignore punished disk(%d %s) uvid(%d) ecidx(%02d) in idc(%s)",
					diskID, hostInfo.Host, unit.Vuid, index, hostInfo.IDC)
				return
			}
			host := hostInfo.Host

			var (
				writeErr  error
				needRetry bool
				crc       uint32
			)
			writeErr = retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
				args.Body = bytes.NewReader(shards[index])

				crc, err = h.blobnodeClient.PutShard(ctxChild, host, args)
				if err == nil {
					if !crcDisabled && crc != crcOrigin {
						return false, fmt.Errorf("crc mismatch 0x%x != 0x%x", crc, crcOrigin)
					}

					needRetry = false
					return true, nil
				}

				code := rpc.DetectStatusCode(err)
				switch code {
				case errcode.CodeDiskBroken, errcode.CodeDiskNotFound,
					errcode.CodeChunkNoSpace, errcode.CodeVUIDReadonly:
					h.discardVidChan <- discardVid{
						cid:      clusterID,
						codeMode: volume.CodeMode,
						vid:      vid,
					}
				}

				switch code {
				// EIO and Readonly error, then we need to punish disk in local and no necessary to retry
				case errcode.CodeDiskBroken, errcode.CodeVUIDReadonly:
					h.punishVolume(ctx, clusterID, vid, host, "BrokenOrRO")
					h.punishDisk(ctx, clusterID, diskID, host, "BrokenOrRO")
					span.Infof("punish disk:%d volume:%d cos:blobnode/%d", diskID, vid, code)
					return true, err

				// chunk no space, we should punish this volume
				case errcode.CodeChunkNoSpace:
					h.punishVolume(ctx, clusterID, vid, host, "NoSpace")
					span.Infof("punish volume:%d cos:blobnode/%d", vid, code)
					return true, err

				// vuid not found means the reflection between vuid and diskID has change, we should refresh the volume
				// disk not found means disk has been repaired or offline
				case errcode.CodeDiskNotFound, errcode.CodeVuidNotFound:
					latestVolume, e := h.getVolume(ctx, clusterID, vid, false)
					if e != nil {
						return true, errors.Base(err, "get volume with no cache failed").Detail(e)
					}

					newUnit := latestVolume.Units[index]
					if diskID != newUnit.DiskID {
						diskID = newUnit.DiskID
						unit = newUnit
						args.DiskID = newUnit.DiskID
						args.Vuid = newUnit.Vuid

						needRetry = true
						return true, err
					}

					h.punishVolume(ctx, clusterID, vid, host, "NotFound")
					h.punishDisk(ctx, clusterID, diskID, host, "NotFound")
					span.Infof("punish disk:%d volume:%d cos:blobnode/%d", diskID, vid, code)
					return true, err
				}

				// in timeout case and writtenNum is not satisfied with putQuorum, then should retry
				if errorTimeout(err) && atomic.LoadUint32(&writtenNum) < putQuorum {
					h.punishDiskWith(ctx, clusterID, diskID, host, "Timeout")
					span.Info("connect timeout, need to punish threshold disk", diskID, host)
					return false, err
				}

				// others, do not retry this round
				return true, err
			})

			if needRetry {
				goto RETRY
			}
			if writeErr != nil {
				span.Warnf("write %s on blobnode(vuid:%d disk:%d host:%s) ecidx(%02d): %s",
					blob.String(), args.Vuid, args.DiskID, hostInfo.Host, index, errors.Detail(writeErr))
				return
			}

			if index < maxWrittenIndex {
				atomic.AddUint32(&writtenNum, 1)
			}
			status.status = true
		}()
	}

	received := make(map[int]shardPutStatus, len(volume.Units))
	for len(received) < len(volume.Units) && atomic.LoadUint32(&writtenNum) < putQuorum {
		st := <-statusCh
		received[st.index] = st
	}

	writeDone := make(chan struct{}, 1)
	// write unaccomplished shard to repair queue
	go func(writeDone <-chan struct{}) {
		for len(received) < len(volume.Units) {
			st := <-statusCh
			received[st.index] = st
		}

		if _, ok := <-writeDone; !ok {
			return
		}

		badIdxes := make([]uint8, 0)
		for idx := range volume.Units {
			if st, ok := received[idx]; ok && st.status {
				continue
			}
			badIdxes = append(badIdxes, uint8(idx))
		}
		if len(badIdxes) > 0 {
			h.sendRepairMsgBg(ctx, blob, badIdxes)
		}
	}(writeDone)

	// return if had quorum successful shards
	if atomic.LoadUint32(&writtenNum) >= putQuorum {
		writeDone <- struct{}{}
		return
	}

	// It tolerate one az was down when we have 3 or more azs.
	// But MUST make sure others azs data is all completed,
	// And all data in the down az are failed.
	if tactic.AZCount >= 3 {
		allFine := 0
		allDown := 0

		for _, azIndexes := range tactic.GetECLayoutByAZ() {
			azFine := true
			azDown := true
			for _, idx := range azIndexes {
				if st, ok := received[idx]; !ok || !st.status {
					azFine = false
				} else {
					azDown = false
				}
			}
			if azFine {
				allFine++
			}
			if azDown {
				allDown++
			}
		}

		span.Debugf("tolerate-multi-az-write (az-fine:%d az-down:%d az-all:%d)", allFine, allDown, tactic.AZCount)
		if allFine == tactic.AZCount-1 && allDown == 1 {
			span.Warnf("tolerate-multi-az-write (az-fine:%d az-down:%d az-all:%d) of %s",
				allFine, allDown, tactic.AZCount, blob.String())
			writeDone <- struct{}{}
			return
		}
	}

	close(writeDone)
	err = fmt.Errorf("quorum write failed (%d < %d) of %s", writtenNum, putQuorum, blob.String())
	return
}
