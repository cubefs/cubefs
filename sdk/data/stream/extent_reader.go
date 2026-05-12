// Copyright 2018 The CubeFS Authors.
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

package stream

import (
	"fmt"
	"hash/crc32"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

// ExtentReader defines the struct of the extent reader.
type ExtentReader struct {
	inode        uint64
	key          *proto.ExtentKey
	dp           *wrapper.DataPartition
	followerRead bool
	retryRead    bool

	maxRetryTimeout time.Duration
}

// NewExtentReader returns a new extent reader.
func NewExtentReader(inode uint64, key *proto.ExtentKey, dp *wrapper.DataPartition, followerRead bool, retryRead bool) *ExtentReader {
	return &ExtentReader{
		inode:        inode,
		key:          key,
		dp:           dp,
		followerRead: followerRead,
		retryRead:    retryRead,
	}
}

// String returns the string format of the extent reader.
func (reader *ExtentReader) String() (m string) {
	return fmt.Sprintf("inode (%v) extentKey(%v)", reader.inode,
		reader.key.Marshal())
}

// Read reads the extent request.
func (reader *ExtentReader) Read(req *ExtentRequest) (readBytes int, err error) {
	offset := req.FileOffset - int(reader.key.FileOffset) + int(reader.key.ExtentOffset)
	size := req.Size

	reqPacket := NewReadPacket(reader.key, offset, size, reader.inode, req.FileOffset, reader.followerRead)
	sc := NewStreamConn(reader.dp, reader.followerRead, reader.maxRetryTimeout)

	log.LogDebugf("ExtentReader Read enter: size(%v) req(%v) reqPacket(%v)", size, req, reqPacket)

	// P4b: try RDMA per-chunk before falling back to the TCP streaming
	// path. RDMA only fires the leader (or the configured follower) for
	// this attempt; on any failure we fall through to the TCP path
	// below, which has its own host-iteration retry. Skipped if the
	// total read is below the P6 small-payload threshold so the SDK
	// doesn't pay round-trip overhead on tiny reads.
	//
	// The size passed to rdmaTryForSize is clamped to ReadBlockSize
	// because readViaRDMA internally chunks the read into
	// ReadBlockSize-sized RDMA round-trips — each one fits one slot.
	// Without this clamp, any read > slot capacity (e.g. an S3 1 MB
	// GET that flows through here as size=1048576) would fail the
	// gate's max_payload check and fall back to TCP, even though every
	// actual per-chunk RDMA call would fit fine.
	chunkSize := size
	if chunkSize > util.ReadBlockSize {
		chunkSize = util.ReadBlockSize
	}
	// Use the same target host that NewStreamConn just picked for the
	// TCP fallback below: when followerRead=false this is dp.LeaderAddr,
	// when followerRead=true it's the nearest / epoch-selected follower.
	// Previously this branch hard-coded dp.Hosts[0], which after a leader
	// election would route reads to a still-replicating follower and trip
	// the server's checkReadOffsetAndSize (offset > e.Size → 244
	// OpArgMismatchErr). Mismatched routing was invisible while CRC
	// errors masked everything; once the CRC bug was fixed the lagging
	// reads surfaced as ~6% read failures on 4 MB s3bench.
	rdmaAddr := sc.CurrAddr()
	if rdmaAddr != "" && rdmaTryForSize(rdmaAddr, chunkSize) {
		// Phase A fast path: one-sided RDMA Read against the
		// DataNode's pre-registered extent MR. Zero server CPU on
		// the data path — the NIC pulls bytes directly. Skipped
		// when the cache isn't initialised (no Phase A wiring) or
		// when a probe call returns nil + error (cache miss with
		// lookup failure, deregistered MR, etc.) — caller drops
		// through to the two-sided readViaRDMA below.
		if n, rerr := reader.tryReadViaRDMARead(rdmaAddr, reqPacket, req, offset, size); rerr == nil && n > 0 {
			readBytes = n
			return
		} else if rerr != nil {
			// Invalidate the cache entry so the next read does a
			// fresh lookup. A logged warn only — the two-sided
			// path below still gets a chance.
			log.LogDebugf("ExtentReader Read: one-sided RDMA failed, trying two-sided: addr(%v) req(%v) err(%v)",
				rdmaAddr, reqPacket, rerr)
			invalidateExtentMRCache(rdmaAddr, reader.dp.PartitionID, reqPacket.ExtentID)
		}

		if n, rerr := reader.readViaRDMA(rdmaAddr, reqPacket, req, offset, size); rerr == nil {
			readBytes = n
			return
		} else {
			log.LogWarnf("ExtentReader Read: RDMA failed addr(%v) req(%v) err(%v), falling back to TCP",
				rdmaAddr, reqPacket, rerr)
		}
	}

	err = sc.Send(&reader.retryRead, reqPacket, func(conn *net.TCPConn) (error, bool) {
		bgTime := stat.BeginStat()
		defer func() {
			addr := conn.RemoteAddr().String()
			parts := strings.Split(addr, ":")
			if len(parts) > 0 {
				stat.EndStat(fmt.Sprintf("dataNode:%v", parts[0]), err, bgTime, 1)
			}
			stat.EndStat("dataNode", err, bgTime, 1)
		}()
		readBytes = 0
		for readBytes < size {
			replyPacket := NewReply(reqPacket.ReqID, reader.dp.PartitionID, reqPacket.ExtentID)
			bufSize := util.Min(util.ReadBlockSize, size-readBytes)
			replyPacket.Data = req.Data[readBytes : readBytes+bufSize]
			e := replyPacket.readFromConn(conn, proto.ReadDeadlineTime)

			if e != nil {
				if sc.dp.ClientWrapper.FollowerRead() && sc.dp.ClientWrapper.NearRead() && sc.dp.MediaType == proto.MediaType_HDD && strings.Contains(e.Error(), "timeout") {
					sc.dp.ClientWrapper.AddReadFailedHosts(sc.dp.PartitionID, conn.RemoteAddr().String())
				}
				log.LogWarnf("Extent Reader Read: failed to read from connect, ino(%v) req(%v) readBytes(%v) err(%v)", reader.inode, reqPacket, readBytes, e)
				// Upon receiving TryOtherAddrError, other hosts will be retried.
				return TryOtherAddrError, false
			}

			if replyPacket.ResultCode == proto.OpAgain {
				return nil, true
			}

			if replyPacket.ResultCode == proto.OpLimitedIoErr {
				// NOTE: use special errors to retry
				return LimitedIoError, true
			}

			e = reader.checkStreamReply(reqPacket, replyPacket)
			if e != nil {
				log.LogWarnf("checkStreamReply failed:(%v) reply msg:(%v)", e, replyPacket.GetResultMsg())
				// Dont change the error message, since the caller will
				// check if it is NotLeaderErr.
				return e, false
			}

			readBytes += int(replyPacket.Size)
		}
		return nil, false
	})

	if err != nil {
		// if cold vol and cach is invaild
		if !reader.retryRead && (err == TryOtherAddrError || strings.Contains(err.Error(), "ExistErr")) {
			log.LogWarnf("Extent Reader Read: err(%v) req(%v) reqPacket(%v)", err, req, reqPacket)
		} else {
			log.LogErrorf("Extent Reader Read: err(%v) req(%v) reqPacket(%v)", err, req, reqPacket)
		}
	}

	log.LogDebugf("ExtentReader Read exit: req(%v) reqPacket(%v) readBytes(%v) err(%v)", req, reqPacket, readBytes, err)
	return
}

// readPrefetchDepth caps how many ReadBlockSize-sized RDMA round-trips
// run concurrently inside a single readViaRDMA call. Higher = better
// latency hiding for large reads, but each in-flight chunk consumes
// one slot pool entry and one ReadBlockSize buffer. 4 is a balanced
// default given the typical pool config (numSlots=256 / maxConns=4).
//
// Now a package var (not const) so InitRDMAConnPool can override from
// cfg.ReadPrefetchDepth at startup. Higher values are typically
// useful when Phase A (one-sided RDMA Read) is hitting — the chunks
// are larger (cfg.ReadSlotSize, typically 4 MiB) and a 4-deep
// prefetch under-uses NIC bandwidth. Operators can crank this via
// mount option rdmaReadPrefetchDepth.
var readPrefetchDepth = 4

// readChunkSpec describes one ReadBlockSize-aligned subrange of an
// outer ExtentReader.Read request. Pure data — chunk splitting logic
// is in splitReadChunks() so it can be unit-tested without RDMA.
type readChunkSpec struct {
	extentOff int // offset within the server-side extent
	bufOff    int // destination offset within req.Data
	bufSize   int // bytes to fetch (== copy length)
}

// splitReadChunks divides a [extentOff, extentOff+size) read into
// ReadBlockSize-aligned pieces. The first and last chunks may be
// smaller. The returned slice is in increasing-offset order.
func splitReadChunks(extentOff, size, blockSize int) []readChunkSpec {
	if size <= 0 || blockSize <= 0 {
		return nil
	}
	n := (size + blockSize - 1) / blockSize
	chunks := make([]readChunkSpec, 0, n)
	for off := 0; off < size; off += blockSize {
		bufSize := blockSize
		if off+bufSize > size {
			bufSize = size - off
		}
		chunks = append(chunks, readChunkSpec{
			extentOff: extentOff + off,
			bufOff:    off,
			bufSize:   bufSize,
		})
	}
	return chunks
}

// readViaRDMA tries to satisfy req entirely over RDMA, chunked at
// util.ReadBlockSize. Returns the number of bytes filled into req.Data
// (== size on success) and an error if any chunk failed; the caller
// falls back to the TCP path, which restarts the read from the
// beginning. We do NOT support partial RDMA + TCP completion because
// the TCP path's getReply expects a single ReqID per StreamConn session
// and re-issuing only the failed tail under a new ReqID would race with
// the server's replication semantics.
//
// Chunks dispatch up to readPrefetchDepth in parallel, each on its own
// slot — the SDK pool's read-side empty-key path (see rdma_client.go
// rdmaRoundTrip) round-robins reads across available slots, so the
// chunks don't serialise behind one slot. Failures cancel only the
// completion-collection side; in-flight chunks that finish after the
// first error are still drained to release their slots cleanly.
func (reader *ExtentReader) readViaRDMA(addr string, reqPacket *Packet, req *ExtentRequest, offset, size int) (int, error) {
	chunks := splitReadChunks(offset, size, util.ReadBlockSize)
	if len(chunks) == 0 {
		return 0, nil
	}
	if len(chunks) == 1 {
		// Single-chunk fast path: avoids goroutine + channel overhead
		// for the common small-read case.
		return reader.readChunkViaRDMA(addr, reqPacket, req, chunks[0])
	}
	return reader.readChunksParallel(addr, reqPacket, req, chunks)
}

// readChunkViaRDMA performs one ReadBlockSize-sized RDMA round-trip
// and copies the response data into the caller-provided req.Data
// region. Returns the number of bytes successfully copied (== bufSize
// on success, 0 on failure).
func (reader *ExtentReader) readChunkViaRDMA(addr string, reqPacket *Packet, req *ExtentRequest, chk readChunkSpec) (int, error) {
	chunkReq := NewReadPacket(reader.key, chk.extentOff, chk.bufSize,
		reader.inode, req.FileOffset+chk.bufOff, reader.followerRead)
	// Inherit ReqID from the outer reqPacket so server-side audit logs
	// correlate; chunk index is implicit in offset.
	chunkReq.ReqID = reqPacket.ReqID

	resp, rerr := recvPacketViaRDMA(addr, chunkReq)
	if rerr != nil {
		return 0, rerr
	}
	if int(resp.Size) != chk.bufSize {
		return 0, fmt.Errorf("rdma read: chunk size %d != requested %d", resp.Size, chk.bufSize)
	}
	copy(req.Data[chk.bufOff:chk.bufOff+chk.bufSize], resp.Data[:resp.Size])
	return chk.bufSize, nil
}

// readChunksParallel dispatches up to readPrefetchDepth chunks
// concurrently, blocking the caller until every chunk has completed
// or any has failed. On success returns size bytes copied; on failure
// returns the first error and 0 (partial reads aren't returned —
// the outer Read() falls back to TCP which restarts from offset 0).
func (reader *ExtentReader) readChunksParallel(addr string, reqPacket *Packet, req *ExtentRequest, chunks []readChunkSpec) (int, error) {
	sem := make(chan struct{}, readPrefetchDepth)
	errCh := make(chan error, len(chunks))
	var wg sync.WaitGroup

	for i := range chunks {
		chk := chunks[i]
		wg.Add(1)
		sem <- struct{}{} // back-pressure: at most prefetchDepth in flight
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := reader.readChunkViaRDMA(addr, reqPacket, req, chk); err != nil {
				// Buffered to len(chunks) so this send is wait-free
				// regardless of how many chunks fail.
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)

	// First error wins. Drain the rest so logs aren't silent on
	// follow-up failures (useful when triaging cascades).
	var firstErr error
	for err := range errCh {
		if firstErr == nil {
			firstErr = err
		} else if log.EnableDebug() {
			log.LogDebugf("readChunksParallel: additional chunk error after first: %v", err)
		}
	}
	if firstErr != nil {
		return 0, firstErr
	}

	totalSize := 0
	for _, chk := range chunks {
		totalSize += chk.bufSize
	}
	return totalSize, nil
}

func (reader *ExtentReader) checkStreamReply(request *Packet, reply *Packet) (err error) {
	if reply.ResultCode == proto.OpTryOtherAddr {
		return TryOtherAddrError
	}

	// if follower read is enabled, try other hosts when triggering OpNotExistErr
	// if reply.ResultCode == proto.OpNotExistErr {
	// 	return ExtentNotFoundError
	// }

	if reply.ResultCode != proto.OpOk {
		if request.Opcode == proto.OpStreamFollowerRead && reply.ResultCode != proto.OpForbidErr {
			log.LogWarnf("checkStreamReply: ResultCode(%v) NOK, OpStreamFollowerRead return TryOtherAddrError, "+
				"req(%v) reply(%v)", reply.GetResultMsg(), request, reply)
			return TryOtherAddrError
		}
		err = errors.New(fmt.Sprintf("checkStreamReply: ResultCode(%v) NOK", reply.GetResultMsg()))
		return
	}
	if !request.isValidReadReply(reply) {
		err = errors.New(fmt.Sprintf("checkStreamReply: inconsistent req and reply, req(%v) reply(%v)", request, reply))
		return
	}
	expectCrc := crc32.ChecksumIEEE(reply.Data[:reply.Size])
	if reply.CRC != expectCrc {
		err = errors.New(fmt.Sprintf("checkStreamReply: inconsistent CRC, expectCRC(%v) replyCRC(%v), relpy(%v)", expectCrc, reply.CRC, reply.GetNoPrefixMsg()))
		return
	}
	return nil
}
