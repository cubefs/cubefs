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

package blobnode

import (
	"context"
	"errors"
	"hash/crc32"
	"io"
	"math/rand"
	"sync"
	"unsafe"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/workutils"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

var (
	errShardDataNotPrepared = errors.New("shard data not prepared")
	errBufHasData           = errors.New("buf already has data")
	errBidNotFoundInBuf     = errors.New("bid not found in buffer")
	errIllegalBuf           = errors.New("illegal buffer")
	errBidCanNotRecover     = errors.New("bid can not recover")
	errCrcNotMatch          = errors.New("data conflict crc32 not match")
	errUnexpectedLength     = errors.New("length of replicas is unexpected")
	errEcVerifyFailed       = errors.New("ec verify failed")
	errShardSizeNotMatch    = errors.New("shard data size not match")
	errBufNotEnough         = errors.New("buf space not enough")
	errInvalidReplicas      = errors.New("invalid volume replicas")
)

const defaultGetConcurrency = 100

type (
	// N data block count
	N int
	// M parity data block count
	M            int
	downloadPlan struct {
		downloadReplicas []proto.VunitLocation
	}
)

// Vunits volume stripe locations.
type Vunits []proto.VunitLocation

func (locs Vunits) IsValid() bool {
	if !proto.CheckVunitLocations(locs) {
		return false
	}
	for idx, loc := range locs {
		if uint8(idx) != loc.Vuid.Index() {
			return false
		}
	}
	return true
}

func (locs Vunits) Indexes() []uint8 {
	idxes := make([]uint8, len(locs))
	for idx, loc := range locs {
		idxes[idx] = loc.Vuid.Index()
	}
	return idxes
}

func (locs Vunits) Subset(idxes []int) Vunits {
	sub := make(Vunits, 0, len(idxes))
	for _, idx := range idxes {
		sub = append(sub, locs[idx])
	}
	return sub
}

func (locs Vunits) IntactGlobalSet(mode codemode.CodeMode, bad []uint8) Vunits {
	ex := make(map[int]struct{}, len(bad))
	for _, idx := range bad {
		ex[int(idx)] = struct{}{}
	}

	var idxes []int
	globalStripe, _, _ := mode.T().GlobalStripe()
	for _, idx := range globalStripe {
		if _, ok := ex[idx]; ok {
			continue
		}
		idxes = append(idxes, idx)
	}

	return locs.Subset(idxes)
}

type repairStripe struct {
	replicas Vunits
	n        N
	m        M
	badIdxes []uint8
}

func (stripe *repairStripe) genDownloadPlans() []downloadPlan {
	badi := stripe.badIdxes
	n := stripe.n
	var downloadPlans []downloadPlan
	var wellReplications []proto.VunitLocation

	stripeReplicas := make([]proto.VunitLocation, len(stripe.replicas))
	copy(stripeReplicas, stripe.replicas)
	rand.Shuffle(len(stripeReplicas), func(i, j int) {
		stripeReplicas[i], stripeReplicas[j] = stripeReplicas[j], stripeReplicas[i]
	})

	badMap := make(map[uint8]struct{})
	for _, bad := range badi {
		badMap[bad] = struct{}{}
	}

	for _, replica := range stripeReplicas {
		replicaIdx := replica.Vuid.Index()
		if _, ok := badMap[replicaIdx]; ok {
			continue
		}
		wellReplications = append(wellReplications, replica)
	}

	planCnt := len(wellReplications) - int(n) + 1
	for i := 0; i < planCnt; i++ {
		plan := downloadPlan{
			downloadReplicas: make([]proto.VunitLocation, n),
		}
		copy(plan.downloadReplicas, wellReplications[0:n-1]) // n-1 replicas
		plan.downloadReplicas[n-1] = wellReplications[int(n-1)+i]
		downloadPlans = append(downloadPlans, plan)
	}

	return downloadPlans
}

// duties：repair shard data
// if get shard data directly fail,
// for global stripe chunks(N+M) will do next step
//   step1:repair use local stripe ,if success return
//   step2 repair use global stripe
// for local stripe chunks(L) will do next step
//   step1:repair use local stripe ,if success return
//   step2:repair other global chunks in same az use global stripe
//   step3:repair use local stripe

// data layout view：
// bid1：shard11 shard12 shard13
// bid2：shard21 shard22 shard23
// bid2：shard31 shard32 shard33
// ShardsBuf:record of download shards data from same chunk({shard11，shard21，shard31})
// ShardRecover.chunksShardsBuf:is a list of chunksShardsBuf, every ele in list is record a chunk shard data,
// the order of chunks in list is keep some with volume replicas

// usage：
// first call RecoverShards to repair shard
// then call GetShard to get assign shard data

type shard struct {
	data []byte
	size int64
	ok   bool
}

// ShardsBuf used to store shard data in memory
type ShardsBuf struct {
	mu     sync.Mutex
	buf    []byte
	shards map[proto.BlobID]*shard
}

// NewShardsBuf returns shards buffer
func NewShardsBuf(buf []byte) *ShardsBuf {
	return &ShardsBuf{
		buf:    buf,
		shards: make(map[proto.BlobID]*shard),
	}
}

// PlanningDataLayout planning data layout
func (shards *ShardsBuf) PlanningDataLayout(bids []*ShardInfoSimple) error {
	shards.mu.Lock()
	defer shards.mu.Unlock()

	var totalSize int64 = 0
	for _, bid := range bids {
		totalSize += bid.Size
	}
	if totalSize > int64(len(shards.buf)) {
		return errBufNotEnough
	}

	var offset int64 = 0
	for _, bid := range bids {
		b := shard{
			data: shards.buf[offset : offset+bid.Size],
			size: bid.Size,
			ok:   false,
		}
		if bid.Size == 0 {
			b.ok = true
		}
		shards.shards[bid.Bid] = &b
		offset += bid.Size
	}
	return nil
}

func (shards *ShardsBuf) getShardBuf(bid proto.BlobID) ([]byte, error) {
	shards.mu.Lock()
	defer shards.mu.Unlock()
	if _, ok := shards.shards[bid]; !ok {
		return nil, errBidNotFoundInBuf
	}
	if shards.shards[bid].ok {
		return shards.shards[bid].data, nil
	}
	retBuf := shards.shards[bid].data[0:0]
	return retBuf, nil
}

func (shards *ShardsBuf) setShardBuf(ctx context.Context, bid proto.BlobID, buf []byte) error {
	shards.mu.Lock()
	defer shards.mu.Unlock()

	span := trace.SpanFromContextSafe(ctx)

	if _, ok := shards.shards[bid]; !ok {
		return errBidNotFoundInBuf
	}
	if shards.shards[bid].ok {
		return errBufHasData
	}
	if shards.shards[bid].size == 0 {
		shards.shards[bid].ok = true
		return nil
	}

	ptr1 := unsafe.Pointer(&shards.shards[bid].data[0])
	ptr2 := unsafe.Pointer(&buf[0])
	if ptr1 == ptr2 && len(buf) == int(shards.shards[bid].size) {
		shards.shards[bid].data = buf
		shards.shards[bid].ok = true
		return nil
	}

	span.Errorf("set shard buf failed: expect point[%p], expect size[%d], actual point[%p], actual size[%d]",
		shards.shards[bid].data, shards.shards[bid].size,
		buf, len(buf))
	return errIllegalBuf
}

// ShardSizeIsZero return true if shard size is zero
func (shards *ShardsBuf) ShardSizeIsZero(bid proto.BlobID) bool {
	shards.mu.Lock()
	defer shards.mu.Unlock()
	return shards.shards[bid].size == 0
}

// FetchShard returns shard data
func (shards *ShardsBuf) FetchShard(bid proto.BlobID) ([]byte, error) {
	shards.mu.Lock()
	defer shards.mu.Unlock()
	if _, ok := shards.shards[bid]; !ok {
		return nil, errBidNotFoundInBuf
	}
	if shards.shards[bid].size == 0 {
		return shards.shards[bid].data, nil
	}
	if !shards.shards[bid].ok {
		return nil, errShardDataNotPrepared
	}

	return shards.shards[bid].data, nil
}

// PutShard put shard data to shardsBuf
func (shards *ShardsBuf) PutShard(bid proto.BlobID, input io.Reader) error {
	shards.mu.Lock()

	if _, ok := shards.shards[bid]; !ok {
		shards.mu.Unlock()
		return errBidNotFoundInBuf
	}
	if shards.shards[bid].size == 0 {
		shards.mu.Unlock()
		return nil
	}
	if shards.shards[bid].ok {
		shards.mu.Unlock()
		return errBufHasData
	}

	size := shards.shards[bid].size
	if int64(len(shards.shards[bid].data)) != size {
		return errShardSizeNotMatch
	}
	shards.mu.Unlock()

	// read data from remote is slow,so optimize use of lock
	_, err := io.ReadFull(input, shards.shards[bid].data)
	if err != nil {
		return err
	}

	shards.mu.Lock()
	shards.shards[bid].ok = true
	shards.mu.Unlock()
	return nil
}

func (shards *ShardsBuf) shardIsOk(bid proto.BlobID) bool {
	shards.mu.Lock()
	defer shards.mu.Unlock()
	if _, exist := shards.shards[bid]; exist {
		return shards.shards[bid].ok
	}
	return false
}

// ShardCrc32 returns shard crc32
func (shards *ShardsBuf) ShardCrc32(bid proto.BlobID) (crc uint32, err error) {
	buf, err := shards.FetchShard(bid)
	if err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(buf), nil
}

type downloadStatus struct {
	mu                sync.Mutex
	downloadedMap     map[proto.Vuid]struct{}
	downloadForbidden map[proto.Vuid]struct{}
}

func newDownloadStatus() *downloadStatus {
	return &downloadStatus{
		downloadedMap:     make(map[proto.Vuid]struct{}),
		downloadForbidden: make(map[proto.Vuid]struct{}),
	}
}

func (d *downloadStatus) needDownload(vuid proto.Vuid) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.downloadForbidden[vuid]; ok {
		return false
	}

	if _, ok := d.downloadedMap[vuid]; ok {
		return false
	}
	return true
}

func (d *downloadStatus) forbiddenDownload(vuid proto.Vuid) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.downloadForbidden[vuid] = struct{}{}
}

func (d *downloadStatus) downloaded(vuid proto.Vuid) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.downloadedMap[vuid] = struct{}{}
}

// ShardRecover used to recover shard data
type ShardRecover struct {
	chunksShardsBuf []*ShardsBuf // record batch download shard data

	replicas           Vunits // stripe replicas list
	codeMode           codemode.CodeMode
	repairBidsReadOnly []*ShardInfoSimple // Strictly not allow modification

	shardGetter              client.IBlobNode
	vunitShardGetConcurrency int
	ioType                   blobnode.IOType
	taskType                 proto.TaskType
	ds                       *downloadStatus
}

// NewShardRecover returns shard recover
func NewShardRecover(replicas Vunits, mode codemode.CodeMode, bidInfos []*ShardInfoSimple,
	shardGetter client.IBlobNode, vunitShardGetConcurrency int, taskType proto.TaskType,
) *ShardRecover {
	if vunitShardGetConcurrency <= 0 {
		vunitShardGetConcurrency = defaultGetConcurrency
	}
	ioType := blobnode.Task2IOType(taskType)

	repair := ShardRecover{
		replicas:                 replicas,
		chunksShardsBuf:          make([]*ShardsBuf, len(replicas)),
		codeMode:                 mode,
		repairBidsReadOnly:       bidInfos,
		shardGetter:              shardGetter,
		ioType:                   ioType,
		taskType:                 taskType,
		vunitShardGetConcurrency: vunitShardGetConcurrency,
		ds:                       newDownloadStatus(),
	}
	return &repair
}

// RecoverShards recover shards
func (r *ShardRecover) RecoverShards(ctx context.Context, repairIdxs []uint8, direct bool) error {
	span := trace.SpanFromContextSafe(ctx)
	if !r.replicas.IsValid() {
		return errInvalidReplicas
	}

	// direct download shard
	repairBids := GetBids(r.repairBidsReadOnly)
	var allocBufErr error
	if direct {
		span.Debugf("recover shards by direct: bids len[%d]", len(repairBids))
		repairBids, allocBufErr = r.directGetShard(ctx, repairBids, repairIdxs)
		if allocBufErr != nil {
			return allocBufErr
		}
		if len(repairBids) == 0 {
			return nil
		}
		span.Debugf("need recover shards by ec: bids len[%d]", len(repairBids))
	}
	// end

	for _, idx := range repairIdxs {
		repairVuid := r.replicas[idx].Vuid
		r.ds.forbiddenDownload(repairVuid)
	}

	//what:split global chunk data and local chunk data repair
	//why:two ways of repair is difference
	var globalRepairIdxs, localRepairIdxs []uint8
	for _, repairIdx := range repairIdxs {
		if workutils.IsLocalStripeIndex(r.codeMode, int(repairIdx)) {
			localRepairIdxs = append(localRepairIdxs, repairIdx)
		} else {
			globalRepairIdxs = append(globalRepairIdxs, repairIdx)
		}
	}
	span.Infof("start recover shards: localRepairIdxs[%+v], globalRepairIdxs[%+v], len repairBidInfos[%d]",
		localRepairIdxs, globalRepairIdxs, len(r.repairBidsReadOnly))

	if len(globalRepairIdxs) != 0 {
		span.Infof("start recoverGlobalReplicaShards")
		err := r.recoverGlobalReplicaShards(ctx, globalRepairIdxs, repairBids)
		if err != nil {
			span.Errorf("end recoverGlobalReplicaShards failed: err[%+v]", err)
			return err
		}
	}

	if len(localRepairIdxs) != 0 {
		span.Infof("start recoverLocalReplicaShards")
		err := r.recoverLocalReplicaShards(ctx, localRepairIdxs, repairBids)
		if err != nil {
			span.Errorf("end recoverLocalReplicaShards failed: err[%+v]", err)
			return err
		}
	}
	span.Infof("end recover shards success")
	return nil
}

func (r *ShardRecover) recoverGlobalReplicaShards(ctx context.Context, repairIdxs []uint8, repairBids []proto.BlobID) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start recover global shards: repairIdxs[%+v], len(repairBids)[%d]", repairIdxs, len(repairBids))

	failBids := repairBids
	var err error

	span.Infof("step1: recover by local stripe")
	err = r.recoverByLocalStripe(ctx, failBids, repairIdxs)
	if err != nil {
		return err
	}

	failBids = r.collectFailBids(failBids, repairIdxs)
	if len(failBids) == 0 {
		return nil
	}

	span.Infof("step2: recover by local stripe fail need recover by global stripe")
	err = r.recoverByGlobalStripe(ctx, failBids, repairIdxs)
	if err != nil {
		return err
	}

	failBids = r.collectFailBids(failBids, repairIdxs)
	if len(failBids) != 0 {
		span.Errorf("recoverGlobalReplicaShards failed: failBids len[%d]", len(failBids))
		return errBidCanNotRecover
	}
	return nil
}

func (r *ShardRecover) recoverLocalReplicaShards(ctx context.Context, repairIdxs []uint8, repairBids []proto.BlobID) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start recover local vunit shards: repairIdxs[%+v], len(repairBids)[%d]", repairIdxs, len(repairBids))

	failBids := repairBids
	var allocBufErr error

	span.Infof("step1: recover by local stripe")
	allocBufErr = r.recoverByLocalStripe(ctx, failBids, repairIdxs)
	if allocBufErr != nil {
		return allocBufErr
	}

	failBids = r.collectFailBids(failBids, repairIdxs)
	if len(failBids) == 0 {
		return nil
	}

	globalRepairIdxs := r.collectGlobalBadReplicas(ctx, failBids, repairIdxs)
	span.Infof("step2: recover by local stripe fail need recover other global repl by global stripeIdx[%+v]", globalRepairIdxs)
	allocBufErr = r.recoverByGlobalStripe(ctx, failBids, globalRepairIdxs)
	if allocBufErr != nil {
		return allocBufErr
	}

	span.Infof("step3: recover by local stripe again")
	allocBufErr = r.recoverByLocalStripe(ctx, failBids, repairIdxs)
	if allocBufErr != nil {
		return allocBufErr
	}

	failBids = r.collectFailBids(failBids, repairIdxs)
	if len(failBids) != 0 {
		span.Errorf("recoverLocalReplicaShards failed: failBids len[%d]", len(failBids))
		return errBidCanNotRecover
	}

	return nil
}

func (r *ShardRecover) collectGlobalBadReplicas(ctx context.Context, failBids []proto.BlobID, repairIdxs []uint8) []uint8 {
	span := trace.SpanFromContextSafe(ctx)

	globalRepairIdxs := []uint8{}
	globalRepairIdxsMap := make(map[int]bool)
	globalReplicaIdxs := []int{}
	repairIdxsInIdc := workutils.IdxSplitByLocalStripe(repairIdxs, r.codeMode)

	for _, repairIdxs := range repairIdxsInIdc {
		if len(repairIdxs) == 0 {
			continue
		}
		idxs, n, _ := r.codeMode.T().LocalStripe(int(repairIdxs[0]))
		globalReplicaIdxs = append(globalReplicaIdxs, idxs[0:n]...)
	}

	for _, bid := range failBids {
		for _, globalReplicaIdx := range globalReplicaIdxs {
			if r.chunksShardsBuf[globalReplicaIdx] == nil {
				globalRepairIdxsMap[globalReplicaIdx] = true
				continue
			}

			if !r.chunksShardsBuf[globalReplicaIdx].shardIsOk(bid) {
				globalRepairIdxsMap[globalReplicaIdx] = true
			}
		}
	}

	for idx := range globalRepairIdxsMap {
		globalRepairIdxs = append(globalRepairIdxs, uint8(idx))
	}

	span.Infof("collect global bad replicas: idx[%+v]", globalRepairIdxs)
	return globalRepairIdxs
}

func (r *ShardRecover) directGetShard(ctx context.Context, repairBids []proto.BlobID, repairIdxs []uint8) (failBids []proto.BlobID, allocBufErr error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Info("start direct get shard")

	allocBufErr = r.allocBuf(ctx, repairIdxs)
	if allocBufErr != nil {
		return nil, allocBufErr
	}
	replicas := make(Vunits, len(repairIdxs))
	for i, idx := range repairIdxs {
		replicas[i] = r.replicas[idx]
	}

	r.download(ctx, repairBids, replicas)
	failBids = r.collectFailBids(repairBids, repairIdxs)
	span.Infof("end direct get shard: failBids len[%d], allocBufErr[%+v]", len(failBids), allocBufErr)

	return failBids, allocBufErr
}

func (r *ShardRecover) recoverByLocalStripe(ctx context.Context, repairBids []proto.BlobID, repairIdxs []uint8) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start recover by local stripe: repairIdxs[%+v]", repairIdxs)

	stripes, err := r.genLocalStripes(repairIdxs)
	if err != nil {
		return err
	}
	span.Infof("start recoverByLocalStripe: badIdxes[%+v], len stripes[%d]", repairIdxs, len(stripes))
	if len(stripes) == 0 {
		return nil
	}

	for _, stripe := range stripes {
		//todo:repairs between strips are completely unrelated,
		// so can improve efficiency through concurrent repair
		idxs := stripe.replicas.Indexes()
		err = r.allocBuf(ctx, idxs)
		if err != nil {
			return
		}
		err = r.repairStripe(ctx, repairBids, stripe)
		if err != nil {
			return err
		}
	}
	span.Info("end recoverByLocalStripe")
	return
}

func (r *ShardRecover) recoverByGlobalStripe(ctx context.Context, repairBids []proto.BlobID, repairIdxs []uint8) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start recoverByGlobalStripe: repairIdxs[%+v]", repairIdxs)

	stripe, err := r.genGlobalStripe(repairIdxs)
	if err != nil {
		return err
	}
	idxs := stripe.replicas.Indexes()
	err = r.allocBuf(ctx, idxs)
	if err != nil {
		return
	}
	err = r.repairStripe(ctx, repairBids, stripe)
	if err != nil {
		return
	}
	span.Info("end recoverByGlobalStripe")
	return
}

func (r *ShardRecover) repairStripe(ctx context.Context, repairBids []proto.BlobID, stripe repairStripe) (err error) {
	// step1:gen download plans for repair
	span := trace.SpanFromContextSafe(ctx)

	downloadPlans := stripe.genDownloadPlans()
	span.Infof("start repairStripe: downloadPlans len[%d], len(repairBids)[%d]", len(downloadPlans), len(repairBids))
	failBids := repairBids

	// step2:download data according download plans and repair data
	for _, plan := range downloadPlans {
		r.download(ctx, failBids, plan.downloadReplicas)
		err = r.repair(ctx, failBids, stripe)
		if err != nil {
			span.Errorf("plan.downloadReplicas:%+v repair error:%v", plan.downloadReplicas, err)
		}
		failBids = r.collectFailBids(failBids, stripe.badIdxes)
		if len(failBids) == 0 {
			return nil
		}
	}
	return err
}

func (r *ShardRecover) download(ctx context.Context, repairBids []proto.BlobID, replicas Vunits) {
	wg := sync.WaitGroup{}
	tp := taskpool.New(len(replicas), len(replicas))
	for _, replica := range replicas {
		wg.Add(1)
		pSpan := trace.SpanFromContextSafe(ctx)
		_, ctxTmp := trace.StartSpanFromContextWithTraceID(context.Background(), "downloadShard", pSpan.TraceID())
		rep := replica
		tp.Run(func() {
			defer wg.Done()
			r.downloadReplShards(ctxTmp, rep, repairBids)
		})
	}
	wg.Wait()
	tp.Close()
}

func (r *ShardRecover) downloadReplShards(ctx context.Context, replica proto.VunitLocation, repairBids []proto.BlobID) {
	span := trace.SpanFromContextSafe(ctx)
	vuid := replica.Vuid

	if !r.ds.needDownload(vuid) {
		span.Infof("skip download: replica[%+v], idx[%d]", replica, vuid.Index())
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	tp := taskpool.New(r.vunitShardGetConcurrency, r.vunitShardGetConcurrency)
	span.Infof("start downloadSingle: repl idx[%d], len bids[%d]", vuid.Index(), len(repairBids))
	for _, bid := range repairBids {
		wg.Add(1)
		downloadBid := bid
		tp.Run(func() {
			defer wg.Done()
			err := r.downloadShard(ctx, replica, downloadBid)
			if err == nil {
				return
			}

			span.Errorf("download shard: replica[%+v], bid[%d], err[%+v]", replica, downloadBid, err)
			if AllShardsCanNotDownload(err) {
				span.Infof("all shards can not download, so cancel download: replica[%+v]", replica)
				cancel()
			}
		})
	}
	wg.Wait()
	tp.Close()
	span.Infof("finish downloadSingle: vuid[%d], idx[%d]", vuid, vuid.Index())
}

func (r *ShardRecover) downloadShard(ctx context.Context, replica proto.VunitLocation, bid proto.BlobID) error {
	span := trace.SpanFromContextSafe(ctx)

	select {
	case <-ctx.Done():
		span.Infof("download cancel: replica[%+v],  bid[%d]", replica, bid)
		return nil
	default:
		data, crc1, err := r.shardGetter.GetShard(ctx, replica, bid, r.ioType)
		r.ds.downloaded(replica.Vuid)
		if err != nil {
			span.Errorf("download failed: replica[%+v], bid[%d], err[%+v]", replica, bid, err)
			return err
		}

		err = r.chunksShardsBuf[replica.Vuid.Index()].PutShard(bid, data)
		data.Close()
		if err == errBidNotFoundInBuf {
			span.Errorf("unexpect put shard failed: err[%+v]", err)
			return err
		}
		if err == errBufHasData {
			bufCrc, _ := r.chunksShardsBuf[replica.Vuid.Index()].ShardCrc32(bid)
			if bufCrc != crc1 {
				span.Errorf("data conflict crc32 not match: bid[%d], bufCrc[%d], crc1[%d]", bid, bufCrc, crc1)
				return errCrcNotMatch
			}
			return nil
		}

		if err != nil {
			span.Errorf("blob put shard to buf failed: replica[%+v], bid[%d], err[%+v]", replica, bid, err)
			return err
		}

		crc2, _ := r.chunksShardsBuf[replica.Vuid.Index()].ShardCrc32(bid)
		if crc1 != crc2 {
			span.Errorf("shard crc32 not match: replica[%+v], bid[%d], crc1[%d], crc2[%d]", replica, bid, crc1, crc2)
			return errCrcNotMatch
		}
		return nil
	}
}

func (r *ShardRecover) repair(ctx context.Context, repairBids []proto.BlobID, stripe repairStripe) error {
	span := trace.SpanFromContextSafe(ctx)

	var err error
	n := stripe.n
	m := stripe.m
	replicas := stripe.replicas

	span.Infof("start repair stripe: n[%d], m[%d], bids len[%d], replicas[%+v]", n, m, len(repairBids), replicas)

	if len(replicas) == 0 {
		span.Error("unexpect len of replicas is zero")
		return errUnexpectedLength
	}

	encoder := workutils.EncoderPoolInst().GetEncoder(int(n), int(m))
	for _, bid := range repairBids {
		span.Debugf("start repair: bid[%d]", bid)

		blobShards := make([][]byte, len(replicas))
		var recoverIdxOfVunit []uint8
		var recoverIdxOfStripe []int
		for i := 0; i < len(replicas); i++ {
			vuid := replicas[i].Vuid
			blobShards[i], err = r.chunksShardsBuf[vuid.Index()].getShardBuf(bid)
			if err != nil {
				span.Errorf("unexpect get shard: bid[%d], buf fail err[%+v]", bid, err)
				return err
			}

			if !r.chunksShardsBuf[vuid.Index()].shardIsOk(bid) {
				recoverIdxOfVunit = append(recoverIdxOfVunit, vuid.Index())
				recoverIdxOfStripe = append(recoverIdxOfStripe, i)
			}
		}
		span.Debugf("shouldRecoverIdx recoverIdxOfVunit[%+v], recoverIdxOfStripe[%+v]", recoverIdxOfVunit, recoverIdxOfStripe)

		if r.chunksShardsBuf[replicas[0].Vuid.Index()].ShardSizeIsZero(bid) {
			span.Infof("blob size is zero not need to recover: bid[%d]", bid)
			continue
		}

		if len(recoverIdxOfVunit) > int(m) {
			span.Debugf("too many data can not prepared: bid[%d]", bid)
			continue
		}

		if len(recoverIdxOfStripe) != len(recoverIdxOfVunit) {
			span.Errorf("unexpect:len of recoverIdxOfStripe(%d) and recoverIdxOfVunit(%d) must equal",
				len(recoverIdxOfStripe), len(recoverIdxOfVunit))
			return errUnexpectedLength
		}

		if len(recoverIdxOfVunit) == 0 {
			span.Warnf("not bids need to recover, theoretically will not appear")
			continue
		}

		err = encoder.Reconstruct(blobShards)
		if err != nil {
			span.Errorf("reconstruct shard failed: err[%+v]", err)
		}
		// make sure ec reconstruct is correct
		ok, err := encoder.Verify(blobShards)
		if err != nil || !ok {
			span.Errorf(" ec verify failed: ok[%+v], err[%+v]", err, ok)
			return errEcVerifyFailed
		}

		for i := range recoverIdxOfVunit {
			volIdx := recoverIdxOfVunit[i]
			stripeIdx := recoverIdxOfStripe[i]
			err = r.chunksShardsBuf[volIdx].setShardBuf(ctx, bid, blobShards[stripeIdx])
			if err != nil {
				span.Errorf("unexpect error when set shard buf: idx[%d], bid[%d], err[%+v]", volIdx, bid, err)
				return err
			}
		}
	}
	return nil
}

func (r *ShardRecover) genLocalStripes(repairIdxs []uint8) (stripes []repairStripe, err error) {
	// generate local stripes list in same az with repairIdxs
	repairIdxsInIdc := workutils.IdxSplitByLocalStripe(repairIdxs, r.codeMode)
	for _, oneIdcRepairIdxs := range repairIdxsInIdc {
		if len(oneIdcRepairIdxs) == 0 {
			continue
		}
		idxs, n, m := r.codeMode.T().LocalStripe(int(oneIdcRepairIdxs[0]))

		replicas := r.replicas.Subset(idxs)
		stripe := repairStripe{
			replicas: replicas,
			n:        N(n),
			m:        M(m),
			badIdxes: oneIdcRepairIdxs,
		}
		stripes = append(stripes, stripe)
	}
	return stripes, nil
}

func (r *ShardRecover) genGlobalStripe(repairIdxs []uint8) (stripe repairStripe, err error) {
	// generate global stripes
	idxs, n, m := r.codeMode.T().GlobalStripe()
	replicas := r.replicas.Subset(idxs)
	return repairStripe{
		replicas: replicas,
		n:        N(n),
		m:        M(m),
		badIdxes: repairIdxs,
	}, nil
}

func (r *ShardRecover) collectFailBids(repairBids []proto.BlobID, repairIdxs []uint8) []proto.BlobID {
	var failBids []proto.BlobID
	for _, bid := range repairBids {
		for _, idx := range repairIdxs {
			if r.chunksShardsBuf[idx] == nil {
				failBids = append(failBids, bid)
				break
			}

			if !r.chunksShardsBuf[idx].shardIsOk(bid) {
				failBids = append(failBids, bid)
				break
			}
		}
	}
	return failBids
}

// GetShard returns shards data
func (r *ShardRecover) GetShard(idx uint8, bid proto.BlobID) ([]byte, error) {
	return r.chunksShardsBuf[idx].FetchShard(bid)
}

// ReleaseBuf release chunks shards buffer
func (r *ShardRecover) ReleaseBuf() {
	for idx := range r.chunksShardsBuf {
		if r.chunksShardsBuf[idx] != nil {
			workutils.TaskBufPool.Put(r.chunksShardsBuf[idx].buf)
			r.chunksShardsBuf[idx] = nil
		}
	}
}

func (r *ShardRecover) allocBuf(ctx context.Context, vunitIdxs []uint8) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("alloc buf: vunit idxs[%+v]", vunitIdxs)
	var (
		buf []byte
		err error
	)
	for _, idx := range vunitIdxs {
		if r.chunksShardsBuf[idx] == nil {
			switch r.taskType {
			case proto.TaskTypeShardRepair:
				buf, err = workutils.TaskBufPool.GetRepairBuf()
			case proto.TaskTypeDiskRepair, proto.TaskTypeBalance, proto.TaskTypeManualMigrate, proto.TaskTypeDiskDrop:
				buf, err = workutils.TaskBufPool.GetMigrateBuf()
			default:
				err = errors.New("unknown type")
			}

			if err != nil {
				span.Errorf("alloc buf failed: err[%+v]", err)
				return err
			}
			r.chunksShardsBuf[idx] = NewShardsBuf(buf)
			err = r.chunksShardsBuf[idx].PlanningDataLayout(r.repairBidsReadOnly)
			if err != nil {
				return err
			}

		}
	}
	return nil
}

// AllShardsCanNotDownload judge whether all shards can  download or not accord by download error
func AllShardsCanNotDownload(shardDownloadFail error) bool {
	code := rpc.DetectStatusCode(shardDownloadFail)
	switch code {
	case errcode.CodeShardMarkDeleted, errcode.CodeBidNotFound, errcode.CodeShardSizeTooLarge:
		return false
	default:
		return true
	}
}
