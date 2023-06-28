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
	"fmt"
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
	errUnexpectedLength     = errors.New("length of locations is unexpected")
	errEcVerifyFailed       = errors.New("ec verify failed")
	errShardSizeNotMatch    = errors.New("shard data size not match")
	errBufNotEnough         = errors.New("buf space not enough")
	errInvalidLocations     = errors.New("invalid volume locations")
	errInvalidCodeMode      = errors.New("invalid codeMode ")
)

const defaultGetConcurrency = 100

type downloadPlan struct {
	locations VunitLocations
}

// VunitLocations volume stripe locations.
type VunitLocations []proto.VunitLocation

func (locs VunitLocations) IsValid() bool {
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

func (locs VunitLocations) Indexes() []uint8 {
	idxes := make([]uint8, len(locs))
	for idx, loc := range locs {
		idxes[idx] = loc.Vuid.Index()
	}
	return idxes
}

func (locs VunitLocations) Subset(idxes []int) VunitLocations {
	sub := make(VunitLocations, 0, len(idxes))
	for _, idx := range idxes {
		sub = append(sub, locs[idx])
	}
	return sub
}

func (locs VunitLocations) IntactGlobalSet(mode codemode.CodeMode, badIdxes []uint8) VunitLocations {
	ex := make(map[int]struct{}, len(badIdxes))
	for _, idx := range badIdxes {
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

func (stripe *repairStripe) genDownloadPlans() []downloadPlan {
	n := stripe.n
	badIdxes := stripe.badIdxes
	normalLocations := make(VunitLocations, 0, n)

	stripeLocations := make(VunitLocations, len(stripe.locations))
	copy(stripeLocations, stripe.locations)
	rand.Shuffle(len(stripeLocations), func(i, j int) {
		stripeLocations[i], stripeLocations[j] = stripeLocations[j], stripeLocations[i]
	})

	badMap := make(map[uint8]struct{})
	for _, badIdx := range badIdxes {
		badMap[badIdx] = struct{}{}
	}

	for _, location := range stripeLocations {
		locationIdx := location.Vuid.Index()
		if _, ok := badMap[locationIdx]; !ok {
			normalLocations = append(normalLocations, location)
		}
	}

	planCnt := len(normalLocations) - int(n) + 1
	downloadPlans := make([]downloadPlan, 0, planCnt)
	for i := 0; i < planCnt; i++ {
		plan := downloadPlan{
			locations: make([]proto.VunitLocation, n),
		}
		copy(plan.locations, normalLocations[0:n-1]) // n-1 locations
		plan.locations[n-1] = normalLocations[n-1+i]
		downloadPlans = append(downloadPlans, plan)
	}
	return downloadPlans
}

type repairStripe struct {
	locations VunitLocations
	n         int
	m         int
	badIdxes  []uint8
}

// duties：repair shard data
// if get shard data directly fail,
// for global stripe chunks(N+M) will do next step
//   step1: repair use local stripe ,if success return
//   step2: use partial repair(just for RS code_mode to reduce Cross-AZ traffic), if success return
//   step3: repair use global stripe
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
// the order of chunks in list is keep some with volume locations

// usage：
// first call RecoverShards to repair shard
// then call GetShard to get assign shard data

const (
	defaultPartialThreshold = 1
)

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
func (b *ShardsBuf) PlanningDataLayout(bids []*ShardInfoSimple) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var totalSize int64 = 0
	for _, bid := range bids {
		totalSize += bid.Size
	}
	if totalSize > int64(len(b.buf)) {
		return errBufNotEnough
	}

	var offset int64 = 0
	for _, bid := range bids {
		s := shard{
			data: b.buf[offset : offset+bid.Size],
			size: bid.Size,
			ok:   false,
		}
		if bid.Size == 0 {
			s.ok = true
		}
		b.shards[bid.Bid] = &s
		offset += bid.Size
	}
	return nil
}

func (b *ShardsBuf) getShardBuf(bid proto.BlobID) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.shards[bid]; !ok {
		return nil, errBidNotFoundInBuf
	}
	if b.shards[bid].ok {
		return b.shards[bid].data, nil
	}
	retBuf := b.shards[bid].data[0:0]
	return retBuf, nil
}

func (b *ShardsBuf) setShardBuf(ctx context.Context, bid proto.BlobID, buf []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	span := trace.SpanFromContextSafe(ctx)

	if _, ok := b.shards[bid]; !ok {
		return errBidNotFoundInBuf
	}
	if b.shards[bid].ok {
		return errBufHasData
	}
	if b.shards[bid].size == 0 {
		b.shards[bid].ok = true
		return nil
	}

	ptr1 := unsafe.Pointer(&b.shards[bid].data[0])
	ptr2 := unsafe.Pointer(&buf[0])
	if ptr1 == ptr2 && len(buf) == int(b.shards[bid].size) {
		b.shards[bid].data = buf
		b.shards[bid].ok = true
		return nil
	}

	span.Errorf("set shard buf failed: expect point[%p], expect size[%d], actual point[%p], actual size[%d]",
		b.shards[bid].data, b.shards[bid].size,
		buf, len(buf))
	return errIllegalBuf
}

// ShardSizeIsZero return true if shard size is zero
func (b *ShardsBuf) ShardSizeIsZero(bid proto.BlobID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.shards[bid].size == 0
}

// FetchShard returns shard data
func (b *ShardsBuf) FetchShard(bid proto.BlobID) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.shards[bid]; !ok {
		return nil, errBidNotFoundInBuf
	}
	if b.shards[bid].size == 0 {
		return b.shards[bid].data, nil
	}
	if !b.shards[bid].ok {
		return nil, errShardDataNotPrepared
	}

	return b.shards[bid].data, nil
}

// PutShard put shard data to shardsBuf
func (b *ShardsBuf) PutShard(bid proto.BlobID, input io.Reader) error {
	b.mu.Lock()

	if _, ok := b.shards[bid]; !ok {
		b.mu.Unlock()
		return errBidNotFoundInBuf
	}
	if b.shards[bid].size == 0 {
		b.mu.Unlock()
		return nil
	}
	if b.shards[bid].ok {
		b.mu.Unlock()
		return errBufHasData
	}

	size := b.shards[bid].size
	if int64(len(b.shards[bid].data)) != size {
		return errShardSizeNotMatch
	}
	b.mu.Unlock()

	// read data from remote is slow,so optimize use of lock
	_, err := io.ReadFull(input, b.shards[bid].data)
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.shards[bid].ok = true
	b.mu.Unlock()
	return nil
}

func (b *ShardsBuf) shardIsOk(bid proto.BlobID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exist := b.shards[bid]; exist {
		return b.shards[bid].ok
	}
	return false
}

// ShardCrc32 returns shard crc32
func (b *ShardsBuf) ShardCrc32(bid proto.BlobID) (crc uint32, err error) {
	buf, err := b.FetchShard(bid)
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

	locations          VunitLocations // stripe locations list
	codeMode           codemode.CodeMode
	repairBidsReadOnly []*ShardInfoSimple // Strictly not allow modification

	shardGetter              client.IBlobNode
	vunitShardGetConcurrency int
	ioType                   blobnode.IOType
	taskType                 proto.TaskType
	ds                       *downloadStatus
	partialData              map[proto.BlobID][]byte // repair intermediate state data
	enablePartial            bool
	partialThreshold         int
}

// NewShardRecover returns shard recover
func NewShardRecover(locations VunitLocations, mode codemode.CodeMode, bidInfos []*ShardInfoSimple,
	shardGetter client.IBlobNode, vunitShardGetConcurrency int, taskType proto.TaskType,
	enableAssist bool,
) *ShardRecover {
	if vunitShardGetConcurrency <= 0 {
		vunitShardGetConcurrency = defaultGetConcurrency
	}
	ioType := blobnode.Task2IOType(taskType)

	repair := ShardRecover{
		locations:                locations,
		chunksShardsBuf:          make([]*ShardsBuf, len(locations)),
		codeMode:                 mode,
		repairBidsReadOnly:       bidInfos,
		shardGetter:              shardGetter,
		ioType:                   ioType,
		taskType:                 taskType,
		vunitShardGetConcurrency: vunitShardGetConcurrency,
		ds:                       newDownloadStatus(),
		partialData:              make(map[proto.BlobID][]byte),
		enablePartial:            enableAssist,
		partialThreshold:         defaultPartialThreshold,
	}
	return &repair
}

// RecoverShards recover shards
func (r *ShardRecover) RecoverShards(ctx context.Context, repairIdxs []uint8, direct bool) error {
	span := trace.SpanFromContextSafe(ctx)
	if !r.locations.IsValid() {
		return errInvalidLocations
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
		repairVuid := r.locations[idx].Vuid
		r.ds.forbiddenDownload(repairVuid)
	}

	span.Infof("start recover shards: repairIdxs[%+v], len repairBidInfos[%d]", repairIdxs, len(r.repairBidsReadOnly))

	err := r.recoverReplicaShards(ctx, repairIdxs, repairBids)
	if err != nil {
		span.Errorf("end recoverReplicaShards failed: err[%+v]", err)
		return err
	}

	span.Infof("end recover shards success")
	return nil
}

func (r *ShardRecover) recoverReplicaShards(ctx context.Context, repairIdxs []uint8, repairBids []proto.BlobID) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start recover global shards: repairIdxs[%+v], len(repairBids)[%d]", repairIdxs, len(repairBids))

	failBids := repairBids
	var err error

	if localRepairable(repairIdxs, r.codeMode) && r.codeMode.Tactic().CodeType == codemode.OPPOLrc {
		span.Infof("recover by local stripe,codemode:%v", r.codeMode.Tactic().CodeType)
		err = r.recoverByLocalStripe(ctx, failBids, repairIdxs)
		if err != nil {
			span.Warnf("recover by local stripe failed:%v", err)
		}

		failBids = r.collectFailBids(failBids, repairIdxs)
		if len(failBids) == 0 {
			return nil
		}
		span.Warnf("after local repaired, still fail bids is:%v", failBids)
	}

	span.Infof("recover by global stripe")
	err = r.recoverByGlobalStripe(ctx, failBids, repairIdxs)
	if err != nil {
		span.Errorf("recover by global stripe failed %v", err)
		return err
	}

	failBids = r.collectFailBids(failBids, repairIdxs)
	if len(failBids) != 0 {
		span.Errorf("recoverReplicaShards failed: failBids len[%d]", len(failBids))
		return errBidCanNotRecover
	}
	return nil
}

func (r *ShardRecover) directGetShard(ctx context.Context, repairBids []proto.BlobID, repairIdxs []uint8) (failBids []proto.BlobID, allocBufErr error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Info("start direct get shard")

	allocBufErr = r.allocBuf(ctx, repairIdxs)
	if allocBufErr != nil {
		return nil, allocBufErr
	}
	locations := make(VunitLocations, len(repairIdxs))
	for i, idx := range repairIdxs {
		locations[i] = r.locations[idx]
	}

	r.download(ctx, repairBids, locations)
	failBids = r.collectFailBids(repairBids, repairIdxs)
	span.Infof("end direct get shard: failBids len[%d], allocBufErr[%+v]", len(failBids), allocBufErr)

	return failBids, allocBufErr
}

func (r *ShardRecover) recoverByLocalStripe(ctx context.Context, failBids []proto.BlobID, repairIdxs []uint8) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start recover by local stripe: repairIdxs[%+v]", repairIdxs)

	rStripes, err := r.genLocalStripes(repairIdxs)
	if err != nil {
		return err
	}
	span.Infof("start recoverByLocalStripe: badIdxes[%+v], len stripes[%d]", repairIdxs, len(rStripes))
	if len(rStripes) == 0 {
		return nil
	}

	for _, rStripe := range rStripes {
		//todo:repairs between strips are completely unrelated,
		// so can improve efficiency through concurrent repair
		idxs := rStripe.locations.Indexes()
		err = r.allocBuf(ctx, idxs)
		if err != nil {
			return
		}
		err = r.repairStripe(ctx, failBids, rStripe)
		if err != nil {
			return err
		}
	}
	span.Info("end recoverByLocalStripe")
	return
}

func (r *ShardRecover) recoverByGlobalStripe(ctx context.Context, failBids []proto.BlobID, repairIdxs []uint8) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start recoverByGlobalStripe: repairIdxs[%+v]", repairIdxs)

	var stripe repairStripe
	switch r.codeMode.Tactic().CodeType {
	case codemode.ReedSolomon, codemode.OPPOLrc:
		stripe = repairStripe{
			locations: r.locations,
			n:         r.codeMode.T().N,
			m:         r.codeMode.T().M,
			badIdxes:  repairIdxs,
		}
	case codemode.AzureLrcP1:
		stripe = repairStripe{
			locations: r.locations,
			n:         r.codeMode.T().N,
			m:         r.codeMode.T().M + r.codeMode.T().L,
			badIdxes:  repairIdxs,
		}
	default:
		span.Errorf("wrong codemode:%v for recoverByGlobalStripe", r.codeMode.Tactic().CodeType)
		return errInvalidCodeMode
	}
	idxs := stripe.locations.Indexes()
	err = r.allocBuf(ctx, idxs)
	if err != nil {
		return
	}
	// if enable partial, repair within az
	if r.needPartialRepair(repairIdxs) {
		err = r.partialRepairBids(ctx, failBids, stripe)
		if err == nil {
			newFailBids := r.collectFailBids(failBids, repairIdxs)
			if len(newFailBids) == 0 {
				span.Debugf("shard repair by partial success, bids[%v]", newFailBids)
				return
			}
			failBids = newFailBids
		}
		span.Warnf("shard partial repair failed, err: %v，continue  use global repair", err)
	}
	err = r.repairStripe(ctx, failBids, stripe)
	if err != nil {
		return
	}
	span.Info("end recoverByGlobalStripe")
	return
}

func (r *ShardRecover) needPartialRepair(badIdxes []uint8) bool {
	return r.enablePartial && len(badIdxes) < 2 &&
		r.codeMode.Tactic().CodeType == codemode.ReedSolomon && r.codeMode.T().AZCount > 1
}

func (r *ShardRecover) repairStripe(ctx context.Context, repairBids []proto.BlobID, stripe repairStripe) (err error) {
	// step1:gen download plans for repair
	span := trace.SpanFromContextSafe(ctx)
	downloadPlans := stripe.genDownloadPlans()
	span.Infof("start repairStripe: downloadPlans len[%d], len(repairBids)[%d]", len(downloadPlans), len(repairBids))
	failBids := repairBids
	// step2: download data according download plans and repair data
	for _, plan := range downloadPlans {
		r.download(ctx, failBids, plan.locations)
		err = r.repair(ctx, failBids, stripe)
		if err != nil {
			span.Errorf("plan.locations:%+v repair error:%v", plan.locations, err)
		}
		failBids = r.collectFailBids(failBids, stripe.badIdxes)
		if len(failBids) == 0 {
			return nil
		}
	}
	return err
}

func (r *ShardRecover) partialRepairBids(ctx context.Context, repairBids []proto.BlobID,
	stripe repairStripe) (err error) {
	badIdx := int(stripe.badIdxes[0])
	// generate repair plan, direct download plan and partial download plan
	downPlan, partialPlan := r.getPartialPlan(badIdx, stripe.n)
	if len(downPlan)+len(partialPlan) < stripe.n {
		return fmt.Errorf("bid[%v]can not be repaired", repairBids)
	}
	surviveIndex := make([]int, stripe.n)
	idx := 0
	for _, location := range downPlan {
		surviveIndex[idx] = int(location.Vuid.Index())
		idx++
	}
	for _, location := range partialPlan {
		surviveIndex[idx] = int(location.Vuid.Index())
		idx++
	}
	failedBids := repairBids
	r.download(ctx, failedBids, downPlan)
	r.partialDownload(ctx, failedBids, &blobnode.ShardPartialRepairArgs{
		CodeMode:      r.codeMode,
		IoType:        r.ioType,
		SurvivalIndex: surviveIndex,
		BadIdx:        badIdx,
		Sources:       partialPlan,
	})
	return r.partialRepair(ctx, failedBids, downPlan, surviveIndex, []int{badIdx})
}

func (r *ShardRecover) partialRepair(ctx context.Context, failedBids []proto.BlobID,
	locations VunitLocations, surviveIndex []int, badIndex []int) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	encoder, err := workutils.GetEncoder(r.codeMode)
	if err != nil {
		span.Errorf("get encoder failed for code_mode[%s]", r.codeMode.String())
		return err
	}
	for _, bid := range failedBids {
		span.Debugf("start repair: bid[%d]", bid)
		if len(r.partialData[bid]) == 0 {
			continue
		}
		blobShards := make([][]byte, r.codeMode.GetShardNum())
		var badIdxOfVunits []uint8
		badIdxOfVunits, err = r.getDownloadData(bid, blobShards, locations)
		if err != nil {
			span.Errorf("get data failed from buffer, bid[%v], err[%s]", bid, err.Error())
			return err
		}
		if len(badIdxOfVunits) != 0 {
			span.Warnf("get shard failed, bid[%v]", bid)
			continue
		}

		bdIdx := badIndex[0]
		blobShards[bdIdx], _ = r.chunksShardsBuf[bdIdx].getShardBuf(bid)
		err = encoder.PartialReconstruct(blobShards, surviveIndex, badIndex)
		if err != nil {
			span.Errorf("partial repair failed, bid[%v], err[%s]", bid, err.Error())
			return err
		}

		for i := 0; i < len(r.partialData[bid]); i++ {
			blobShards[bdIdx][i] ^= r.partialData[bid][i]
		}
		err = r.chunksShardsBuf[bdIdx].setShardBuf(ctx, bid, blobShards[bdIdx])
		if err != nil {
			span.Errorf("unexpect error when set shard buf: idx[%d], bid[%d], err[%+v]", bdIdx, bid, err)
			return err
		}
	}
	return nil
}

func (r *ShardRecover) getDownloadData(bid proto.BlobID, data [][]byte, locations VunitLocations) ([]uint8, error) {
	var badIdxOfVunits []uint8
	var err error
	for i := 0; i < len(locations); i++ {
		vuid := locations[i].Vuid
		idx := vuid.Index()
		if !r.chunksShardsBuf[idx].shardIsOk(bid) {
			badIdxOfVunits = append(badIdxOfVunits, idx)
			continue
		}
		data[idx], err = r.chunksShardsBuf[idx].getShardBuf(bid)
		if err != nil {
			return nil, err
		}
	}
	return badIdxOfVunits, nil
}

func (r *ShardRecover) partialDownload(ctx context.Context, failedBids []proto.BlobID,
	args *blobnode.ShardPartialRepairArgs) {
	var wg sync.WaitGroup
	var lock sync.Mutex
	span := trace.SpanFromContextSafe(ctx)

	bidSize := make(map[proto.BlobID]int64, len(r.repairBidsReadOnly))
	for _, bidInfo := range r.repairBidsReadOnly {
		bidSize[bidInfo.Bid] = bidInfo.Size
	}
	for i, bid := range failedBids {
		if bidSize[bid] == 0 {
			continue
		}
		wg.Add(1)
		arg := &blobnode.ShardPartialRepairArgs{
			Bid:           bid,
			Size:          bidSize[bid],
			CodeMode:      args.CodeMode,
			Sources:       args.Sources,
			IoType:        r.ioType,
			SurvivalIndex: args.SurvivalIndex,
			BadIdx:        args.BadIdx,
		}
		go func(j int) {
			defer wg.Done()
			partialRepairRet, err := r.shardGetter.ShardPartialRepair(ctx, arg.Sources[j].Host, arg)
			if err != nil {
				span.Errorf("shard partial repair failed to host[%s]", arg.Sources[j].Host)
				return
			}
			if len(partialRepairRet.BadIdxes) != 0 {
				for _, idx := range partialRepairRet.BadIdxes {
					r.ds.forbiddenDownload(r.locations[idx].Vuid)
				}
				return
			}
			lock.Lock()
			r.partialData[arg.Bid] = partialRepairRet.Data
			lock.Unlock()
		}(i % len(arg.Sources))
	}
	wg.Wait()
}

func (r *ShardRecover) getPartialPlan(badIdx, n int) (dl, pl VunitLocations) {
	tactic := r.codeMode.Tactic()
	layoutByAZ := tactic.GetECLayoutByAZ()
	surviveLocation := make([][]proto.VunitLocation, len(layoutByAZ))
	indexToLocations := make(map[uint8]proto.VunitLocation, r.codeMode.GetShardNum())
	for _, location := range r.locations {
		idx := location.Vuid.Index()
		indexToLocations[idx] = location
	}
	workAz := -1
	for i, azIdxes := range layoutByAZ {
		for _, idx := range azIdxes {
			if idx == badIdx {
				workAz = i
				continue
			}
			surviveLocation[i] = append(surviveLocation[i], r.locations[idx])
		}
	}

	dl = append(dl, surviveLocation[workAz]...)
	need := n - len(surviveLocation[workAz])
	numAz := len(surviveLocation)
	az := (workAz + 1) % numAz
	num := len(surviveLocation[az])
	if num >= need {
		pl = append(pl, surviveLocation[az][:need]...)
		return
	}
	pl = append(pl, surviveLocation[az]...)
	need -= num
	for need != 0 {
		az = (az + 1) % numAz
		if az == workAz {
			break
		}
		num = len(surviveLocation[az])
		if num >= need {
			dl = append(dl, surviveLocation[az][:need]...)
			return
		}
		dl = append(dl, surviveLocation[az]...)
		need -= num
	}
	return
}

func (r *ShardRecover) download(ctx context.Context, repairBids []proto.BlobID, locations VunitLocations) {
	wg := sync.WaitGroup{}
	tp := taskpool.New(len(locations), len(locations))
	for _, location := range locations {
		wg.Add(1)
		pSpan := trace.SpanFromContextSafe(ctx)
		_, ctxTmp := trace.StartSpanFromContextWithTraceID(context.Background(), "downloadShard", pSpan.TraceID())
		rep := location
		tp.Run(func() {
			defer wg.Done()
			r.downloadReplShards(ctxTmp, rep, repairBids)
		})
	}
	wg.Wait()
	tp.Close()
}

func (r *ShardRecover) downloadReplShards(ctx context.Context, location proto.VunitLocation, repairBids []proto.BlobID) {
	span := trace.SpanFromContextSafe(ctx)
	vuid := location.Vuid

	if !r.ds.needDownload(vuid) {
		span.Infof("skip download: location[%+v], idx[%d]", location, vuid.Index())
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
			err := r.downloadShard(ctx, location, downloadBid)
			if err == nil {
				return
			}

			span.Errorf("download shard: location[%+v],index[%d], bid[%d], err[%+v]", location, location.Vuid.Index(), downloadBid, err)
			if AllShardsCanNotDownload(err) {
				span.Infof("all shards can not download, so cancel download: location[%+v]", location)
				cancel()
			}
		})
	}
	wg.Wait()
	tp.Close()
	span.Infof("finish downloadSingle: vuid[%d], idx[%d]", vuid, vuid.Index())
}

func (r *ShardRecover) downloadShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID) error {
	span := trace.SpanFromContextSafe(ctx)

	select {
	case <-ctx.Done():
		span.Infof("download cancel: location[%+v],  bid[%d]", location, bid)
		return nil
	default:
		data, crc1, err := r.shardGetter.GetShard(ctx, location, bid, r.ioType)
		r.ds.downloaded(location.Vuid)
		if err != nil {
			span.Errorf("download failed: location[%+v], bid[%d], err[%+v]", location, bid, err)
			return err
		}

		err = r.chunksShardsBuf[location.Vuid.Index()].PutShard(bid, data)
		data.Close()
		if err == errBidNotFoundInBuf {
			span.Errorf("unexpect put shard failed: err[%+v]", err)
			return err
		}
		if err == errBufHasData {
			bufCrc, _ := r.chunksShardsBuf[location.Vuid.Index()].ShardCrc32(bid)
			if bufCrc != crc1 {
				span.Errorf("data conflict crc32 not match: bid[%d], bufCrc[%d], crc1[%d]", bid, bufCrc, crc1)
				return errCrcNotMatch
			}
			return nil
		}

		if err != nil {
			span.Errorf("blob put shard to buf failed: location[%+v], bid[%d], err[%+v]", location, bid, err)
			return err
		}

		crc2, _ := r.chunksShardsBuf[location.Vuid.Index()].ShardCrc32(bid)
		if crc1 != crc2 {
			span.Errorf("shard crc32 not match: location[%+v], bid[%d], crc1[%d], crc2[%d]", location, bid, crc1, crc2)
			return errCrcNotMatch
		}
		return nil
	}
}

func (r *ShardRecover) repair(ctx context.Context, repairBids []proto.BlobID, stripe repairStripe) error {
	span := trace.SpanFromContextSafe(ctx)

	var err error
	locations := stripe.locations

	span.Infof("start repair stripe: code mode[%v], bids len[%d], locations[%+v]",
		r.codeMode.Tactic(), len(repairBids), locations)

	if len(locations) == 0 {
		span.Error("unexpect len of locations is zero")
		return errUnexpectedLength
	}

	encoder, err := workutils.GetEncoder(r.codeMode)
	if err != nil {
		return err
	}

	for _, bid := range repairBids {
		span.Debugf("start repair: bid[%d]", bid)

		blobShards := make([][]byte, len(locations))
		var badIdxOfVunits []uint8
		var badIdxes []int
		for i := 0; i < len(locations); i++ {
			vuid := locations[i].Vuid
			blobShards[i], err = r.chunksShardsBuf[vuid.Index()].getShardBuf(bid)
			if err != nil {
				span.Errorf("unexpect get shard: bid[%d], buf fail err[%+v]", bid, err)
				return err
			}

			if !r.chunksShardsBuf[vuid.Index()].shardIsOk(bid) {
				badIdxOfVunits = append(badIdxOfVunits, vuid.Index())
				badIdxes = append(badIdxes, i)
			}
		}
		span.Debugf("shouldRecoverIdx badIdxOfVunits[%+v], badIdxes[%+v]", badIdxOfVunits, badIdxes)

		if r.chunksShardsBuf[locations[0].Vuid.Index()].ShardSizeIsZero(bid) {
			span.Infof("blob size is zero not need to recover: bid[%d]", bid)
			continue
		}

		// broken uints length > M + L can not repair
		if len(badIdxOfVunits) > r.codeMode.Tactic().M+r.codeMode.Tactic().L {
			span.Warnf("too many data can not prepared: bid[%d]", bid)
			continue
		}

		if len(badIdxes) != len(badIdxOfVunits) {
			span.Errorf("unexpect:len of badIdxes(%d) and badIdxOfVunits(%d) must equal",
				len(badIdxes), len(badIdxOfVunits))
			return errUnexpectedLength
		}

		if len(badIdxOfVunits) == 0 {
			span.Warnf("not bids need to recover, theoretically will not appear")
			continue
		}

		err = encoder.Reconstruct(blobShards, badIdxes)
		if err != nil {
			span.Errorf("reconstruct shard failed: err[%+v],codemode:%v", err, r.codeMode.Tactic())
			return errBidCanNotRecover
		}

		if ok, err := encoder.Verify(blobShards); !ok {
			span.Errorf("verify data failed,err[%v]", err)
			return errEcVerifyFailed
		}

		for i, volIdx := range badIdxOfVunits {
			stripeIdx := badIdxes[i]
			err = r.chunksShardsBuf[volIdx].setShardBuf(ctx, bid, blobShards[stripeIdx])
			if err != nil {
				span.Errorf("unexpect error when set shard buf: idx[%d], bid[%d], err[%+v]", volIdx, bid, err)
				return err
			}
		}
	}

	return nil
}

func (r *ShardRecover) genLocalStripes(repairIdxs []uint8) (rStripes []repairStripe, err error) {
	// generate local stripes list in same az with repairIdxs
	repairIdxsInIdc := workutils.IdxSplitByLocalStripe(repairIdxs, r.codeMode)
	for _, oneIdcRepairIdxs := range repairIdxsInIdc {
		if len(oneIdcRepairIdxs) == 0 {
			continue
		}
		idxs, n, m := r.codeMode.T().LocalStripe(int(oneIdcRepairIdxs[0]))

		locations := r.locations.Subset(idxs)
		rStripe := repairStripe{
			locations: locations,
			n:         n,
			m:         m,
			badIdxes:  oneIdcRepairIdxs,
		}
		rStripes = append(rStripes, rStripe)
	}
	return rStripes, nil
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
			if r.enablePartial && r.partialData[bid] == nil && !r.chunksShardsBuf[idx].ShardSizeIsZero(bid) {
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

func localRepairable(badIdxs []uint8, mode codemode.CodeMode) bool {
	// localMap use count each az bad uint num
	localMap := make(map[int]int)
	for _, idx := range badIdxs {
		stripeIdxs, _, _ := mode.T().LocalStripe(int(idx))
		if len(stripeIdxs) == 0 {
			return false
		}
		localMap[stripeIdxs[0]]++
	}
	for _, v := range localMap {
		if v > mode.T().L/mode.T().AZCount {
			return false
		}
	}
	return true
}
