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
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/klauspost/reedsolomon"

	api "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/workutils"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

func init() {
	log.SetOutputLevel(log.Lfatal)
}

func testWithAllMode(t *testing.T, testFunc func(t *testing.T, mode codemode.CodeMode)) {
	for _, mode := range codemode.GetAllCodeModes() {
		testFunc(t, mode)
	}
}

func genMockVol(vid proto.Vid, mode codemode.CodeMode) ([]proto.VunitLocation, codemode.CodeMode) {
	modeInfo := mode.Tactic()
	replicas := make([]proto.VunitLocation, modeInfo.N+modeInfo.M+modeInfo.L)
	for i := 0; i < modeInfo.N+modeInfo.M+modeInfo.L; i++ {
		vuid, _ := proto.NewVuid(vid, uint8(i), 1)
		replicas[i] = proto.VunitLocation{
			Vuid:   vuid,
			Host:   "127.0.0.1:xxxx",
			DiskID: 1,
		}
	}
	return replicas, mode
}

func genMockBytes(letter byte, size int64) []byte {
	ret := make([]byte, size)
	var i int64
	for i = 0; i < size; i++ {
		ret[i] = byte(uint8(letter) + uint8(i))
	}
	return ret
}

type MockGetter struct {
	mu       sync.Mutex
	vunits   map[proto.Vuid]*mockVunit
	failVuid map[proto.Vuid]error
	bids     []proto.BlobID
	sizes    []int64
}

func NewMockGetter(replicas []proto.VunitLocation, mode codemode.CodeMode) *MockGetter {
	bids := []proto.BlobID{1, 2, 3, 4, 5, 6, 7}
	sizes := []int64{1024, 2048, 0, 512, 23, 65, 12}
	return NewMockGetterWithBids(replicas, mode, bids, sizes)
}

func NewMockGetterWithBids(replicas []proto.VunitLocation, mode codemode.CodeMode, bids []proto.BlobID, sizes []int64) *MockGetter {
	getter := MockGetter{
		bids:     bids,
		sizes:    sizes,
		vunits:   make(map[proto.Vuid]*mockVunit),
		failVuid: make(map[proto.Vuid]error),
	}
	for _, replica := range replicas {
		vunit := newMockVunit(replica.Vuid, api.ChunkStatusReadOnly)
		getter.vunits[replica.Vuid] = vunit
	}

	modeInfo := mode.Tactic()
	IdcDataShards := make([][][]byte, modeInfo.AZCount)
	for idx := range IdcDataShards {
		IdcDataShards[idx] = make([][]byte, modeInfo.N/modeInfo.AZCount)
	}

	// gen stripe
	for i := 0; i < len(bids); i++ {
		if sizes[i] == 0 {
			data := make([]byte, 0)
			for k := 0; k < modeInfo.N+modeInfo.M+modeInfo.L; k++ {
				vuid := replicas[k].Vuid
				getter.vunits[vuid].putShard(bids[i], data)
			}
			continue
		}

		stripeShards := make([][]byte, modeInfo.N+modeInfo.M)
		globalStripe, n, m := workutils.GlobalStripe(mode)
		for _, nShardIdx := range globalStripe[0:n] {
			vuid := replicas[nShardIdx].Vuid
			data := genMockBytes(byte(uint8(bids[i])+vuid.Index()), sizes[i])
			stripeShards[nShardIdx] = data
			getter.vunits[vuid].putShard(bids[i], data)
		}

		genParityShards(n, m, stripeShards)
		for _, mShardIdx := range globalStripe[n : n+m] {
			vuid := replicas[mShardIdx].Vuid
			getter.vunits[vuid].putShard(bids[i], stripeShards[mShardIdx])
		}

		stripeShards = append(stripeShards, make([][]byte, modeInfo.L)...)

		// gen local ParityShards
		if modeInfo.L == 0 {
			continue
		}

		localStripes, localN, localM := workutils.AllLocalStripe(mode)
		for _, localStripe := range localStripes {
			localStripeShards := abstractShards(localStripe, stripeShards)
			genParityShards(localN, localM, localStripeShards)
			for j, localUintIdx := range localStripe[localN : localN+localM] {
				data := localStripeShards[localN+j]
				vuid := replicas[localUintIdx].Vuid
				getter.vunits[vuid].putShard(bids[i], data)
			}
		}
	}
	return &getter
}

func (getter *MockGetter) MissSomeReplicaBid(replicas []proto.VunitLocation, missedInfo map[proto.BlobID][]int) {
	for bid, missedIdxs := range missedInfo {
		for _, idx := range missedIdxs {
			vuid := replicas[idx].Vuid
			getter.vunits[vuid].delete(bid)
		}
	}
}

func (getter *MockGetter) Recover(ctx context.Context, vuid proto.Vuid, bid proto.BlobID) {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	getter.vunits[vuid].recover(bid)
}

func (getter *MockGetter) setFail(vuid proto.Vuid, err error) {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	getter.failVuid[vuid] = err
}

func (getter *MockGetter) setWell(vuid proto.Vuid) {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	delete(getter.failVuid, vuid)
}

func (getter *MockGetter) setVunitStatus(vuid proto.Vuid, status api.ChunkStatus) {
	getter.mu.Lock()
	defer getter.mu.Unlock()

	if _, ok := getter.vunits[vuid]; ok {
		getter.vunits[vuid].status = status
	}
}

func (getter *MockGetter) PutShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, size int64, body io.Reader, ioType api.IOType) (err error) {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	if err, ok := getter.failVuid[location.Vuid]; ok {
		return err
	}
	data := make([]byte, size)
	body.Read(data)
	getter.vunits[location.Vuid].putShard(bid, data)
	return
}

func (getter *MockGetter) GetShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, ioType api.IOType) (body io.ReadCloser, crc32 uint32, err error) {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	vuid := location.Vuid
	if err, ok := getter.failVuid[vuid]; ok {
		return nil, 0, err
	}
	reader, crc, err := getter.vunits[vuid].getShard(bid)
	return ioutil.NopCloser(reader), crc, err
}

func (getter *MockGetter) MarkDelete(ctx context.Context, vuid proto.Vuid, bid proto.BlobID) {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	getter.vunits[vuid].markDelete(bid)
}

func (getter *MockGetter) Delete(ctx context.Context, vuid proto.Vuid, bid proto.BlobID) {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	getter.vunits[vuid].delete(bid)
}

func (getter *MockGetter) StatShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID) (si *client.ShardInfo, err error) {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	vuid := location.Vuid
	if err, ok := getter.failVuid[vuid]; ok {
		return nil, err
	}
	return getter.vunits[vuid].statShard(bid)
}

func (getter *MockGetter) ListShards(ctx context.Context, location proto.VunitLocation) (shards []*client.ShardInfo, err error) {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	vuid := location.Vuid
	if err, ok := getter.failVuid[vuid]; ok {
		return nil, err
	}
	return getter.vunits[vuid].listShards()
}

func (getter *MockGetter) StatChunk(ctx context.Context, location proto.VunitLocation) (ci *client.ChunkInfo, err error) {
	vuid := location.Vuid
	if _, ok := getter.vunits[vuid]; ok {
		vunitInfo := api.ChunkInfo{
			Vuid:   vuid,
			Status: getter.vunits[vuid].status,
		}
		return &client.ChunkInfo{
			ChunkInfo: vunitInfo,
		}, nil
	}
	return
}

func (getter *MockGetter) getSizes() []int64 {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	return getter.sizes
}

func (getter *MockGetter) getBids() []proto.BlobID {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	return getter.bids
}

func (getter *MockGetter) getShardCrc32(vuid proto.Vuid, bid proto.BlobID) uint32 {
	getter.mu.Lock()
	defer getter.mu.Unlock()
	return getter.vunits[vuid].getCrc32(bid)
}

func genParityShards(N, M int, shards [][]byte) {
	enc, _ := reedsolomon.New(N, M)
	err := enc.Reconstruct(shards)
	if err != nil {
		panic("repair: failed to ec reconstruct")
	}
	ok, err := enc.Verify(shards)
	if err != nil || !ok {
		panic("repair: failed to ec verify")
	}
}

func abstractShards(idxs []int, shards [][]byte) [][]byte {
	ret := [][]byte{}
	for _, idx := range idxs {
		ret = append(ret, shards[idx])
	}
	return ret
}

// mockVunit
type mockVunit struct {
	mu       sync.Mutex
	vuid     proto.Vuid
	status   api.ChunkStatus
	shards   map[proto.BlobID][]byte
	crc32    map[proto.BlobID]uint32
	bidInfos map[proto.BlobID]*client.ShardInfo
}

func newMockVunit(vuid proto.Vuid, status api.ChunkStatus) *mockVunit {
	m := mockVunit{
		vuid:     vuid,
		status:   status,
		shards:   make(map[proto.BlobID][]byte),
		crc32:    make(map[proto.BlobID]uint32),
		bidInfos: make(map[proto.BlobID]*client.ShardInfo),
	}
	return &m
}

func (m *mockVunit) putShard(bid proto.BlobID, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.shards[bid] = data
	m.crc32[bid] = crc32.ChecksumIEEE(data)
	info := client.ShardInfo{}
	info.Vuid = m.vuid
	info.Bid = bid
	info.Size = int64(len(data))
	info.Crc = m.crc32[bid]
	info.Flag = api.ShardStatusNormal
	m.bidInfos[bid] = &info
}

func (m *mockVunit) getShard(bid proto.BlobID) (data io.Reader, crc uint32, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.bidInfos[bid]; !ok {
		return nil, 0, errors.New("bid not exist")
	}
	if m.bidInfos[bid].Flag == api.ShardStatusMarkDelete {
		return nil, 0, errors.New("flag delete fake error")
	}

	return bytes.NewReader(m.shards[bid]), crc32.ChecksumIEEE(m.shards[bid]), nil
}

func (m *mockVunit) statShard(bid proto.BlobID) (si *client.ShardInfo, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.bidInfos[bid]; !ok {
		var info2 client.ShardInfo
		info2.Vuid = m.vuid
		info2.Bid = bid
		info2.Flag = client.ShardStatusNotExist
		return &info2, nil
	}
	return m.bidInfos[bid], nil
}

func (m *mockVunit) listShards() (shards []*client.ShardInfo, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, bid := range m.bidInfos {
		shards = append(shards, bid)
	}
	return shards, nil
}

func (m *mockVunit) getCrc32(bid proto.BlobID) uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.crc32[bid]
}

func (m *mockVunit) delete(bid proto.BlobID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.bidInfos, bid)
}

func (m *mockVunit) markDelete(bid proto.BlobID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bidInfos[bid].Flag = api.ShardStatusMarkDelete
}

func (m *mockVunit) recover(bid proto.BlobID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bidInfos[bid].Flag = api.ShardStatusNormal
}
