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
	"crypto/rand"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestAccessStreamGetBase(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamGetBase")
	// error
	{
		dataShards.clean()
		data := []byte("x")
		loc, err := streamer.Put(ctx(), bytes.NewReader(data), int64(len(data)), nil)
		require.NoError(t, err)

		buff := bytes.NewBuffer(nil)
		_, err = streamer.Get(ctx(), buff, *loc, 1, 1)
		require.NotNil(t, err)
		_, err = streamer.Get(ctx(), buff, *loc, 2, 0)
		require.NotNil(t, err)
	}
	// 1 byte
	{
		dataShards.clean()
		data := []byte("x")
		loc, err := streamer.Put(ctx(), bytes.NewReader(data), int64(len(data)), nil)
		require.NoError(t, err)

		buff := bytes.NewBuffer(nil)
		transfer, err := streamer.Get(ctx(), buff, *loc, 1, 0)
		require.NoError(t, err)
		err = transfer()
		require.NoError(t, err)
		require.True(t, dataEqual(data, buff.Bytes()))

		_, err = streamer.Get(ctx(), nil, *loc, 0, 1)
		require.NoError(t, err)
	}

	vuidController.SetBNRealError(true)
	defer func() {
		vuidController.SetBNRealError(false)
	}()

	cases := []struct {
		size int
	}{
		{12},
		{1 << 12},
		{(1 << 13) + 777},
		{1 << 22},
		{(1 << 22) + 1},
		{(1 << 22) + 1023},
		{1 << 23},
		{(1 << 23) + 1025},
	}
	for _, cs := range cases {
		dataShards.clean()
		size := cs.size
		data := make([]byte, size)
		rand.Read(data)
		loc, err := streamer.Put(ctx(), bytes.NewReader(data), int64(size), nil)
		require.NoError(t, err)

		buff := bytes.NewBuffer(nil)
		transfer, err := streamer.Get(ctx(), buff, *loc, uint64(size), 0)
		require.NoError(t, err)
		err = transfer()
		require.NoError(t, err)
		require.True(t, dataEqual(data, buff.Bytes()))

		// time wait the punished services
		time.Sleep(time.Second * time.Duration(punishServiceS))
	}

	dataShards.clean()
}

func TestAccessStreamGetBroken(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamGetBroken")
	defer func() {
		dataShards.clean()
		for ii := 0; ii < len(allID); ii++ {
			vuidController.Unbreak(proto.Vuid(allID[ii]))
		}
		vuidController.Break(1005)
	}()

	dataShards.clean()
	tactic := codemode.EC6P6.Tactic()
	size := tactic.N * tactic.MinShardSize
	data := make([]byte, size)
	rand.Read(data)
	// time wait the punished services
	time.Sleep(time.Second * time.Duration(punishServiceS))
	loc, err := streamer.Put(ctx(), bytes.NewReader(data), int64(size), nil)
	require.NoError(t, err)

	cases := []struct {
		id       proto.Vuid
		hasError bool
	}{
		{1001, false},
		{1002, false},
		{1003, false},
		{1004, false},
		{1005, false},
		{1006, false},
		{1007, true},
		{1008, true},
	}

	for _, cs := range cases {
		vuidController.Break(cs.id)

		buff := bytes.NewBuffer(nil)
		transfer, err := streamer.Get(ctx(), buff, *loc, uint64(size), 0)
		require.Nil(t, err)
		err = transfer()
		if cs.hasError {
			require.NotNil(t, err)
		} else {
			require.NoError(t, err)
			require.True(t, dataEqual(data, buff.Bytes()))
		}
	}
}

func TestAccessStreamGetOffset(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamGetOffset")
	vuidController.Unbreak(1005)
	defer vuidController.Break(1005)

	cases := []struct {
		size     int64
		offset   uint64
		readSize uint64
	}{
		{12, 0, 12},
		{12, 1, 11},
		{12, 1, 1},
		{12, 1, 4},
		{12, 1, 7},
		{(1 << 22) + 1, 0, 1 << 22},
		{(1 << 22) + 1, 1 << 20, 1 << 20},
		{(1 << 22) + 1, 1 << 22, 1},
		{(1 << 22) + 1023, 0, 1},
		{(1 << 22) + 1023, 1 << 22, 1022},
		{(1 << 22) + 1023, 1 << 22, 1023},
		{12192823, 6799138, 908019},
		// segment ec
		{(1 << 22) + 1, 0, (1 << 22) + 1},
		{(1 << 22) + 1, (1 << 22) - 1, 2},
		{(1 << 22) + 1, (1 << 22) - 100, 101},
		{(1 << 22) + 1024, (1 << 22) - 1024, 2048},
	}
	for _, cs := range cases {
		dataShards.clean()
		size := cs.size
		data := make([]byte, size)
		rand.Read(data)
		loc, err := streamer.Put(ctx(), bytes.NewReader(data), size, nil)
		require.NoError(t, err)

		buff := bytes.NewBuffer(nil)
		transfer, _ := streamer.Get(ctx(), buff, *loc, cs.readSize, cs.offset)
		err = transfer()
		require.NoError(t, err)
		require.True(t, dataEqual(data[cs.offset:cs.offset+cs.readSize], buff.Bytes()))
	}

	dataShards.clean()
}

func TestAccessStreamGetShardTimeout(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamGetShardTimeout")
	dataShards.clean()
	vuidController.Unbreak(1005)
	streamer.MinReadShardsX = defaultMinReadShardsX
	defer func() {
		vuidController.Break(1005)
		streamer.MinReadShardsX = minReadShardsX
		dataShards.clean()
	}()

	size := 1 << 22
	buff := make([]byte, size)
	rand.Read(buff)
	loc, err := streamer.Put(ctx(), bytes.NewReader(buff), int64(size), nil)
	require.NoError(t, err)

	// no delay when blocking one shard, cos MinReadShardsX = 1
	vuidController.Block(1001)
	defer func() {
		vuidController.Unblock(1001)
	}()
	{
		startTime := time.Now()
		transfer, _ := streamer.Get(ctx(), bytes.NewBuffer(nil), *loc, uint64(size), 0)
		err := transfer()
		require.NoError(t, err)

		duration := time.Since(startTime)
		require.GreaterOrEqual(t, vuidController.duration, duration, "greater duration: ", duration)
	}

	// delay one duration when blocking two shard, cos MinReadShardsX = 1
	vuidController.Block(1002)
	defer func() {
		vuidController.Unblock(1002)
	}()
	{
		startTime := time.Now()
		transfer, _ := streamer.Get(ctx(), bytes.NewBuffer(nil), *loc, uint64(size), 0)
		err = transfer()
		require.NoError(t, err)

		duration := time.Since(startTime)
		minDuration := vuidController.duration
		maxDuration := minDuration + minDuration/2
		t.Log(duration, minDuration, maxDuration)
		require.LessOrEqual(t, minDuration, duration, "less duration: ", duration)
		require.GreaterOrEqual(t, maxDuration, duration, "greater duration: ", duration)
	}
}

func TestAccessStreamGetShardBroken(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamGetShardBroken")
	dataShards.clean()
	vuidController.Unbreak(1005)
	streamer.MinReadShardsX = defaultMinReadShardsX
	defer func() {
		vuidController.Break(1005)
		streamer.MinReadShardsX = minReadShardsX
		dataShards.clean()
	}()

	size := 1 << 22
	buff := make([]byte, size)
	rand.Read(buff)
	loc, err := streamer.Put(ctx(), bytes.NewReader(buff), int64(size), nil)
	require.NoError(t, err)

	// no delay when blocking one shard, cos MinReadShardsX = 1
	// it will not wait the blocking shard, cos has no enough shards to reconstruct
	vuidController.Block(1001)
	for _, id := range allID[1:] {
		vuidController.Break(proto.Vuid(id))
	}
	defer func() {
		vuidController.Unblock(1001)
		for _, id := range allID[1:] {
			vuidController.Unbreak(proto.Vuid(id))
		}
	}()
	{
		startTime := time.Now()
		transfer, err := streamer.Get(ctx(), bytes.NewBuffer(nil), *loc, uint64(size), 0)
		require.NoError(t, err)
		err = transfer()
		require.Error(t, err)

		duration := time.Since(startTime)
		retryWait := time.Millisecond * (200 + 400) // retry.ExponentialBackoff(attempts, 200)
		require.GreaterOrEqual(t, vuidController.duration+retryWait, duration, "greater duration:", duration)
	}
}

func TestAccessStreamGetLocalIDC(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamGetLocalIDC")
	dataShards.clean()
	vuidController.Unbreak(1005)
	defer func() {
		vuidController.Break(1005)
		dataShards.clean()
	}()

	size := 1 << 22
	buff := make([]byte, size)
	rand.Read(buff)
	loc, err := streamer.Put(ctx(), bytes.NewReader(buff), int64(size), nil)
	require.NoError(t, err)

	// no delay when blocking other idc all shards
	func() {
		for _, id := range idcOtherID {
			vuidController.Block(proto.Vuid(id))
		}
		defer func() {
			for _, id := range idcOtherID {
				vuidController.Unblock(proto.Vuid(id))
			}
		}()
		startTime := time.Now()
		transfer, _ := streamer.Get(ctx(), bytes.NewBuffer(nil), *loc, uint64(size), 0)
		err = transfer()
		require.NoError(t, err)

		duration := time.Since(startTime)
		require.GreaterOrEqual(t, vuidController.duration, duration, "greater duration: ", duration)
	}()

	// max delay one duration when other local has two blocked shard, cos MinReadShardsX = 1
	vuidController.Break(proto.Vuid(idcID[0]))
	defer func() {
		vuidController.Unbreak(proto.Vuid(idcID[0]))
	}()
	func() {
		vuidController.Block(proto.Vuid(idcOtherID[0]))
		vuidController.Block(proto.Vuid(idcOtherID[1]))
		defer func() {
			vuidController.Unblock(proto.Vuid(idcOtherID[0]))
			vuidController.Unblock(proto.Vuid(idcOtherID[1]))
		}()
		startTime := time.Now()
		transfer, _ := streamer.Get(ctx(), bytes.NewBuffer(nil), *loc, uint64(size), 0)
		err = transfer()
		require.NoError(t, err)

		duration := time.Since(startTime)
		if duration < vuidController.duration {
			return
		}
		minDuration := vuidController.duration
		maxDuration := minDuration + minDuration/2
		t.Log(duration, minDuration, maxDuration)
		require.LessOrEqual(t, minDuration, duration, "less duration: ", duration)
		require.GreaterOrEqual(t, maxDuration, duration, "greater duration: ", duration)
	}()
}

func TestAccessStreamGetAligned(t *testing.T) {
	ctx := ctxWithName("TestAccessStreamGetAligned")
	defer func() {
		dataShards.clean()
		for ii := 0; ii < len(allID); ii++ {
			vuidController.Unbreak(proto.Vuid(allID[ii]))
		}
		vuidController.Break(1005)
	}()
	vuidController.Unbreak(1005)

	shardSize := codemode.EC6P6.Tactic().MinShardSize
	cases := []struct {
		size       int
		goodShards int
		succ       bool
	}{
		{1, 0, false},
		{1, 1, true},
		{1, 2, true},

		{shardSize - 1, 1, true},
		{shardSize, 1, true},
		{shardSize + 1, 1, false},
		{shardSize + 1, 2, true},

		{shardSize * 2, 1, false},
		{shardSize * 2, 2, true},
		{shardSize * 3, 2, false},
		{shardSize * 3, 3, true},
		{shardSize * 4, 3, false},
		{shardSize * 4, 4, true},
		{shardSize * 5, 4, false},
		{shardSize * 5, 5, true},
		{shardSize * 6, 5, false},
		{shardSize * 6, 6, true},
	}

	randomGoodShards := func(n int) {
		for ii := 0; ii < len(allID); ii++ {
			vuidController.Break(proto.Vuid(allID[ii]))
		}

		shards := make([]int, 0, len(allID))
		shards = append(shards, allID[6:]...)
		shards = append(shards, allID[:n]...)

		mrand.Shuffle(len(shards), func(i, j int) {
			shards[i], shards[j] = shards[j], shards[i]
		})
		for ii := 0; ii < n; ii++ {
			vuidController.Unbreak(proto.Vuid(shards[ii]))
		}
	}

	for _, cs := range cases {
		dataShards.clean()

		data := make([]byte, cs.size)
		rand.Read(data)
		loc, err := streamer.Put(ctx(), bytes.NewReader(data), int64(cs.size), nil)
		require.NoError(t, err)

		// cos put shards asynchronously, should wait all shard written
		time.Sleep(20 * time.Millisecond)

		randomGoodShards(cs.goodShards)

		buff := bytes.NewBuffer(nil)
		transfer, err := streamer.Get(ctx(), buff, *loc, uint64(cs.size), 0)
		require.NoError(t, err)

		err = transfer()
		if cs.succ {
			require.NoError(t, err)
			require.True(t, dataEqual(data, buff.Bytes()))
		} else {
			require.Error(t, err)
		}

		for ii := 0; ii < len(allID); ii++ {
			vuidController.Unbreak(proto.Vuid(allID[ii]))
		}
	}
}

func TestAccessStreamGenLocationBlobs(t *testing.T) {
	firstSliceStart := proto.BlobID(100)
	secondSliceStart := proto.BlobID(200)

	loc := access.Location{
		ClusterID: 0,
		CodeMode:  codemode.EC6P6,
		Size:      1024*4 + 37 + 1024*2, // 5 fine blobs and 2 missing blobs
		BlobSize:  1024,
		Blobs: []access.SliceInfo{
			{
				MinBid: firstSliceStart,
				Vid:    proto.Vid(1001),
				Count:  3,
			},
			{
				MinBid: secondSliceStart,
				Vid:    proto.Vid(2001),
				Count:  2,
			},
		},
	}

	type blobArgs = blobGetArgs
	cases := []struct {
		readSize, offset uint64
		err              bool
		checker          func([]blobArgs) bool
	}{
		{1024 * 7, 0, true, func(blobs []blobArgs) bool { return blobs == nil }},
		{1024 * 6, 38, true, func(blobs []blobArgs) bool { return blobs == nil }},
		{1024 * 5, 38, true, func(blobs []blobArgs) bool { return blobs == nil }},

		{0, 0, false, func(blobs []blobArgs) bool {
			return len(blobs) == 0
		}},
		{1, 0, false, func(blobs []blobArgs) bool {
			return len(blobs) == 1 && blobs[0].Bid == firstSliceStart
		}},
		{1024, 0, false, func(blobs []blobArgs) bool {
			return len(blobs) == 1 && blobs[0].Bid == firstSliceStart
		}},
		{1025, 0, false, func(blobs []blobArgs) bool {
			return len(blobs) == 2 && blobs[1].Bid == firstSliceStart+1
		}},
		{1024*4 + 37, 0, false, func(blobs []blobArgs) bool {
			return len(blobs) == 5
		}},

		{0, 1024, false, func(blobs []blobArgs) bool {
			return len(blobs) == 0
		}},
		{1, 1023, false, func(blobs []blobArgs) bool {
			return len(blobs) == 1 && blobs[0].Bid == firstSliceStart
		}},
		{1, 1024, false, func(blobs []blobArgs) bool {
			return len(blobs) == 1 && blobs[0].Bid == firstSliceStart+1
		}},
		{1024, 1024, false, func(blobs []blobArgs) bool {
			return len(blobs) == 1 && blobs[0].Bid == firstSliceStart+1
		}},
		{1024 * 2, 1024 + 1, false, func(blobs []blobArgs) bool {
			return len(blobs) == 3 && blobs[0].Bid == firstSliceStart+1 &&
				blobs[0].Offset == 1 && blobs[0].ReadSize == 1023 &&
				blobs[1].Offset == 0 && blobs[1].ReadSize == 1024 &&
				blobs[2].Offset == 0 && blobs[2].ReadSize == 1
		}},

		{1, 1024 * 4, false, func(blobs []blobArgs) bool {
			return len(blobs) == 1 && blobs[0].Bid == secondSliceStart+1 &&
				blobs[0].Offset == 0 && blobs[0].ReadSize == 1
		}},
		{1, 1024*4 + 1, false, func(blobs []blobArgs) bool {
			return len(blobs) == 1 && blobs[0].Bid == secondSliceStart+1 &&
				blobs[0].Offset == 1 && blobs[0].ReadSize == 1
		}},
		{36, 1024*4 + 1, false, func(blobs []blobArgs) bool {
			return len(blobs) == 1 && blobs[0].Bid == secondSliceStart+1 &&
				blobs[0].Offset == 1 && blobs[0].ReadSize == 36
		}},
	}

	for _, cs := range cases {
		blobs, err := genLocationBlobs(&loc, cs.readSize, cs.offset)
		if cs.err {
			require.True(t, err != nil)
		} else {
			require.True(t, err == nil)
		}
		require.True(t, cs.checker(blobs))
	}
}

func TestAccessStreamShardSegment(t *testing.T) {
	shardSize := 2333
	for _, cs := range []struct {
		offset, readSize           int
		shardOffset, shardReadSize int
	}{
		{0, 0, 0, 0},
		{0, 1, 0, 1},
		{100, 233, 100, 233},
		{shardSize - 1, 1, shardSize - 1, 1},
		{shardSize*10 - 1, 1, shardSize - 1, 1},
		{shardSize*10 + 1, 1, 1, 1},
		{shardSize*10 + 100, 233, 100, 233},
		{shardSize - 1, 2, 0, shardSize},
		{1, shardSize, 0, shardSize},
		{shardSize, shardSize + 10, 0, shardSize},
		{1, shardSize * 2, 0, shardSize},
		{shardSize + 1, shardSize * 100, 0, shardSize},
	} {
		shardOffset, shardReadSize := shardSegment(shardSize, cs.offset, cs.readSize)
		require.Equal(t, cs.shardOffset, shardOffset)
		require.Equal(t, cs.shardReadSize, shardReadSize)
	}
}

type writer struct {
	buf []byte
}

func (w *writer) Write(p []byte) (n int, err error) {
	copy(w.buf, p)
	return len(p), nil
}

func BenchmarkAccessStreamGet(b *testing.B) {
	ctx := ctxWithName("BenchmarkAccessStreamGet")()
	vuidController.Unbreak(1005)
	defer func() {
		vuidController.Break(1005)
		dataShards.clean()
	}()

	cases := []struct {
		name string
		size int
	}{
		{"1B", 1},
		{"1KB", 1 << 10},
		{"1MB", 1 << 20},
		{"4MB", 1 << 22},
		{"8MB", 1 << 23},
	}

	w := &writer{buf: make([]byte, 1<<23)}
	for _, cs := range cases {
		b.ResetTimer()
		b.Run(cs.name, func(b *testing.B) {
			loc, err := streamer.Put(ctx, newReader(cs.size), int64(cs.size), nil)
			require.NoError(b, err)

			b.ResetTimer()
			for ii := 0; ii <= b.N; ii++ {
				transfer, _ := streamer.Get(ctx, w, *loc, uint64(cs.size), 0)
				transfer()
			}
		})
	}
}
