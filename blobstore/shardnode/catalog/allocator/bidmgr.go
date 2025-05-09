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

package allocator

import (
	"context"
	"sync"

	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
)

const defaultBidAllocNums = 10000

type BlobConfig struct {
	BidAllocNums uint64 `json:"bid_alloc_nums"`
}

type bidRange struct {
	startBid proto.BlobID
	endBid   proto.BlobID
}

type allocatableBids struct {
	minBid proto.BlobID
	maxBid proto.BlobID
}

type bm struct {
	BlobConfig
	mu *sync.RWMutex

	current   *allocatableBids
	backup    *allocatableBids
	transport base.Transport
	allocCh   chan struct{}
}

func confCheck(cfg *BlobConfig) {
	if cfg.BidAllocNums < defaultBidAllocNums {
		cfg.BidAllocNums = defaultBidAllocNums
	}
}

// BidMgr Assume the task of assigning bid segments
type bidMgr interface {
	alloc(ctx context.Context, count uint64) (bidRange []bidRange, err error)
}

func newBidMgr(ctx context.Context, cfg BlobConfig, tp base.Transport) (bidMgr, error) {
	confCheck(&cfg)
	b := &bm{
		transport:  tp,
		BlobConfig: cfg,
		mu:         &sync.RWMutex{},
		allocCh:    make(chan struct{}),
	}
	err := b.allocBid(ctx)
	if err != nil {
		return b, err
	}

	go b.allocBidLoop()

	return b, nil
}

func (b *bm) allocBidLoop() {
	for range b.allocCh {
		span, ctx := trace.StartSpanFromContext(context.Background(), "")
		err := b.allocBid(ctx)
		if err != nil {
			span.Errorf("alloc bid from cm error: %v", err)
		}
	}
}

func (b *bm) allocBid(ctx context.Context) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	var startBid proto.BlobID
	for try := 0; try < 3; try++ {
		startBid, err = b.transport.AllocBid(ctx, b.BidAllocNums)
		if err == nil {
			break
		}
		span.Errorf("alloc bid scope from transport error: %v", err)
	}
	if err != nil {
		return
	}

	endBid := startBid + proto.BlobID(b.BidAllocNums) - 1
	span.Debugf("bid scope from clustermgr: [%v:%v]", startBid, endBid)
	scope := &allocatableBids{startBid, endBid}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.current != nil {
		b.backup = scope
		return
	}
	b.current = scope
	b.backup = nil
	return
}

func (b *bm) alloc(ctx context.Context, count uint64) (bidRg []bidRange, err error) {
	span := trace.SpanFromContextSafe(ctx)
	bidRg = make([]bidRange, 0)

	b.mu.Lock()
	defer func() {
		if b.backup == nil {
			select {
			case b.allocCh <- struct{}{}:
			default:
			}
		}
		b.mu.Unlock()
	}()

	span.Debugf("need bid: %v,current bidScope: %v,backup bidScope: %v", count, b.current, b.backup)
	if count > b.BidAllocNums {
		return nil, errcode.ErrIllegalArguments
	}
	if b.current == nil {
		return nil, errcode.ErrAllocBidFromCm
	}
	if count+uint64(b.current.minBid)-1 <= uint64(b.current.maxBid) {
		br := bidRange{
			startBid: b.current.minBid,
			endBid:   proto.BlobID(uint64(b.current.minBid) + count - 1),
		}
		b.current.minBid += proto.BlobID(count)
		currentCount := uint64(b.current.maxBid - b.current.minBid + 1)
		if currentCount == 0 {
			b.current = b.backup
			b.backup = nil
		}
		bidRg = append(bidRg, br)
		span.Debugf("after alloc, current bidScope: %v,backup bidScope: %v", b.current, b.backup)
		return
	}
	br1 := bidRange{
		startBid: b.current.minBid,
		endBid:   b.current.maxBid,
	}
	bidRg = append(bidRg, br1)
	bidCountRemain := count - uint64(b.current.maxBid-b.current.minBid+1)
	if b.backup == nil {
		return nil, errcode.ErrAllocBidFromCm
	}
	b.current = b.backup
	b.backup = nil
	br2 := bidRange{
		startBid: b.current.minBid,
		endBid:   proto.BlobID(uint64(b.current.minBid) + bidCountRemain - 1),
	}
	bidRg = append(bidRg, br2)
	b.current.minBid += proto.BlobID(bidCountRemain)
	span.Debugf("after alloc, current bidRange: %v,backup bidRange: %v", b.current, b.backup)
	return
}
