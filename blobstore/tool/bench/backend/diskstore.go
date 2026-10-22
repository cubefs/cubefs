package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/blobnode/core/disk"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	fixedDiskID = 1
)

type DiskChunkBackend struct {
	conf       core.Config
	ds         *disk.DiskStorageWrapper
	chunkCount int

	nextBid uint64
}

func fixEnv() error {
	return os.Setenv("JENKINS_TEST", "1")
}

func NewDiskChunkBackend(confPath string, chunkCount int, chunkSizeGiB int64) (ObjectStorage, error) {
	if err := fixEnv(); err != nil {
		return nil, err
	}

	ctx := context.TODO()
	confStr, err := os.ReadFile(confPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var conf core.Config
	if err := json.Unmarshal(confStr, &conf); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}

	if err := os.MkdirAll(conf.Path, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create disk path: %w", err)
	}
	conf.AllocDiskID = func(_ context.Context) (proto.DiskID, error) {
		return fixedDiskID, nil
	}
	conf.NotifyCompacting = func(ctx context.Context, args *cmapi.SetCompactChunkArgs) (err error) { return }
	conf.HandleIOError = func(ctx context.Context, diskID proto.DiskID, diskErr error) {}

	ds, err := disk.NewDiskStorage(ctx, conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create DiskStorage: %w", err)
	}

	if len(ds.Chunks) == 0 {
		chunks := make([]core.ChunkAPI, chunkCount)
		for i := 0; i < chunkCount; i++ {
			vuid := proto.Vuid(i)
			cs, err := ds.CreateChunk(context.Background(), vuid, (chunkSizeGiB << 30))
			if err != nil {
				for j := 0; j < i; j++ {
					if chunks[j] != nil {
						chunks[j].Close(ctx)
					}
				}
				ds.Close(ctx)
				return nil, fmt.Errorf("failed to create chunk %d (vuid=%d): %w", i, vuid, err)
			}
			chunks[i] = cs
		}
	}

	return &DiskChunkBackend{
		conf:       conf,
		ds:         ds,
		chunkCount: chunkCount,
		nextBid:    1,
	}, nil
}

func (b *DiskChunkBackend) nextBlobID() proto.BlobID {
	return proto.BlobID(atomic.AddUint64(&b.nextBid, 1))
}

func (b *DiskChunkBackend) selectChunk(bid proto.BlobID) core.ChunkAPI {
	idx := proto.Vuid(uint64(bid) % uint64(b.chunkCount))
	return b.ds.Chunks[idx]
}

func (b *DiskChunkBackend) PutObject(ctx context.Context, reader io.Reader, size int64) (loc LocInfo, err error) {
	bid := b.nextBlobID()
	cs := b.selectChunk(bid)

	shard := &core.Shard{
		Bid:  bid,
		Vuid: cs.Vuid(),
		Flag: bnapi.ShardStatusNormal,
		Size: uint32(size),
		Body: reader,
	}

	ctx = bnapi.SetIoType(ctx, bnapi.WriteIO)
	err = cs.Write(ctx, shard)
	if err != nil {
		return LocInfo{}, fmt.Errorf("write shard bid=%d failed: %w", bid, err)
	}

	if !b.conf.DisableSync {
		if err = cs.SyncData(ctx); err != nil {
			return LocInfo{}, fmt.Errorf("sync shard bid=%d failed: %w", bid, err)
		}
	}

	loc.Value = proto.Location{
		SliceSize: uint32(size),
		Slices: []proto.Slice{{
			MinSliceID: bid,
			Vid:        proto.Vid(cs.Vuid()),
			Count:      1,
		}},
	}
	return loc, nil
}

func (b *DiskChunkBackend) retriveChunk(vuid proto.Vuid) (core.ChunkAPI, error) {
	index := int(vuid)
	if index < 0 || index >= b.chunkCount {
		return nil, fmt.Errorf("invalid vuid %d", vuid)
	}
	return b.ds.Chunks[proto.Vuid(index)], nil
}

func (b *DiskChunkBackend) GetObject(ctx context.Context, loc LocInfo, writer io.Writer, size int64) error {
	location, err := loc.ExtractBlobLocation()
	if err != nil {
		return err
	}

	if len(location.Slices) == 0 {
		return fmt.Errorf("empty blobs in location")
	}
	slice := location.Slices[0]
	bid := proto.BlobID(slice.MinSliceID)
	vuid := proto.Vuid(slice.Vid)

	cs, err := b.retriveChunk(vuid)
	if err != nil {
		return err
	}

	shard := &core.Shard{
		Bid:    bid,
		Vuid:   vuid,
		Flag:   bnapi.ShardStatusNormal,
		Size:   uint32(size),
		Writer: writer,
	}

	ctx = bnapi.SetIoType(ctx, bnapi.ReadIO)
	n, err := cs.Read(ctx, shard)
	if err != nil {
		return fmt.Errorf("read shard bid=%d failed: %w", bid, err)
	}
	if n != size {
		return fmt.Errorf("short read: expected %d, got %d", size, n)
	}
	return nil
}

func (b *DiskChunkBackend) DelObject(ctx context.Context, loc LocInfo) error {
	location, err := loc.ExtractBlobLocation()
	if err != nil {
		return err
	}

	if len(location.Slices) == 0 {
		return fmt.Errorf("empty blobs in location")
	}
	slice := location.Slices[0]
	bid := proto.BlobID(slice.MinSliceID)
	vuid := proto.Vuid(slice.Vid)

	cs, err := b.retriveChunk(vuid)
	if err != nil {
		return err
	}

	ctx = bnapi.SetIoType(ctx, bnapi.DeleteIO)
	if err := cs.MarkDelete(ctx, bid); err != nil {
		return fmt.Errorf("mark delete bid=%d failed: %w", bid, err)
	}
	if err := cs.Delete(ctx, bid); err != nil {
		return fmt.Errorf("delete bid=%d failed: %w", bid, err)
	}
	return nil
}

func (b *DiskChunkBackend) Close() error {
	ctx := context.TODO()
	if b.ds != nil {
		b.ds.Close(ctx)
		b.ds = nil
	}

	return nil
}
