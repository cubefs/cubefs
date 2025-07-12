package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/sdk"
)

// Blobstore implementation of ObjectStorage interface
type BlobStorage struct {
	client access.API
}

func NewBlobStorage(confPath string) (ObjectStorage, error) {
	confStr, err := os.ReadFile(confPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var conf sdk.Config
	if err := json.Unmarshal(confStr, &conf); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}

	client, err := sdk.New(&conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob client: %v", err)
	}

	return &BlobStorage{
		client: client,
	}, nil
}

func (b *BlobStorage) PutObject(ctx context.Context, key string, data io.Reader, size int64) (loc LocInfo, err error) {
	args := &access.PutArgs{
		Size:   size,
		Hashes: access.HashAlgDummy,
		Body:   data,
	}

	loc.Value, _, err = b.client.Put(ctx, args)
	if err != nil {
		return LocInfo{}, fmt.Errorf("failed to put object to blob: %v", err)
	}

	return loc, nil
}

func (b *BlobStorage) GetObject(ctx context.Context, loc LocInfo, writer io.Writer, size int64) error {
	location, err := loc.ExtractBlobLocation()
	if err != nil {
		return err
	}

	// Download object from BlobStore
	args := &access.GetArgs{
		Location: location,
		Offset:   0,
		ReadSize: uint64(size),
	}
	rc, err := b.client.Get(ctx, args)
	if err != nil {
		return fmt.Errorf("failed to get object from blob: %v", err)
	}
	defer rc.Close()

	// Write data to the provided io.Writer
	_, err = io.Copy(writer, rc)
	if err != nil {
		return fmt.Errorf("failed to write data to writer: %v", err)
	}

	return nil
}

func (b *BlobStorage) DelObject(ctx context.Context, loc LocInfo) error {
	location, err := loc.ExtractBlobLocation()
	if err != nil {
		return err
	}

	args := &access.DeleteArgs{
		Locations: []proto.Location{location},
	}
	_, err = b.client.Delete(ctx, args)

	return err
}

func (b *BlobStorage) Close() {
}
