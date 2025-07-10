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
type blobStorage struct {
	client access.API
}

func NewBlobStorage(confPath string) (ObjectStorage, error) {
	confStr, err := os.ReadFile(confPath)
	if err != nil {
		return nil, fmt.Errorf("read config file: %v", err)
	}

	var conf sdk.Config
	if err := json.Unmarshal(confStr, &conf); err != nil {
		return nil, fmt.Errorf("parse config: %v", err)
	}

	client, err := sdk.New(&conf)
	if err != nil {
		return nil, fmt.Errorf("create blob client: %v", err)
	}
	return &blobStorage{client: client}, nil
}

func (b *blobStorage) PutObject(ctx context.Context, data io.Reader, size int64) (loc LocInfo, err error) {
	loc.Value, _, err = b.client.Put(ctx, &access.PutArgs{Size: size, Body: data})
	if err != nil {
		return LocInfo{}, fmt.Errorf("put object to blob: %v", err)
	}
	return loc, nil
}

func (b *blobStorage) GetObject(ctx context.Context, loc LocInfo, writer io.Writer, size int64) error {
	location, err := loc.ExtractBlobLocation()
	if err != nil {
		return err
	}
	rc, err := b.client.Get(ctx, &access.GetArgs{Location: location, ReadSize: uint64(size)})
	if err != nil {
		return fmt.Errorf("get object from blob: %v", err)
	}
	defer rc.Close()

	_, err = io.Copy(writer, rc)
	if err != nil {
		return fmt.Errorf("write data to writer: %v", err)
	}
	return nil
}

func (b *blobStorage) DelObject(ctx context.Context, loc LocInfo) error {
	location, err := loc.ExtractBlobLocation()
	if err != nil {
		return err
	}
	_, err = b.client.Delete(ctx, &access.DeleteArgs{Locations: []proto.Location{location}})
	return err
}

func (b *blobStorage) Close() error {
	return nil
}
