package backend

import (
	"context"
	"fmt"
	"io"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

// LocInfo is a wrapper for rados key and blobstore location.
type LocInfo struct {
	Value interface{}
}

// ExtractBlobLocation extracts a blobstore location from the LocInfo instance.
func (loc *LocInfo) ExtractBlobLocation() (proto.Location, error) {
	if loc == nil || loc.Value == nil {
		return proto.Location{}, fmt.Errorf("loc is nil")
	}

	switch v := loc.Value.(type) {
	case proto.Location:
		return v, nil
	default:
		return proto.Location{}, fmt.Errorf("loc type error: %T", v)
	}
}

// ExtractRadosKey extracts a rados key from the LocInfo instance.
func (loc *LocInfo) ExtractRadosKey() (string, error) {
	if loc == nil || loc.Value == nil {
		return "", fmt.Errorf("loc is nil")
	}

	switch v := loc.Value.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("loc type error: %T", v)
	}
}

// Defines bench storage backend operations
// size - is used to optimize buffer management ops
type ObjectStorage interface {
	PutObject(ctx context.Context, reader io.Reader, size int64) (location LocInfo, err error)
	GetObject(ctx context.Context, location LocInfo, writer io.Writer, size int64) error
	DelObject(ctx context.Context, location LocInfo) error
	// do cleanup work
	Close() error
}

// dummyStore always do nothing and return success.
// Just used to measure lateny of bench tool itself.
type dummyStore struct{}

func NewDummyStorage() (ObjectStorage, error) {
	return dummyStore{}, nil
}

func (dummyStore) PutObject(context.Context, io.Reader, int64) (loc LocInfo, err error) { return }
func (dummyStore) GetObject(context.Context, LocInfo, io.Writer, int64) error           { return nil }
func (dummyStore) DelObject(context.Context, LocInfo) error                             { return nil }
func (dummyStore) Close() error                                                         { return nil }
