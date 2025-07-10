package backend

import (
	"context"
	"io"
)

// DummyStore always do nothing and return success.
// Just used to measure lateny of bench tool itself.
type DummyStore struct{}

func NewDummyStore() (ObjectStorage, error) {
	return &DummyStore{}, nil
}

func (d *DummyStore) PutObject(ctx context.Context, key string, reader io.Reader, size int64) (location LocInfo, err error) {
	return LocInfo{}, nil
}

func (d *DummyStore) GetObject(ctx context.Context, location LocInfo, writer io.Writer, size int64) error {
	return nil
}

func (d *DummyStore) DelObject(ctx context.Context, location LocInfo) error {
	return nil
}
func (d *DummyStore) Close() {}
