package storage

import (
	"context"
	"io"
)

type Storage interface {
	Put(ctx context.Context, reqId, key string, r io.Reader, length int64) (err error)
	Get(ctx context.Context, reqId, key string, from, to int64) (r io.ReadCloser, length int64, shouldCache bool, err error)
	Name() string
}
