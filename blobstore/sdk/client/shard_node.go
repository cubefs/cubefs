package client

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type Config struct {
	rpc.Config
}

type client struct {
	rpc.Client
}

func New(cfg *Config) ShardNodeAPI {
	return &client{rpc.NewClient(&cfg.Config)}
}

type ShardNodeAPI interface {
	CreateBlob(ctx context.Context, host string, args *shardnode.CreateBlobArgs) (*shardnode.CreateBlobRet, error)
	ListBlob(ctx context.Context, host string, args *shardnode.ListBlobArgs) (*shardnode.ListBlobRet, error)
	GetBlob(ctx context.Context, host string, args *shardnode.GetBlobArgs) (*shardnode.GetBlobRet, error)
	DeleteBlob(ctx context.Context, host string, args *shardnode.DeleteBlobArgs) error
	SealBlob(ctx context.Context, host string, args *shardnode.SealBlobArgs) error
}

func (c *client) CreateBlob(ctx context.Context, host string, args *shardnode.CreateBlobArgs) (*shardnode.CreateBlobRet, error) {
	return nil, nil
}

func (c *client) ListBlob(ctx context.Context, host string, args *shardnode.ListBlobArgs) (*shardnode.ListBlobRet, error) {
	return nil, nil
}

func (c *client) GetBlob(ctx context.Context, host string, args *shardnode.GetBlobArgs) (ret *shardnode.GetBlobRet, err error) {
	// urlStr := fmt.Sprintf("%s/blob/get", host)
	// ret = &shardnode.GetBlobResponse{}
	// err = c.PostWith(ctx, urlStr, ret, args)
	return
}

func (c *client) DeleteBlob(ctx context.Context, host string, args *shardnode.DeleteBlobArgs) error {
	return nil
}

func (c *client) SealBlob(ctx context.Context, host string, args *shardnode.SealBlobArgs) error {
	return nil
}
