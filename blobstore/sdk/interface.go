package sdk

import (
	"context"
	"io"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/sdk/base"
)

type EbsClient interface {
	access.API
	CreateBlob(ctx context.Context, args *base.CreateBlobArgs) (proto.Blob, error)
	ListBlob(ctx context.Context, args *base.ListBlobArgs) (base.ListBlobResponse, error)
	StatBlob(ctx context.Context, args *base.StatBlobArgs) (proto.Blob, error)
	SealBlob(ctx context.Context, args *base.SealBlobArgs) error
	GetBlob(ctx context.Context, args *base.GetBlobArgs) (io.ReadCloser, error)
	DeleteBlob(ctx context.Context, args *base.DelBlobArgs) error
	PutBlob(ctx context.Context, args *base.PutBlobArgs) (proto.ClusterID, error)
}
