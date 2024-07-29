package base

import (
	"io"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type CreateBlobArgs struct {
	BlobName []byte
	CodeMode codemode.CodeMode
}

type ListBlobArgs struct {
	Prefix string
	Marker string
	Count  int
}

type ListBlobResponse struct {
	Blobs      []proto.Blob
	NextMarker string
}

type StatBlobArgs struct {
	BlobName  []byte
	ClusterID proto.ClusterID
}

type SealBlobArgs struct {
	BlobName  []byte
	ClusterID proto.ClusterID
	BlobSize  uint64
}

type GetBlobArgs struct {
	BlobName  []byte
	ClusterID proto.ClusterID
	Offset    uint64
	ReadSize  uint64
	Writer    io.Writer
}

type DelBlobArgs struct {
	BlobName  []byte
	ClusterID proto.ClusterID
}

type PutBlobArgs struct {
	BlobName []byte
	CodeMode codemode.CodeMode
	NeedSeal bool

	Size uint64
	Body io.Reader
}
