package blobnode

import (
	"errors"

	"github.com/golang/mock/gomock"
)

//go:generate mockgen -destination=./disk_mock_test.go -package=blobnode -mock_names DiskAPI=MockDiskAPI,Storage=MockStorage,ChunkAPI=MockChunkAPI github.com/cubefs/cubefs/blobstore/blobnode/core DiskAPI,Storage,ChunkAPI

var (
	any     = gomock.Any()
	errMock = errors.New("fake error")
)
