package sdk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access/stream"
	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/security"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var (
	gAny    = gomock.Any()
	errMock = errors.New("fake error")
)

func newSdkHandler(t *testing.T) *sdkHandler {
	ctr := gomock.NewController(t)
	h := mocks.NewMockStreamHandler(ctr)
	l := stream.NewLimiter(stream.LimitConfig{
		NameRps: map[string]int{"alloc": 2},
	})

	conf := Config{LogLevel: log.Lpanic}
	fixConfig(&conf)
	return &sdkHandler{
		handler: h,
		limiter: l,
		conf:    conf,
		closer:  closer.New(),
	}
}

func TestSdkBlobstore_New(t *testing.T) {
	conf := &Config{}
	conf.IDC = "xx"
	_, err := New(conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "consul can not be empty")

	conf.ClusterConfig.ConsulAgentAddr = "xxx"
	_, err = New(conf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cluster")
}

func TestSdkHandler_Delete(t *testing.T) {
	ctx := context.Background()
	hd := newSdkHandler(t)
	_, err := hd.Delete(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	_, err = hd.Delete(ctx, &acapi.DeleteArgs{})
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	args := &acapi.DeleteArgs{Locations: []proto.Location{{}}}
	ret, err := hd.Delete(ctx, args)
	require.NoError(t, err)
	require.Nil(t, ret)

	args.Locations[0].Size_ = 1
	_, err = hd.Delete(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	// retry 3 time
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(gAny, gAny).Times(3).Return(errMock)
	crc, _ := security.LocationCrcCalculate(&args.Locations[0])
	args.Locations[0].Crc = crc
	args.Locations[0].Slices = make([]proto.Slice, 0)
	ret, err = hd.Delete(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrUnexpected)
	require.Equal(t, args.Locations, ret)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(gAny, gAny).Return(nil)
	ret, err = hd.Delete(ctx, args)
	require.NoError(t, err)
	require.Nil(t, ret)

	// 3 location
	loc := proto.Location{
		ClusterID: 1,
		Size_:     1,
		Slices:    []proto.Slice{{Vid: 9}},
	}
	crc, _ = security.LocationCrcCalculate(&loc)
	loc.Crc = crc
	args.Locations = make([]proto.Location, 0)
	for len(args.Locations) < 3 {
		args.Locations = append(args.Locations, loc)
	}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(gAny, gAny).Return(nil)
	ret, err = hd.Delete(ctx, args)
	require.NoError(t, err)
	require.Nil(t, ret)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(gAny, gAny).Return(errMock).Times(3) // retry
	ret, err = hd.Delete(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrUnexpected)
	require.Equal(t, args.Locations, ret)
}

func TestSdkHandler_Get(t *testing.T) {
	ctx := context.Background()
	hd := newSdkHandler(t)

	_, err := hd.Get(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	// get size 0
	ret, err := hd.Get(ctx, &acapi.GetArgs{})
	require.NoError(t, err)
	retBuf := make([]byte, 2)
	n, _ := ret.Read(retBuf)
	require.Equal(t, 0, n)

	// args error
	args := &acapi.GetArgs{
		ReadSize: 1,
		Location: proto.Location{
			Size_: 2,
		},
	}
	_, err = hd.Get(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	// stream get error, get nil
	crc, _ := security.LocationCrcCalculate(&args.Location)
	args.Location.Crc = crc
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Get(gAny, gAny, gAny, gAny, gAny).Return(nil, errMock)
	args.ReadSize = 2
	ret, err = hd.Get(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)
	n, _ = ret.Read(retBuf)
	require.Equal(t, 0, n)

	// stream get error, do transfer error
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Get(gAny, gAny, gAny, gAny, gAny).Return(func() error { return errMock }, nil)
	ret, err = hd.Get(ctx, args)
	require.NoError(t, err)
	n, err = ret.Read(retBuf)
	require.NotNil(t, err)
	require.Equal(t, 0, n)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().Get(gAny, gAny, gAny, gAny, gAny).Return(func() error { return nil }, nil)
	ret, err = hd.Get(ctx, args)
	require.NoError(t, err)
	n, _ = ret.Read(retBuf)
	require.Equal(t, 0, n)

	// ok
	data := "test read"
	args.ReadSize = uint64(len(data))
	args.Location.Size_ = args.ReadSize
	crc, _ = security.LocationCrcCalculate(&args.Location)
	args.Location.Crc = crc
	rd, wr := io.Pipe()
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Get(gAny, gAny, gAny, gAny, gAny).DoAndReturn(
		func(ctx context.Context, w io.Writer, location proto.Location, readSize, offset uint64) (func() error, error) {
			w = wr
			if readSize > 0 && readSize < 1024 {
				return func() error {
					_, err1 := w.Write([]byte(data))
					return err1
				}, nil
			}
			return nil, errMock
		})
	_, err = hd.Get(ctx, args)
	require.NoError(t, err)
	retBuf = make([]byte, args.ReadSize*2)
	ret = rd
	n, _ = ret.Read(retBuf)
	require.Equal(t, args.ReadSize, uint64(n))
	require.Equal(t, data, string(retBuf[:n]))

	// ok, zero copy
	data = "test read"
	args.ReadSize = uint64(len(data))
	args.Location.Size_ = args.ReadSize
	crc, _ = security.LocationCrcCalculate(&args.Location)
	args.Location.Crc = crc
	buff := bytes.NewBuffer([]byte{})
	args.Writer = buff
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Get(gAny, args.Writer, gAny, gAny, gAny).DoAndReturn(
		func(ctx context.Context, w io.Writer, location proto.Location, readSize, offset uint64) (func() error, error) {
			if readSize > 0 && readSize < 1024 {
				return func() error {
					_, err1 := w.Write([]byte(data))
					return err1
				}, nil
			}
			return nil, errMock
		})
	ret, err = hd.Get(ctx, args)
	require.NoError(t, err)
	require.Equal(t, data, buff.String())
	n, _ = ret.Read(retBuf)
	require.Equal(t, 0, n)
}

func TestSdkHandler_Put(t *testing.T) {
	ctx := context.Background()
	hd := newSdkHandler(t)

	_, _, err := hd.Put(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)
	_, _, err = hd.doPutObject(ctx, nil)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	// size 0
	loc, hash, err := hd.Put(ctx, &acapi.PutArgs{Hashes: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(0), loc.Size_)
	require.Equal(t, 1, len(hash))

	args := &acapi.PutArgs{Size: 2}

	// stream put error
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Put(gAny, gAny, gAny, gAny).Return(nil, errMock)
	loc, hash, err = hd.Put(ctx, args)
	require.NotNil(t, err)
	require.Equal(t, uint64(0), loc.Size_)
	require.Equal(t, 0, len(hash))

	// ok
	args.Hashes = 1
	mockLoc := proto.Location{Size_: 2}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Put(gAny, gAny, gAny, gAny).Return(&mockLoc, nil)
	loc, hash, err = hd.Put(ctx, args)
	require.NoError(t, err)
	require.Equal(t, mockLoc.Size_, loc.Size_)
	require.Equal(t, int(args.Hashes), len(hash))

	// handler put error, retry 3 times
	args.GetBody = func() (io.ReadCloser, error) {
		return nil, nil
	}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Put(gAny, gAny, gAny, gAny).Return(nil, errMock).Times(3)
	loc, hash, err = hd.Put(ctx, args)
	require.NotNil(t, err)
	require.Equal(t, uint64(0), loc.Size_)
	require.Equal(t, 0, len(hash))

	// retry ok
	data := make([]byte, 8)
	args.GetBody = func() (io.ReadCloser, error) {
		buff := bytes.NewBuffer(data)
		return io.NopCloser(buff), nil
	}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Put(gAny, gAny, gAny, gAny).Return(nil, errMock)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Put(gAny, gAny, gAny, gAny).Return(&mockLoc, nil)
	loc, hash, err = hd.Put(ctx, args)
	require.NoError(t, err)
	require.Equal(t, mockLoc.Size_, loc.Size_)
	require.Equal(t, int(args.Hashes), len(hash))
}

func TestSdkHandler_Alloc(t *testing.T) {
	ctx := context.Background()
	hd := newSdkHandler(t)

	_, err := hd.alloc(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)
	_, err = hd.alloc(ctx, &acapi.AllocArgs{})
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	args := &acapi.AllocArgs{
		Size:            1,
		BlobSize:        1,
		AssignClusterID: 1,
		CodeMode:        codemode.EC3P3,
	}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(gAny, gAny, gAny, gAny, gAny).Return(nil, errMock)
	ret, err := hd.alloc(ctx, args)
	require.NotNil(t, err)
	require.Equal(t, proto.Location{}, ret.Location)

	loca := &proto.Location{
		ClusterID: 1,
		Size_:     2,
		SliceSize: 1,
	}
	crc, _ := security.LocationCrcCalculate(loca)
	loca.Crc = crc
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(gAny, gAny, gAny, gAny, gAny).Return(loca, nil)
	ret, err = hd.alloc(ctx, args)
	require.NoError(t, err)
	require.Equal(t, *loca, ret.Location)
}

func TestSdkHandler_putParts(t *testing.T) {
	ctx := context.Background()
	hd := newSdkHandler(t)

	hd.conf.MaxSizePutOnce = 8
	args := &acapi.PutArgs{Size: 12}

	// alloc fail
	args.Hashes = 1
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(gAny, gAny, gAny, gAny, gAny).Return(nil, errMock)
	loc, hash, err := hd.Put(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)
	require.NotEqual(t, args.Size, int64(loc.Size_))
	require.Nil(t, hash)

	// all ok
	data := "test alloc put"
	args.Body = bytes.NewBuffer([]byte(data))
	args.Size = int64(len(data))
	loca := &proto.Location{
		ClusterID: 1,
		CodeMode:  codemode.EC3P3,
		Size_:     uint64(args.Size),
		SliceSize: 4,
		Slices: []proto.Slice{{
			MinSliceID: 1001,
			Vid:        10,
			Count:      4,
		}},
	}
	crc, _ := security.LocationCrcCalculate(loca)
	loca.Crc = crc
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(gAny, gAny, gAny, gAny, gAny).Return(loca, nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).Return(nil).Times(14/4 + 1)
	loc, hash, err = hd.Put(ctx, args)
	require.NoError(t, err)
	require.Equal(t, 1, len(hash))
	require.Equal(t, *loca, loc)

	// waiting at least one blob, errcode.ErrAccessReadRequestBody
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(gAny, gAny, gAny, gAny, gAny).Return(loca, nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(gAny, gAny).Return(nil)
	_, _, err = hd.Put(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrAccessReadRequestBody)

	// alloc the rest parts failed
	loca.CodeMode = codemode.EC3P3
	args.Body = bytes.NewBuffer([]byte(data))
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(gAny, gAny, gAny, gAny, gAny).Return(loca, nil).Times(4) // init, retry3
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).Return(errMock).Times(4)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(gAny, gAny).Return(nil).Times(4 + 1) // 4 blobs, fail del
	_, _, err = hd.Put(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrUnexpected)

	// alloc the rest parts failed
	{
		loca.CodeMode = codemode.EC3P3
		args.Body = bytes.NewBuffer([]byte(data))
		hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(gAny, gAny, gAny, gAny, gAny).Return(loca, nil)
		locb := &proto.Location{
			ClusterID: 1,
			Size_:     uint64(args.Size),
			SliceSize: 4,
			Slices: []proto.Slice{{
				MinSliceID: 1001,
				Vid:        11, // loca + 1
				Count:      4,
			}},
		}
		err = security.LocationCrcFill(locb)
		require.Nil(t, err)
		hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(gAny, gAny, gAny, gAny, gAny).Return(locb, nil)
		hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).Return(errMock).Times(1)
		hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).Return(nil).AnyTimes()
		hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(gAny, gAny).Return(nil).Times(4 + 1) // 4 blobs, fail del
		_, _, err = hd.Put(ctx, args)
		require.NotNil(t, err)
		require.ErrorIs(t, err, errcode.ErrUnexpected)
	}
}

func TestSdkBlob_Get(t *testing.T) {
	ctx := context.Background()
	hd := newSdkHandler(t)
	hd.conf.ShardnodeConfig = &stream.ShardnodeConfig{}

	// err
	_, err := hd.GetBlob(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	// err
	_, err = hd.GetBlob(ctx, &acapi.GetBlobArgs{
		ClusterID: 0,
		BlobName:  nil,
	})
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	// err mock
	args := &acapi.GetBlobArgs{
		ClusterID: 1,
		BlobName:  []byte("blob1"),
		Mode:      acapi.GetShardModeRandom,
		Offset:    0,
	}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().GetBlob(gAny, gAny).Return(nil, errMock)
	_, err = hd.GetBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)

	// get size 0
	hd.handler.(*mocks.MockStreamHandler).EXPECT().GetBlob(gAny, gAny).Return(&proto.Location{
		ClusterID: 1,
		CodeMode:  codemode.EC3P3,
		Size_:     1,
	}, nil)
	ret, err := hd.GetBlob(ctx, args)
	require.NoError(t, err)
	retBuf := make([]byte, 2)
	n, _ := ret.Read(retBuf)
	require.Equal(t, 0, n)

	// ok
	data := "test read"
	args.ReadSize = uint64(len(data))

	loc := proto.Location{
		ClusterID: 1,
		CodeMode:  codemode.EC3P3,
		Size_:     1,
		SliceSize: 1,
		Slices:    make([]proto.Slice, 0),
	}
	crc, _ := security.LocationCrcCalculate(&loc)
	loc.Crc = crc

	rd, wr := io.Pipe()
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Get(gAny, gAny, gAny, gAny, gAny).DoAndReturn(
		func(ctx context.Context, w io.Writer, location proto.Location, readSize, offset uint64) (func() error, error) {
			w = wr
			if readSize > 0 && readSize < 1024 {
				return func() error {
					_, err1 := w.Write([]byte(data))
					return err1
				}, nil
			}
			return nil, errMock
		})
	hd.handler.(*mocks.MockStreamHandler).EXPECT().GetBlob(gAny, gAny).Return(&loc, nil)
	_, err = hd.GetBlob(ctx, &acapi.GetBlobArgs{
		ClusterID: 1,
		BlobName:  []byte("blob1"),
		ReadSize:  1,
	})
	require.NoError(t, err)
	retBuf = make([]byte, args.ReadSize*2)
	n, _ = rd.Read(retBuf)
	require.Equal(t, args.ReadSize, uint64(n))
	require.Equal(t, data, string(retBuf[:n]))
}

func TestSdkBlob_List(t *testing.T) {
	ctx := context.Background()
	hd := newSdkHandler(t)

	hd.conf.ShardnodeConfig = &stream.ShardnodeConfig{}
	_, err := hd.ListBlob(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	args := &acapi.ListBlobArgs{}
	_, err = hd.ListBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().ListBlob(gAny, gAny).Return(nil, errMock)
	args.ClusterID = 1
	_, err = hd.ListBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().ListBlob(gAny, gAny).Return(&shardnode.ListBlobRet{}, nil)
	_, err = hd.ListBlob(ctx, args)
	require.NoError(t, err)
}

func TestSdkBlob_Create(t *testing.T) {
	ctx := context.Background()
	hd := newSdkHandler(t)

	hd.conf.ShardnodeConfig = &stream.ShardnodeConfig{}
	_, err := hd.CreateBlob(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	args := &acapi.CreateBlobArgs{}
	_, err = hd.CreateBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().CreateBlob(gAny, gAny).Return(nil, errMock)
	args.CodeMode = codemode.EC3P3
	args.Size = 1
	args.BlobName = []byte("blob1")
	_, err = hd.CreateBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)

	loca := &proto.Location{
		ClusterID: 1,
		Size_:     2,
		SliceSize: 1,
	}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().CreateBlob(gAny, gAny).Return(loca, nil)
	_, err = hd.CreateBlob(ctx, args)
	require.NoError(t, err)
}

func TestSdkBlob_Seal(t *testing.T) {
	ctx := context.Background()
	hd := newSdkHandler(t)

	hd.conf.ShardnodeConfig = &stream.ShardnodeConfig{}
	err := hd.SealBlob(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	args := &acapi.SealBlobArgs{}
	err = hd.SealBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().SealBlob(gAny, gAny).Return(errMock)
	args.ClusterID = 1
	args.BlobName = []byte("blob1")
	args.Slices = make([]proto.Slice, 1)
	err = hd.SealBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().SealBlob(gAny, gAny).Return(nil)
	err = hd.SealBlob(ctx, args)
	require.NoError(t, err)
}

func TestSdkBlob_Delete(t *testing.T) {
	ctx := context.Background()
	hd := newSdkHandler(t)

	hd.conf.ShardnodeConfig = &stream.ShardnodeConfig{}
	err := hd.DeleteBlob(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	args := &acapi.DelBlobArgs{}
	err = hd.DeleteBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().DeleteBlob(gAny, gAny).Return(errMock)
	args.ClusterID = 1
	args.BlobName = []byte("blob1")
	err = hd.DeleteBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().DeleteBlob(gAny, gAny).Return(nil)
	err = hd.DeleteBlob(ctx, args)
	require.NoError(t, err)
}

func TestSdkBlob_Put(t *testing.T) {
	ctx := context.Background()
	hd := newSdkHandler(t)

	hd.conf.ShardnodeConfig = &stream.ShardnodeConfig{}
	args := &acapi.PutBlobArgs{}
	_, err := hd.PutBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	args = &acapi.PutBlobArgs{
		BlobName:  []byte("blob1"),
		CodeMode:  codemode.EC3P3,
		NeedSeal:  false,
		ShardKeys: nil,
		Hashes:    0,
	}
	data := "test_put1"
	args.Body = bytes.NewBuffer([]byte(data))
	args.Size = uint64(len(data))
	loca := &proto.Location{
		ClusterID: 1,
		Size_:     uint64(len(data)),
		SliceSize: 4,
		Slices: []proto.Slice{
			{
				MinSliceID: 1,
				Vid:        10,
				Count:      3,
				ValidSize:  uint64(len(data)),
			},
		},
	}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().CreateBlob(gAny, gAny).Return(loca, nil)
	wt := bytes.NewBuffer(nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).DoAndReturn(
		func(ctx context.Context, rd io.Reader, cid proto.ClusterID, vid proto.Vid, bid proto.BlobID, sz int64, hm acapi.HasherMap) error {
			io.Copy(wt, rd)
			return nil
		}).Times(9/4 + 1)
	// put ok
	cid, err := hd.PutBlob(ctx, args)
	require.NoError(t, err)
	require.Equal(t, proto.ClusterID(1), cid)
	require.Equal(t, data, wt.String())

	// put fail, not have read, return EOF
	args.BlobName = []byte("blob0")
	hd.handler.(*mocks.MockStreamHandler).EXPECT().CreateBlob(gAny, gAny).Return(loca, nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().DeleteBlob(gAny, gAny).Return(nil)
	cid, err = hd.PutBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, proto.ClusterID(1), cid)

	// put fail, create fail
	hd.handler.(*mocks.MockStreamHandler).EXPECT().CreateBlob(gAny, gAny).Return(&proto.Location{}, errMock)
	cid, err = hd.PutBlob(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)
	require.Equal(t, proto.ClusterID(0), cid)

	// split slice. blobID 2 fail
	hd.conf.MaxRetry = 3
	args.BlobName = []byte("blob2")
	data = "test_put2"
	args.Body = bytes.NewBuffer([]byte(data))
	locb := &proto.Location{
		ClusterID: 1,
		Size_:     uint64(len(data)),
		SliceSize: 2,
		Slices: []proto.Slice{{
			MinSliceID: 1, // 1,2,3
			Vid:        1,
			Count:      3,
			ValidSize:  6,
		}, {
			MinSliceID: 10, // 10,11
			Vid:        2,
			Count:      2,
			ValidSize:  3,
		}},
	}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().CreateBlob(gAny, gAny).Return(locb, nil)
	wt = bytes.NewBuffer(nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).DoAndReturn(
		func(ctx context.Context, rd io.Reader, cid proto.ClusterID, vid proto.Vid, bid proto.BlobID, sz int64, hm acapi.HasherMap) error {
			io.Copy(wt, rd)
			return nil
		}).Times(1)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).Return(errMock) // .Times(hd.conf.MaxRetry)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(gAny, gAny).Return(nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().AllocSlice(gAny, gAny).Return(&shardnode.AllocSliceRet{
		Slices: []proto.Slice{{
			MinSliceID: 4, // 2,3 -> 4,5
			Vid:        3,
			Count:      2,
			ValidSize:  4,
		}},
	}, nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).DoAndReturn(
		func(ctx context.Context, rd io.Reader, cid proto.ClusterID, vid proto.Vid, bid proto.BlobID, sz int64, hm acapi.HasherMap) error {
			io.Copy(wt, rd)
			return nil
		}).Times(4)
	retLoc, err := hd.putBlobs(ctx, args)
	require.Nil(t, err)
	require.Equal(t, proto.ClusterID(1), retLoc.ClusterID)
	require.Equal(t, args.Size, retLoc.Size_)
	require.Equal(t, 3, len(retLoc.Slices))
	sz := uint64(0)
	for i := range retLoc.Slices {
		sz += retLoc.Slices[i].ValidSize
	}
	require.Equal(t, args.Size, sz)
	require.Equal(t, data, wt.String())

	// first slice idx fail(blobID 1 fail), retry alloc all slices
	hd.conf.MaxRetry = 3
	data = "test_put3"
	args.BlobName = []byte("blob3")
	args.Body = bytes.NewBuffer([]byte(data))
	hd.handler.(*mocks.MockStreamHandler).EXPECT().CreateBlob(gAny, gAny).Return(locb, nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).Return(errMock) // .Times(hd.conf.MaxRetry)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(gAny, gAny).Return(nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().AllocSlice(gAny, gAny).Return(&shardnode.AllocSliceRet{
		Slices: []proto.Slice{{
			MinSliceID: 4, // 1,2,3 -> 4,5,6
			Vid:        3,
			Count:      3,
			ValidSize:  6,
		}},
	}, nil)
	wt = bytes.NewBuffer(nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).DoAndReturn(
		func(ctx context.Context, rd io.Reader, cid proto.ClusterID, vid proto.Vid, bid proto.BlobID, sz int64, hm acapi.HasherMap) error {
			io.Copy(wt, rd)
			return nil
		}).Times(3 + 2) // count 3+2
	retLoc, err = hd.putBlobs(ctx, args)
	require.Nil(t, err)
	require.Equal(t, proto.ClusterID(1), retLoc.ClusterID)
	require.Equal(t, args.Size, retLoc.Size_)
	require.Equal(t, 2, len(retLoc.Slices))
	sz = uint64(0)
	for i := range retLoc.Slices {
		sz += retLoc.Slices[i].ValidSize
	}
	require.Equal(t, args.Size, sz)
	require.Equal(t, data, wt.String())

	// last slice idx fail(blobID 11 fail), retry alloc last blobId of last slice
	hd.conf.MaxRetry = 3
	data = "test_put4"
	args.BlobName = []byte("blob4")
	args.Body = bytes.NewBuffer([]byte(data))
	hd.handler.(*mocks.MockStreamHandler).EXPECT().CreateBlob(gAny, gAny).Return(locb, nil)
	wt = bytes.NewBuffer(nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).DoAndReturn(
		func(ctx context.Context, rd io.Reader, cid proto.ClusterID, vid proto.Vid, bid proto.BlobID, sz int64, hm acapi.HasherMap) error {
			io.Copy(wt, rd)
			return nil
		}).Times(3 + 1) // count 3+2
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).Return(errMock) // .Times(hd.conf.MaxRetry)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(gAny, gAny).Return(nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().AllocSlice(gAny, gAny).Return(&shardnode.AllocSliceRet{
		Slices: []proto.Slice{{
			MinSliceID: 12, // 11 -> 12
			Vid:        3,
			Count:      1,
			ValidSize:  1,
		}},
	}, nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(gAny, gAny, gAny, gAny, gAny, gAny, gAny).DoAndReturn(
		func(ctx context.Context, rd io.Reader, cid proto.ClusterID, vid proto.Vid, bid proto.BlobID, sz int64, hm acapi.HasherMap) error {
			io.Copy(wt, rd)
			return nil
		}).Times(1)
	retLoc, err = hd.putBlobs(ctx, args)
	require.Nil(t, err)
	require.Equal(t, proto.ClusterID(1), retLoc.ClusterID)
	require.Equal(t, args.Size, retLoc.Size_)
	require.Equal(t, 3, len(retLoc.Slices))
	sz = uint64(0)
	for i := range retLoc.Slices {
		sz += retLoc.Slices[i].ValidSize
	}
	require.Equal(t, args.Size, sz)
	require.Equal(t, data, wt.String())
}
