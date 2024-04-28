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
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

func newSdkHandler() *sdkHandler {
	ctr := gomock.NewController(&testing.T{})
	h := mocks.NewMockStreamHandler(ctr)
	l := stream.NewLimiter(stream.LimitConfig{
		NameRps: map[string]int{
			"alloc": 2,
		},
		ReaderMBps: 0,
		WriterMBps: 0,
	})

	conf := Config{}
	fixConfig(&conf)
	return &sdkHandler{
		handler: h,
		limiter: l,
		conf:    conf,
		closer:  closer.New(),
	}
}

func TestNewSdkBlobstore(t *testing.T) {
	conf := &Config{}
	conf.IDC = "xx"
	_, err := New(conf)
	require.NotNil(t, err)
}

func TestSdkHandler_Delete(t *testing.T) {
	any := gomock.Any()
	errMock := errors.New("fake error")
	ctx := context.Background()
	hd := newSdkHandler()
	_, err := hd.Delete(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	_, err = hd.Delete(ctx, &acapi.DeleteArgs{})
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	args := &acapi.DeleteArgs{
		Locations: []acapi.Location{
			{
				Size: 0,
			},
		},
	}
	ret, err := hd.Delete(ctx, args)
	require.NoError(t, err)
	require.Nil(t, ret)

	args.Locations[0].Size = 1
	_, err = hd.Delete(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	// retry 3 time
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(any, any).Times(3).Return(errMock)
	crc, _ := stream.LocationCrcCalculate(&args.Locations[0])
	args.Locations[0].Crc = crc
	args.Locations[0].Blobs = make([]acapi.SliceInfo, 0)
	ret, err = hd.Delete(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrUnexpected)
	require.Equal(t, args.Locations, ret)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(any, any).Return(nil)
	ret, err = hd.Delete(ctx, args)
	require.NoError(t, err)
	require.Nil(t, ret)

	// 3 location
	loc := acapi.Location{
		ClusterID: 1,
		Size:      1,
		Blobs:     []acapi.SliceInfo{{Vid: 9}},
	}
	crc, _ = stream.LocationCrcCalculate(&loc)
	loc.Crc = crc
	args.Locations = make([]acapi.Location, 0)
	for len(args.Locations) < 3 {
		args.Locations = append(args.Locations, loc)
	}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(any, any).Return(nil)
	ret, err = hd.Delete(ctx, args)
	require.NoError(t, err)
	require.Nil(t, ret)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(any, any).Return(errMock).Times(3) // retry
	ret, err = hd.Delete(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrUnexpected)
	require.Equal(t, args.Locations, ret)
}

func TestSdkHandler_Get(t *testing.T) {
	any := gomock.Any()
	errMock := errors.New("fake error")
	ctx := context.Background()
	hd := newSdkHandler()

	_, err := hd.Get(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	ret, err := hd.Get(ctx, &acapi.GetArgs{Body: bytes.NewBuffer([]byte{})})
	require.NoError(t, err)
	retBuf := make([]byte, 2)
	n, _ := ret.Read(retBuf)
	require.Equal(t, 0, n)

	args := &acapi.GetArgs{
		ReadSize: 1,
		Location: acapi.Location{
			Size: 2,
		},
		Body: bytes.NewBuffer([]byte{}),
	}
	_, err = hd.Get(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	crc, _ := stream.LocationCrcCalculate(&args.Location)
	args.Location.Crc = crc
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Get(any, any, any, any, any).Return(nil, errMock)
	args.ReadSize = 2
	_, err = hd.Get(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().Get(any, any, any, any, any).Return(func() error { return errMock }, nil)
	ret, err = hd.Get(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)
	require.Nil(t, ret)

	hd.handler.(*mocks.MockStreamHandler).EXPECT().Get(any, any, any, any, any).Return(func() error { return nil }, nil)
	ret, err = hd.Get(ctx, args)
	require.NoError(t, err)
	retBuf = make([]byte, args.ReadSize*2)
	n, _ = ret.Read(retBuf)
	require.Equal(t, 0, n)

	data := "test read"
	args.ReadSize = uint64(len(data))
	args.Location.Size = args.ReadSize
	crc, _ = stream.LocationCrcCalculate(&args.Location)
	args.Location.Crc = crc
	args.Body = bytes.NewBuffer([]byte{})
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Get(any, args.Body, any, any, any).DoAndReturn(
		func(ctx context.Context, w io.Writer, location acapi.Location, readSize, offset uint64) (func() error, error) {
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
	retBuf = make([]byte, args.ReadSize*2)
	n, _ = ret.Read(retBuf)
	require.Equal(t, args.ReadSize, uint64(n))
	require.Equal(t, data, string(retBuf[:n]))
}

func TestSdkHandler_Put(t *testing.T) {
	any := gomock.Any()
	errMock := errors.New("fake error")
	ctx := context.Background()
	hd := newSdkHandler()

	_, _, err := hd.Put(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)
	_, _, err = hd.doPutObject(ctx, nil)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	// size 0
	loc, hash, err := hd.Put(ctx, &acapi.PutArgs{Hashes: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(0), loc.Size)
	require.Equal(t, 1, len(hash))

	args := &acapi.PutArgs{
		Size: 2,
	}

	hd.handler.(*mocks.MockStreamHandler).EXPECT().Put(any, any, any, any).Return(nil, errMock)
	loc, hash, err = hd.Put(ctx, args)
	require.NotNil(t, err)
	require.Equal(t, uint64(0), loc.Size)
	require.Equal(t, 0, len(hash))

	args.Hashes = 1
	mockLoc := acapi.Location{Size: 2}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Put(any, any, any, any).Return(&mockLoc, nil)
	loc, hash, err = hd.Put(ctx, args)
	require.NoError(t, err)
	require.Equal(t, mockLoc.Size, loc.Size)
	require.Equal(t, int(args.Hashes), len(hash))
}

func TestSdkHandler_Alloc(t *testing.T) {
	any := gomock.Any()
	errMock := errors.New("fake error")
	ctx := context.Background()
	hd := newSdkHandler()

	_, err := hd.Alloc(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)
	_, err = hd.Alloc(ctx, &acapi.AllocArgs{})
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	args := &acapi.AllocArgs{
		Size:            1,
		BlobSize:        1,
		AssignClusterID: 1,
		CodeMode:        codemode.EC3P3,
	}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(any, any, any, any, any).Return(nil, errMock)
	ret, err := hd.Alloc(ctx, args)
	require.NotNil(t, err)
	require.Equal(t, acapi.Location{}, ret.Location)

	loca := &acapi.Location{
		ClusterID: 1,
		Size:      2,
		BlobSize:  1,
	}
	crc, _ := stream.LocationCrcCalculate(loca)
	loca.Crc = crc
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(any, any, any, any, any).Return(loca, nil)
	ret, err = hd.Alloc(ctx, args)
	require.NoError(t, err)
	require.Equal(t, *loca, ret.Location)
}

func TestSdkHandler_putParts(t *testing.T) {
	any := gomock.Any()
	errMock := errors.New("fake error")
	ctx := context.Background()
	hd := newSdkHandler()

	hd.conf.MaxSizePutOnce = 8
	args := &acapi.PutArgs{
		Size: 12,
	}

	// alloc fail
	args.Hashes = 1
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(any, any, any, any, any).Return(nil, errMock)
	loc, hash, err := hd.Put(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errMock)
	require.NotEqual(t, args.Size, int64(loc.Size))
	require.Nil(t, hash)

	// all ok
	data := "test alloc put"
	args.Body = bytes.NewBuffer([]byte(data))
	args.Size = int64(len(data))
	loca := &acapi.Location{
		ClusterID: 1,
		Size:      uint64(args.Size),
		BlobSize:  4,
		Blobs: []acapi.SliceInfo{{
			MinBid: 1001,
			Vid:    10,
			Count:  4,
		}},
	}
	crc, _ := stream.LocationCrcCalculate(loca)
	loca.Crc = crc
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(any, any, any, any, any).Return(loca, nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(any, any, any, any, any, any, any).Return(nil).Times(14/4 + 1)
	loc, hash, err = hd.Put(ctx, args)
	require.NoError(t, err)
	require.Equal(t, 1, len(hash))
	require.Equal(t, *loca, loc)

	// waiting at least one blob, errcode.ErrAccessReadRequestBody
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(any, any, any, any, any).Return(loca, nil)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(any, any).Return(nil)
	_, _, err = hd.Put(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrAccessReadRequestBody)

	// alloc the rest parts failed
	loca.CodeMode = codemode.EC3P3
	args.Body = bytes.NewBuffer([]byte(data))
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(any, any, any, any, any).Return(loca, nil).Times(4) // init, retry3
	hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(any, any, any, any, any, any, any).Return(errMock).Times(4)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(any, any).Return(nil).Times(4 + 1) // 4 blobs, fail del
	_, _, err = hd.Put(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrUnexpected)

	// alloc the rest parts failed
	{
		loca.CodeMode = codemode.EC3P3
		args.Body = bytes.NewBuffer([]byte(data))
		hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(any, any, any, any, any).Return(loca, nil)
		locb := &acapi.Location{
			ClusterID: 1,
			Size:      uint64(args.Size),
			BlobSize:  4,
			Blobs: []acapi.SliceInfo{{
				MinBid: 1001,
				Vid:    11, // loca + 1
				Count:  4,
			}},
		}
		stream.LocationCrcFill(locb)
		hd.handler.(*mocks.MockStreamHandler).EXPECT().Alloc(any, any, any, any, any).Return(locb, nil)
		hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(any, any, any, any, any, any, any).Return(errMock).Times(1)
		hd.handler.(*mocks.MockStreamHandler).EXPECT().PutAt(any, any, any, any, any, any, any).Return(nil).AnyTimes()
		hd.handler.(*mocks.MockStreamHandler).EXPECT().Delete(any, any).Return(nil).Times(4 + 1 + 1) // 4 blobs, fail del
		_, _, err = hd.Put(ctx, args)
		require.NotNil(t, err)
		require.ErrorIs(t, err, errcode.ErrUnexpected)
	}
}
