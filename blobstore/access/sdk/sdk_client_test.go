package sdk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access"
	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func newSdkHandler() *sdkHandler {
	ctr := gomock.NewController(&testing.T{})
	h := mocks.NewMockStreamHandler(ctr)
	l := access.NewLimiter(access.LimitConfig{
		NameRps: map[string]int{
			"alloc": 2,
		},
		ReaderMBps: 0,
		WriterMBps: 0,
	})

	return &sdkHandler{
		handler: h,
		limiter: l,
	}
}

func TestNewSdkBlobstore(t *testing.T) {
	conf := &SdkConfig{}
	conf.IDC = "xx"
	_, err := NewSdkBlobstore(conf)
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
	_, err = hd.doDelete(ctx, nil)
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
	crc, _ := access.CalcCrcWrap(&args.Locations[0])
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
	crc, _ = access.CalcCrcWrap(&loc)
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
	_, err = hd.doGet(ctx, nil)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	ret, err := hd.Get(ctx, &acapi.GetArgs{})
	require.NoError(t, err)
	retBuf := make([]byte, 2)
	n, _ := ret.Read(retBuf)
	require.Equal(t, 0, n)

	args := &acapi.GetArgs{
		ReadSize: 1,
		Location: acapi.Location{
			Size: 2,
		},
	}
	_, err = hd.Get(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

	crc, _ := access.CalcCrcWrap(&args.Location)
	args.Location.Crc = crc
	ret, err = hd.Get(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)
	require.Nil(t, ret)
	_, err = hd.doGet(ctx, args)
	require.ErrorIs(t, err, errcode.ErrIllegalArguments)

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
	require.Equal(t, args.ReadSize, uint64(n))

	data := "test read"
	args.ReadSize = uint64(len(data))
	args.Location.Size = args.ReadSize
	crc, _ = access.CalcCrcWrap(&args.Location)
	args.Location.Crc = crc
	dstBuf := make([]byte, args.ReadSize)
	w := bytes.NewBuffer(dstBuf)
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Get(any, w, any, any, any).DoAndReturn(
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

	loc, hash, err := hd.Put(ctx, &acapi.PutArgs{})
	require.NoError(t, err)
	require.Equal(t, uint64(0), loc.Size)
	require.Equal(t, 0, len(hash))

	hd.conf.MaxSizePutOnce = 1
	args := &acapi.PutArgs{
		Size: 2,
	}
	loc, hash, err = hd.Put(ctx, args)
	require.NotNil(t, err)
	require.ErrorIs(t, err, errcode.ErrRequestNotAllow)
	require.Equal(t, uint64(0), loc.Size)
	require.Equal(t, 0, len(hash))

	hd.conf.MaxSizePutOnce = 10
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Put(any, any, any, any).Return(nil, errMock)
	loc, hash, err = hd.Put(ctx, args)
	require.NotNil(t, err)
	require.Equal(t, uint64(0), loc.Size)
	require.Equal(t, 0, len(hash))

	mockLoc := acapi.Location{Size: 2}
	hd.handler.(*mocks.MockStreamHandler).EXPECT().Put(any, any, any, any).Return(&mockLoc, nil)
	loc, hash, err = hd.Put(ctx, args)
	require.NoError(t, err)
	require.Equal(t, mockLoc.Size, loc.Size)
	require.Equal(t, 0, len(hash))
}
