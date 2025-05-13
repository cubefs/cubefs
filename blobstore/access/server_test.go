// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package access

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access/stream"
	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/uptoken"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

var (
	ctx              = context.Background()
	_blobSize uint32 = 1 << 20
	location         = &proto.Location{
		ClusterID: 1,
		CodeMode:  1,
		SliceSize: _blobSize,
		Crc:       0,
		Slices: []proto.Slice{{
			MinSliceID: 111,
			Vid:        1111,
			Count:      11,
		}},
	}

	testServer *httptest.Server
	once       sync.Once
)

func runMockService(s *Service) string {
	once.Do(func() {
		testServer = httptest.NewServer(NewHandler(s))
	})
	return testServer.URL
}

func newService() *Service {
	ctr := gomock.NewController(&testing.T{})
	s := mocks.NewMockStreamHandler(ctr)

	s.EXPECT().Alloc(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, size uint64, blobSize uint32,
			assignClusterID proto.ClusterID, codeMode codemode.CodeMode,
		) (*proto.Location, error) {
			if size < 1024 {
				return nil, errors.New("fake alloc location")
			}
			loc := location.Copy()
			loc.Size_ = uint64(size)
			stream.LocationCrcFill(&loc)
			return &loc, nil
		})

	s.EXPECT().PutAt(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, rc io.Reader,
			clusterID proto.ClusterID, vid proto.Vid, bid proto.BlobID, size int64, hasherMap access.HasherMap,
		) error {
			if size < 1024 {
				return errcode.ErrAccessLimited
			}
			return nil
		})

	s.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, rc io.Reader, size int64, hasherMap access.HasherMap) (*proto.Location, error) {
			if size < 1024 {
				return nil, errors.New("fake put nil body")
			}
			loc := location.Copy()
			loc.Size_ = uint64(size)
			stream.LocationCrcFill(&loc)
			return &loc, nil
		})

	s.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, w io.Writer, location proto.Location, readSize, offset uint64) (func() error, error) {
			if readSize < 1024 {
				return nil, errors.New("fake get nil body")
			}
			return func() error { return nil }, nil
		})
	s.EXPECT().Delete(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, location *proto.Location) error {
			if location.ClusterID >= 10 {
				return errors.New("fake delete error with cluster")
			} else if location.ClusterID == 1 && location.Crc > 0 && location.Size_ < 1024 {
				return errors.New("fake delete error")
			}
			return nil
		})

	return &Service{
		streamHandler: s,
		limiter: stream.NewLimiter(stream.LimitConfig{
			NameRps: map[string]int{
				limitNameAlloc: 2,
			},
			ReaderMBps: 0,
			WriterMBps: 0,
		}),
	}
}

func newClient() rpc.Client {
	return rpc.NewClient(&rpc.Config{})
}

func TestAccessServiceNew(t *testing.T) {
	runMockService(newService())
}

func TestAccessServiceAlloc(t *testing.T) {
	host := runMockService(newService())
	cli := newClient()

	url := func() string {
		return fmt.Sprintf("%s/alloc", host)
	}
	args := access.AllocArgs{
		Size:            0,
		BlobSize:        0,
		AssignClusterID: 0,
		CodeMode:        0,
	}
	{
		resp := &access.AllocResp{}
		err := cli.PostWith(ctx, url(), resp, args)
		assertErrorCode(t, 400, err)
	}
	{
		args.Size = 1023
		resp := &access.AllocResp{}
		err := cli.PostWith(ctx, url(), resp, args)
		assertErrorCode(t, 500, err)
	}
	{
		args.Size = 1024
		resp := &access.AllocResp{}
		err := cli.PostWith(ctx, url(), resp, args)
		require.NoError(t, err)
		require.Equal(t, uint64(1024), resp.Location.Size_)
	}
	{
		args.Size = uint64(_blobSize)
		resp := &access.AllocResp{}
		err := cli.PostWith(ctx, url(), resp, args)
		require.NoError(t, err)
		require.Equal(t, uint64(_blobSize), resp.Location.Size_)
	}
	{
		args.Size = uint64(_blobSize) + 1
		resp := &access.AllocResp{}
		err := cli.PostWith(ctx, url(), resp, args)
		require.NoError(t, err)
		require.Equal(t, uint64(_blobSize)+1, resp.Location.Size_)
	}
}

func TestAccessServicePutAt(t *testing.T) {
	host := runMockService(newService())
	cli := newClient()

	url := func(size int64, token string) string {
		return fmt.Sprintf("%s/putat?clusterid=1&volumeid=1111&blobid=111&size=%d&hashes=14&token=%s",
			host, size, token)
	}

	for _, method := range []string{http.MethodPut, http.MethodPost} {
		args := access.PutArgs{
			Size: 0,
		}
		{
			buf := make([]byte, args.Size)
			req, _ := http.NewRequest(method, url(args.Size, ""), bytes.NewReader(buf))
			resp := &access.PutAtResp{}
			err := cli.DoWith(ctx, req, resp, rpc.WithCrcEncode())
			assertErrorCode(t, 400, err)
		}
		{
			args.Size = 1023
			buf := make([]byte, args.Size)
			req, _ := http.NewRequest(method, url(args.Size, ""), bytes.NewReader(buf))
			resp := &access.PutAtResp{}
			err := cli.DoWith(ctx, req, resp, rpc.WithCrcEncode())
			assertErrorCode(t, 400, err)
		}
		{
			args.Size = 1023
			buf := make([]byte, args.Size)
			resp := &access.PutAtResp{}
			req, _ := http.NewRequest(method, url(args.Size, "c1fdcecaacbfafd86f0b00"), bytes.NewReader(buf))
			err := cli.DoWith(ctx, req, resp, rpc.WithCrcEncode())
			assertErrorCode(t, errcode.CodeAccessLimited, err)
		}
		{
			args.Size = 1024
			buf := make([]byte, args.Size)
			req, _ := http.NewRequest(method, url(args.Size, "8238436d05ecf2366f0b00"), bytes.NewReader(buf))
			resp := &access.PutAtResp{}
			err := cli.DoWith(ctx, req, resp, rpc.WithCrcEncode())
			require.NoError(t, err)

			req, _ = http.NewRequest(method, url(args.Size, "1238436d05ecf2366f0b00"), bytes.NewReader(buf))
			err = cli.DoWith(ctx, req, resp, rpc.WithCrcEncode())
			assertErrorCode(t, 400, err)
		}
	}
}

func TestAccessServicePut(t *testing.T) {
	host := runMockService(newService())
	cli := newClient()

	url := func(size int64, hashes access.HashAlgorithm) string {
		return fmt.Sprintf("%s/put?size=%d&hashes=%d", host, size, hashes)
	}

	for _, method := range []string{http.MethodPut, http.MethodPost} {
		args := access.PutArgs{
			Size:   0,
			Hashes: 14,
			Body:   nil,
		}
		{
			req, _ := http.NewRequest(method, fmt.Sprintf("%s/put?size=size", host), args.Body)
			resp := &access.PutResp{}
			err := cli.DoWith(ctx, req, resp, rpc.WithCrcEncode())
			assertErrorCode(t, 400, err)
		}
		{
			req, _ := http.NewRequest(method, url(args.Size, args.Hashes), args.Body)
			resp := &access.PutResp{}
			err := cli.DoWith(ctx, req, resp, rpc.WithCrcEncode())
			assertErrorCode(t, 400, err)
		}
		{
			args.Body = bytes.NewReader(make([]byte, 1023))
			req, _ := http.NewRequest(method, url(1023, args.Hashes), args.Body)
			resp := &access.PutResp{}
			err := cli.DoWith(ctx, req, resp, rpc.WithCrcEncode())
			assertErrorCode(t, 500, err)
		}
		{
			args.Body = bytes.NewReader(make([]byte, 1024))
			req, _ := http.NewRequest(method, url(1024, args.Hashes), args.Body)
			resp := &access.PutResp{}
			err := cli.DoWith(ctx, req, resp, rpc.WithCrcEncode())
			require.NoError(t, err)
			require.Equal(t, uint64(1024), resp.Location.Size_)
		}
	}
}

func TestAccessServiceGet(t *testing.T) {
	host := runMockService(newService())
	cli := newClient()

	url := func() string {
		return fmt.Sprintf("%s/get", host)
	}
	args := access.GetArgs{
		Location: location.Copy(),
		Offset:   0,
		ReadSize: 0,
	}
	{
		args.ReadSize = 10
		resp, err := cli.Post(ctx, url(), args)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode, resp.Status)
	}
	{
		args.Location.Size_ = 1023
		args.ReadSize = 1023
		stream.LocationCrcFill(&args.Location)
		resp, err := cli.Post(ctx, url(), args)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 500, resp.StatusCode, resp.Status)
	}
	{
		args.Location.Size_ = 1024
		args.ReadSize = 1024
		stream.LocationCrcFill(&args.Location)
		resp, err := cli.Post(ctx, url(), args)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode, resp.Status)
	}
	{
		args.Location.Size_ = 10240
		args.Offset = 1000
		args.ReadSize = 1024
		stream.LocationCrcFill(&args.Location)
		resp, err := cli.Post(ctx, url(), args)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 206, resp.StatusCode, resp.Status)
	}
}

func TestAccessServiceDelete(t *testing.T) {
	host := runMockService(newService())
	cli := newClient()

	url := fmt.Sprintf("%s/delete", host)
	deleteRequest := func(args interface{}) (code int, ret access.DeleteResp, err error) {
		resp, err := cli.Post(ctx, url, args)
		if err != nil {
			return
		}
		defer resp.Body.Close()

		code = resp.StatusCode
		if code/100 == 2 {
			size, _ := strconv.Atoi(resp.Header.Get("Content-Length"))
			buf := make([]byte, size)
			_, err = io.ReadFull(resp.Body, buf)
			if err != nil {
				return
			}
			if err = json.Unmarshal(buf, &ret); err != nil {
				return
			}
		}
		if code >= 400 {
			err = rpc.NewError(code, "Code", fmt.Errorf("httpcode: %d", code))
			return
		}
		return
	}

	args := access.DeleteArgs{
		Locations: []proto.Location{location.Copy()},
	}
	{
		code, _, err := deleteRequest(access.DeleteArgs{})
		require.Error(t, err)
		require.Equal(t, 400, code)
	}
	{
		code, _, err := deleteRequest(args)
		require.Error(t, err)
		require.Equal(t, 400, code)
	}
	{
		stream.LocationCrcFill(&args.Locations[0])
		code, resp, err := deleteRequest(args)
		require.NoError(t, err)
		require.Equal(t, 226, code)
		require.Equal(t, args.Locations[0], resp.FailedLocations[0])
	}
	{
		loc := &args.Locations[0]
		loc.Size_ = 1024
		stream.LocationCrcFill(loc)
		code, _, err := deleteRequest(args)
		require.NoError(t, err)
		require.Equal(t, 200, code)
	}
	{
		loc := location.Copy()
		loc.Size_ = 1024
		stream.LocationCrcFill(&loc)
		locs := make([]proto.Location, access.MaxDeleteLocations)
		for idx := range locs {
			locs[idx] = loc
		}
		code, resp, err := deleteRequest(access.DeleteArgs{Locations: locs})
		require.NoError(t, err)
		require.Equal(t, 200, code)
		require.Equal(t, 0, len(resp.FailedLocations))
	}
	{
		loc := location.Copy()
		loc.Size_ = 1024
		stream.LocationCrcFill(&loc)
		locs := make([]proto.Location, access.MaxDeleteLocations+1)
		for idx := range locs {
			locs[idx] = loc
		}
		code, _, err := deleteRequest(access.DeleteArgs{Locations: locs})
		require.Error(t, err)
		require.Equal(t, 400, code)
	}
	{
		loc := location.Copy()
		loc.Size_ = 1024
		loc.ClusterID = proto.ClusterID(11)
		stream.LocationCrcFill(&loc)
		code, resp, err := deleteRequest(access.DeleteArgs{Locations: []proto.Location{loc}})
		require.NoError(t, err)
		require.Equal(t, 226, code)
		require.Equal(t, 1, len(resp.FailedLocations))
		require.Equal(t, proto.ClusterID(11), resp.FailedLocations[0].ClusterID)
	}
	{
		locs := make([]proto.Location, access.MaxDeleteLocations)
		for idx := range locs {
			loc := location.Copy()
			loc.Size_ = 1024
			loc.ClusterID = proto.ClusterID(idx % 11)
			stream.LocationCrcFill(&loc)
			locs[idx] = loc
		}
		code, resp, err := deleteRequest(access.DeleteArgs{Locations: locs})
		require.NoError(t, err)
		require.Equal(t, 226, code)
		require.Equal(t, 93, len(resp.FailedLocations))
	}
}

func TestAccessServiceDeleteBlob(t *testing.T) {
	host := runMockService(newService())
	cli := newClient()

	url := func(size int64, token string) string {
		return fmt.Sprintf("%s/deleteblob?clusterid=1&volumeid=1111&blobid=111&size=%d&token=%s",
			host, size, token)
	}

	method := http.MethodDelete
	args := access.PutArgs{
		Size: 0,
	}
	{
		req, _ := http.NewRequest(method, url(args.Size, "xxx"), nil)
		err := cli.DoWith(ctx, req, nil)
		assertErrorCode(t, 400, err)
	}
	{
		req, _ := http.NewRequest(method, url(args.Size, ""), nil)
		err := cli.DoWith(ctx, req, nil)
		assertErrorCode(t, 400, err)
	}
	{
		args.Size = 1023
		req, _ := http.NewRequest(method, url(args.Size, ""), nil)
		err := cli.DoWith(ctx, req, nil)
		assertErrorCode(t, 400, err)
	}
	{
		args.Size = 1023
		req, _ := http.NewRequest(method, url(args.Size, "xxx"), nil)
		err := cli.DoWith(ctx, req, nil)
		assertErrorCode(t, 400, err)
	}
	{
		args.Size = 1023
		req, _ := http.NewRequest(method, url(args.Size, "c1fdcecaacbfafd86f0b00"), nil)
		err := cli.DoWith(ctx, req, nil)
		require.NoError(t, err)
	}

	{
		url := func() string {
			return fmt.Sprintf("%s/deleteblob?clusterid=11&volumeid=1111&blobid=111&size=%d&token=%s",
				host, 1024, "f034db4503d5dc3f6f0100")
		}
		req, _ := http.NewRequest(method, url(), nil)
		err := cli.DoWith(ctx, req, nil)
		assertErrorCode(t, 500, err)
	}
}

func TestAccessServiceSign(t *testing.T) {
	host := runMockService(newService())
	cli := newClient()

	url := func() string {
		return fmt.Sprintf("%s/sign", host)
	}
	args := access.SignArgs{
		Locations: []proto.Location{location.Copy()},
		Location:  location.Copy(),
	}
	{
		resp := &access.SignResp{}
		err := cli.PostWith(ctx, url(), resp, access.SignArgs{})
		assertErrorCode(t, 400, err)
	}
	{
		resp := &access.SignResp{}
		err := cli.PostWith(ctx, url(), resp, args)
		assertErrorCode(t, 400, err)
	}
	{
		stream.LocationCrcFill(&args.Locations[0])
		resp := &access.SignResp{}
		err := cli.PostWith(ctx, url(), resp, args)
		require.NoError(t, err)
	}
}

func assertErrorCode(t *testing.T, code int, err error) {
	require.Error(t, err)
	codeActual := rpc.DetectStatusCode(err)
	require.Equal(t, code, codeActual, err.Error())
}

func TestAccessServiceTokens(t *testing.T) {
	skey := stream.TokenSecretKeys()[0][:]
	checker := func(loc *proto.Location, tokens []string) {
		if loc.Size_ == 0 {
			require.Equal(t, 0, len(tokens))
			return
		}

		hasMultiBlobs := loc.Size_ >= uint64(loc.SliceSize)
		lastSize := uint32(loc.Size_ % uint64(loc.SliceSize))
		if !hasMultiBlobs {
			require.Equal(t, 1, len(tokens))

			token := uptoken.DecodeToken(tokens[0])
			blob := loc.Slices[0]
			for bid := blob.MinSliceID - 100; bid < blob.MinSliceID+100; bid++ {
				require.False(t, token.IsValid(loc.ClusterID, blob.Vid, bid, loc.SliceSize, skey))
			}
			require.True(t, token.IsValid(loc.ClusterID, blob.Vid, blob.MinSliceID, lastSize, skey))
			return
		}

		if lastSize == 0 {
			require.Equal(t, len(loc.Slices), len(tokens))
			for idx, blob := range loc.Slices {
				token := uptoken.DecodeToken(tokens[idx])
				for ii := uint32(0); ii < 100; ii++ {
					bid := blob.MinSliceID - proto.BlobID(ii) - 1
					require.False(t, token.IsValid(loc.ClusterID, blob.Vid, bid, loc.SliceSize, skey))
					bid = blob.MinSliceID + proto.BlobID(blob.Count+ii)
					require.False(t, token.IsValid(loc.ClusterID, blob.Vid, bid, loc.SliceSize, skey))
				}
				for ii := uint32(0); ii < blob.Count; ii++ {
					bid := blob.MinSliceID + proto.BlobID(ii)
					require.True(t, token.IsValid(loc.ClusterID, blob.Vid, bid, loc.SliceSize, skey))
				}
			}
			return
		}

		require.Equal(t, len(loc.Slices)+1, len(tokens))
		for ii := 0; ii < len(loc.Slices)-1; ii++ {
			token := uptoken.DecodeToken(tokens[ii])
			blob := loc.Slices[ii]
			for ii := uint32(0); ii < blob.Count; ii++ {
				bid := blob.MinSliceID + proto.BlobID(ii)
				require.True(t, token.IsValid(loc.ClusterID, blob.Vid, bid, loc.SliceSize, skey))
			}
		}

		token := uptoken.DecodeToken(tokens[len(loc.Slices)-1])
		blob := loc.Slices[len(loc.Slices)-1]
		for ii := uint32(0); ii < 100; ii++ {
			bid := blob.MinSliceID - proto.BlobID(ii) - 1
			require.False(t, token.IsValid(loc.ClusterID, blob.Vid, bid, loc.SliceSize, skey))
			bid = blob.MinSliceID + proto.BlobID(blob.Count+ii) - 1
			require.False(t, token.IsValid(loc.ClusterID, blob.Vid, bid, loc.SliceSize, skey))
		}
		for ii := uint32(0); ii < blob.Count-1; ii++ {
			bid := blob.MinSliceID + proto.BlobID(ii)
			require.True(t, token.IsValid(loc.ClusterID, blob.Vid, bid, loc.SliceSize, skey))
		}

		token = uptoken.DecodeToken(tokens[len(loc.Slices)])
		lastbid := blob.MinSliceID + proto.BlobID(blob.Count) - 1
		require.True(t, token.IsValid(loc.ClusterID, blob.Vid, lastbid, lastSize, skey))
	}

	{
		loc := &proto.Location{
			Size_:     0,
			SliceSize: 333,
			Slices:    []proto.Slice{},
		}
		checker(loc, stream.StreamGenTokens(loc))
	}
	{
		loc := &proto.Location{
			Size_:     1,
			SliceSize: 1024,
			Slices: []proto.Slice{
				{MinSliceID: 100, Vid: 1000, Count: 1},
			},
		}
		checker(loc, stream.StreamGenTokens(loc))
	}
	{
		loc := &proto.Location{
			Size_:     1024,
			SliceSize: 1024,
			Slices: []proto.Slice{
				{MinSliceID: 100, Vid: 1000, Count: 1},
			},
		}
		checker(loc, stream.StreamGenTokens(loc))
	}
	{
		loc := &proto.Location{
			Size_:     1025,
			SliceSize: 1024,
			Slices: []proto.Slice{
				{MinSliceID: 100, Vid: 1000, Count: 2},
			},
		}
		checker(loc, stream.StreamGenTokens(loc))
	}
	{
		loc := &proto.Location{
			Size_:     2048,
			SliceSize: 1024,
			Slices: []proto.Slice{
				{MinSliceID: 100, Vid: 1000, Count: 2},
			},
		}
		checker(loc, stream.StreamGenTokens(loc))
	}
	{
		loc := &proto.Location{
			Size_:     10240,
			SliceSize: 1024,
			Slices: []proto.Slice{
				{MinSliceID: 100, Vid: 1000, Count: 4},
				{MinSliceID: 200, Vid: 1000, Count: 6},
			},
		}
		checker(loc, stream.StreamGenTokens(loc))
	}
	{
		loc := &proto.Location{
			Size_:     1025,
			SliceSize: 1024,
			Slices: []proto.Slice{
				{MinSliceID: 100, Vid: 1000, Count: 1},
				{MinSliceID: 200, Vid: 1000, Count: 1},
			},
		}
		checker(loc, stream.StreamGenTokens(loc))
	}
	{
		loc := &proto.Location{
			Size_:     10242,
			SliceSize: 1024,
			Slices: []proto.Slice{
				{MinSliceID: 100, Vid: 1000, Count: 5},
				{MinSliceID: 200, Vid: 1000, Count: 6},
			},
		}
		checker(loc, stream.StreamGenTokens(loc))
	}
}

func TestAccessServiceLimited(t *testing.T) {
	host := runMockService(newService())
	cli := newClient()

	url := func() string {
		return fmt.Sprintf("%s/alloc", host)
	}
	args := access.AllocArgs{Size: 1024}
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			resp := &access.AllocResp{}
			err := cli.PostWith(ctx, url(), resp, args)
			if err != nil {
				assertErrorCode(t, errcode.CodeAccessLimited, err)
			} else {
				require.Equal(t, uint64(1024), resp.Location.Size_)
			}
		}()
	}
	wg.Wait()
}
