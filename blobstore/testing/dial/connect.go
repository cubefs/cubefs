// Copyright 2023 The CubeFS Authors.
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

package dial

import (
	"context"
	"errors"
	"io"
	"math/rand"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/retry"
	"github.com/cubefs/cubefs/blobstore/util/task"
)

var dollars = newBuffer(36)

func newBuffer(val byte) (buff [1 << 20]byte) {
	for idx := range buff {
		buff[idx] = val
	}
	return
}

type anyReader struct {
	remain int
}

func (r *anyReader) Read(p []byte) (n int, err error) {
	if r.remain <= 0 {
		err = io.EOF
		return
	}
	for r.remain > 0 && len(p) > 0 {
		remain := r.remain
		if remain > len(dollars) {
			remain = len(dollars)
		}
		nn := copy(p, dollars[:remain])
		n += nn
		p = p[nn:]
		r.remain -= nn
	}
	return
}

func runConnection(conn Connection) {
	if conn.Sequentially {
		for ii := 0; ii < conn.N; ii++ {
			connect(conn)
		}
		return
	}

	tasks := make([]func() error, conn.N)
	for idx := range tasks {
		tasks[idx] = func() error {
			connect(conn)
			return nil
		}
	}
	task.Run(context.Background(), tasks...)
}

func connect(conn Connection) {
	var (
		span, ctx = trace.StartSpanFromContextWithTraceID(
			context.Background(), "dial", "dial-"+hostname+"-"+trace.RandomID().String())
		body     io.ReadCloser
		location access.Location
		err      error
	)
	defer func() {
		if err == nil {
			return
		}
		if err = retry.Timed(10, 1).On(func() error {
			_, e := conn.api.Delete(ctx, &access.DeleteArgs{
				Locations: []access.Location{location},
			})
			return e
		}); err != nil {
			span.Error("delete", err)
		}
	}()

	span.Infof("to upload conn:%v", conn)
	runTimer(conn, "put", conn.Size, func() error {
		location, _, err = conn.api.Put(ctx, &access.PutArgs{
			Size:   int64(conn.Size),
			Hashes: access.HashAlgMD5 | access.HashAlgSHA1,
			Body:   &anyReader{remain: conn.Size},
		})
		return err
	})
	if err != nil {
		span.Error(conn, "put error:", err)
		err = nil
		return
	}

	readSize := 10
	if readSize > conn.Size-1 {
		readSize = conn.Size - 1
	}
	readOffset := rand.Intn(conn.Size - readSize)

	span.Infof("to download conn:%v offset:%d-(%d) location:%+v", conn, readOffset, readSize, location)
	runTimer(conn, "get", readSize, func() error {
		body, err = conn.api.Get(ctx, &access.GetArgs{
			Location: location,
			Offset:   uint64(readOffset),
			ReadSize: uint64(readSize),
		})
		if err != nil {
			return err
		}
		defer body.Close()

		buff, e := io.ReadAll(body)
		if e != nil {
			return e
		}
		for idx := range buff {
			if buff[idx] != dollars[0] {
				return errors.New("checksum mismatched")
			}
		}
		return nil
	})

	span.Infof("to delete conn:%v", conn)
	runTimer(conn, "del", 0, func() error {
		_, err = conn.api.Delete(ctx, &access.DeleteArgs{
			Locations: []access.Location{location},
		})
		return err
	})
}
