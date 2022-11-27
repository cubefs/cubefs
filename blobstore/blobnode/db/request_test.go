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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"

	rdb "github.com/cubefs/cubefs/blobstore/common/kvstore"
)

func TestRequest_Register(t *testing.T) {
	req := Request{
		Type: msgPut,
		Data: rdb.KV{Key: []byte{0x1}, Value: []byte{0x2}},
	}

	ch := req.Register()

	go func() {
		req.Trigger(Result{err: ErrStopped})
	}()

	x := <-ch
	require.Error(t, x.err)
}
