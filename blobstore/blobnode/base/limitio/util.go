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

package limitio

import (
	"context"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// key is unexported and used for context.Context
type key int

const (
	_LimitTrack key = 0
)

func IsLimitTrack(ctx context.Context) bool {
	v := ctx.Value(_LimitTrack)
	if v == nil {
		return false
	}
	return v.(bool)
}

func SetLimitTrack(ctx context.Context) context.Context {
	return context.WithValue(ctx, _LimitTrack, true)
}

func AddTrackTag(ctx context.Context, name string) {
	if span := trace.SpanFromContext(ctx); span != nil {
		span.AppendRPCTrackLog([]string{name})
	}
}
