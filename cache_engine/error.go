// Copyright 2018 The CubeFS Authors.
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

package cache_engine

import (
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
)

var (
	CacheSizeOverflowsError = errors.New("cache size overflows")
	CacheClosedError        = errors.New("cache is closed")
)

func NewParameterMismatchErr(msg string) (err error) {
	err = fmt.Errorf("parameter mismatch error: %s", msg)
	return
}
