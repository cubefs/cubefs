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

package iostat

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
)

var ErrInvalidParam = errors.New("iostat: invalid params")

func PurgeStatFile(ctx context.Context, dirname string, suffix string, purgeFunc func(path string) error) (
	deleted []string, err error,
) {
	if purgeFunc == nil || dirname == "" || suffix == "" {
		return deleted, ErrInvalidParam
	}

	// /dev/shm/*.iostat
	matchPattern := fmt.Sprintf("*.%s", suffix)

	fnames, err := filepath.Glob(filepath.Join(dirname, matchPattern))
	if err != nil {
		return
	}

	for _, n := range fnames {
		if err = purgeFunc(n); err == nil {
			deleted = append(deleted, n)
		}
	}
	return deleted, nil
}
