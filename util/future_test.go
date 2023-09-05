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

package util_test

import (
	"errors"
	"testing"

	"github.com/cubefs/cubefs/util"
)

func TestFuture(t *testing.T) {
	future := util.NewFuture()
	future.Respond(1, nil)
	val, err := future.Response()
	if err != nil {
		t.Errorf("err should be nil")
		return
	}
	if val.(int) != 1 {
		t.Errorf("val should be 1")
		return
	}
	future = util.NewFuture()
	future.Respond(nil, errors.New("future error"))
	_, err = future.Response()
	if err == nil {
		t.Errorf("error should be non-nil")
		return
	}
}
