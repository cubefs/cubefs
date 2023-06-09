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

package fileutil_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/cubefs/cubefs/util/fileutil"
)

func TestExits(t *testing.T) {
	if fileutil.Exist("/not_exist") {
		t.Fail()
		return
	}
	tempDir := os.TempDir()
	tempFile := fmt.Sprintf("%v/exist", tempDir)
	file, err := os.Create(tempFile)
	if err != nil {
		t.Errorf("failed to create file %v error: %v\n", tempFile, err.Error())
		t.Fail()
		return
	}
	file.Close()
	if !fileutil.Exist(tempFile) {
		t.Fail()
		return
	}
	if !fileutil.ExistDir(tempDir) {
		t.Fail()
		return
	}
}
