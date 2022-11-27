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

package common_test

import (
	"io"
	"os"
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

func init() {
	if os.Getenv("JENKINS_TEST") != "" {
		color.Output = io.Discard
		fmt.SetOutput(io.Discard)
	}
}

func TestCmdCommonContext(t *testing.T) {
	for range [3]struct{}{} {
		span := trace.SpanFromContext(common.CmdContext())
		t.Log(span)
		span.Info("infoooooo bar")
	}
}

func TestCmdCommonConfirm(t *testing.T) {
	require.False(t, common.Confirm("test confirm?"))
}
