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

package iputil_test

import (
	"net"
	"testing"

	"github.com/cubefs/cubefs/util/iputil"
	"github.com/stretchr/testify/require"
)

// from iputil.GetDistance
func commonPrefixLen(first net.IP, second net.IP) int {
	return (iputil.GetDistance(first, second) - iputil.DEFAULT_MAX_DISTANCE) * -1
}

func TestCommonPrefixLength(t *testing.T) {
	firstV6 := net.ParseIP("fe80::1")
	secondV6 := net.ParseIP("fe80::2")
	require.Equal(t, 64, commonPrefixLen(firstV6, secondV6))
}
