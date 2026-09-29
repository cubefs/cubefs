// Copyright 2019 The CubeFS Authors.
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

package objectnode

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSignatureEqual(t *testing.T) {
	cases := []struct {
		expected string
		actual   string
	}{
		{"", ""},
		{"a", ""},
		{"", "a"},
		{"deadbeef", "deadbeef"},
		{"deadbeef", "xeadbeef"}, // differs at first byte
		{"deadbeef", "deadbeex"}, // differs at last byte
		{"deadbeef", "deadbeeff"},
	}
	for _, c := range cases {
		require.Equal(t, c.expected == c.actual, signatureEqual(c.expected, c.actual),
			"signatureEqual(%q, %q) must agree with string equality", c.expected, c.actual)
	}
}
