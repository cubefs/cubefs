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

package objectnode

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_MakeLoggingTimeName(t *testing.T) {
	cases := []struct {
		reqTime  time.Time
		gritMin  int
		expected string
	}{
		{
			reqTime:  time.Date(2006, 01, 02, 03, 04, 05, 0, time.UTC),
			gritMin:  5,
			expected: "2006-01-02/03-00-00",
		},
		{
			reqTime:  time.Date(2006, 01, 02, 03, 14, 05, 0, time.UTC),
			gritMin:  5,
			expected: "2006-01-02/03-10-00",
		},
		{
			reqTime:  time.Date(2006, 01, 02, 03, 15, 05, 0, time.UTC),
			gritMin:  7,
			expected: "2006-01-02/03-14-00",
		},
		{
			reqTime:  time.Date(2006, 01, 02, 03, 43, 05, 0, time.UTC),
			gritMin:  7,
			expected: "2006-01-02/03-42-00",
		},
		{
			reqTime:  time.Date(2006, 01, 02, 03, 13, 05, 0, time.UTC),
			gritMin:  40,
			expected: "2006-01-02/03-00-00",
		},
		{
			reqTime:  time.Date(2006, 01, 02, 03, 59, 05, 0, time.UTC),
			gritMin:  40,
			expected: "2006-01-02/03-40-00",
		},
		{
			reqTime:  time.Date(2006, 01, 02, 03, 59, 05, 0, time.UTC),
			gritMin:  65,
			expected: "2006-01-02/03-00-00",
		},
	}
	for _, c := range cases {
		require.Equal(t, c.expected, makeLoggingTimeName(c.reqTime, c.gritMin))
	}
}
