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

package rpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSelector_Base(t *testing.T) {
	cfg := newCfg([]string{"http://127.0.0.1:8888", "http://127.0.0.1:8888"},
		[]string{"http://127.0.0.1:8988", "http://127.0.0.1:8998"})
	cfg.HostTryTimes = 3
	cfg.FailRetryIntervalS = 1
	cfg.MaxFailsPeriodS = 1
	s := newSelector(cfg)

	sel, ok := s.(*selector)
	require.Equal(t, true, ok)
	require.NotNil(t, sel)
	require.Equal(t, 4, len(s.GetAvailableHosts()))

	// test set fail
	s.SetFail("http://127.0.0.1:8988")
	s.SetFail("http://127.0.0.1:8988")
	s.SetFail("http://127.0.0.1:8988")
	require.Equal(t, 3, len(s.GetAvailableHosts()))

	// test enable host
	for key := range sel.crackHosts {
		sel.enableHost(key)
	}
	require.Equal(t, 4, len(s.GetAvailableHosts()))

	// test detect
	s.SetFail("http://127.0.0.1:8888")
	s.SetFail("http://127.0.0.1:8888")
	s.SetFail("http://127.0.0.1:8888")
	time.Sleep(time.Second * 2)
	require.Equal(t, 4, len(s.GetAvailableHosts()))
}
