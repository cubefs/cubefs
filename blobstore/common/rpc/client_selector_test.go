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
	host1 := "http://127.0.0.1:8888"
	host3 := "http://127.0.0.1:8988"
	host4 := "http://127.0.0.1:8998"
	cfg := newCfg([]string{host1, host1}, []string{host3, host4})
	cfg.HostTryTimes = 3
	cfg.FailRetryIntervalS = 1
	cfg.MaxFailsPeriodS = 1
	s := NewSelector(cfg)
	defer s.Close()

	hosts := s.GetAllHosts()
	require.Equal(t, 4, len(hosts))

	sel, ok := s.(*selector)
	require.Equal(t, true, ok)
	require.NotNil(t, sel)
	require.Equal(t, 4, len(s.GetAvailableHosts()))

	// test set fail
	failHost := hosts[3]
	for range [3]struct{}{} {
		s.SetFailHost(failHost)
	}
	require.Equal(t, 3, len(s.GetAvailableHosts()))

	// test enable host
	for key := range sel.unavailHosts {
		sel.enableHost(key)
		require.Equal(t, failHost.Host(), key.rawHost)
	}
	require.Equal(t, 4, len(s.GetAvailableHosts()))

	// test detect
	for range [3]struct{}{} {
		s.SetFailHost(failHost)
	}
	time.Sleep(time.Millisecond * 1200)
	require.Equal(t, 4, len(s.GetAvailableHosts()))
}
