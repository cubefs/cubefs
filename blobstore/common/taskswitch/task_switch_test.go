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

package taskswitch

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskSwitch(t *testing.T) {
	ts := newTaskSwitch()
	require.Equal(t, false, ts.Enabled())
	ts.Enable()
	require.Equal(t, true, ts.Enabled())
	ts.Disable()
	require.Equal(t, false, ts.Enabled())
}

type mockCfgGetter struct {
	m map[string]string
}

func (cfgGetter *mockCfgGetter) GetConfig(ctx context.Context, key string) (val string, err error) {
	if val, ok := cfgGetter.m[key]; ok {
		return val, nil
	}
	return "", errors.New("no such key")
}

func TestSwitchMgr(t *testing.T) {
	cfgGetter := mockCfgGetter{
		m: make(map[string]string),
	}
	cfgGetter.m["switch1"] = SwitchOpen
	cfgGetter.m["switch2"] = SwitchClose
	sm := NewSwitchMgr(&cfgGetter)
	s1, err := sm.AddSwitch("switch1")
	require.NoError(t, err)
	s2, err := sm.AddSwitch("switch2")
	require.NoError(t, err)

	sm.update()
	require.Equal(t, true, s1.Enabled())
	require.Equal(t, false, s2.Enabled())

	sm.update()
	err = sm.DelSwitch("switch1")
	require.NoError(t, err)
	err = sm.DelSwitch("switch2")
	require.NoError(t, err)
	require.Equal(t, 0, len(sm.switchs))
}
