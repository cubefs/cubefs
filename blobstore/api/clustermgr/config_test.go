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

package clustermgr

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type mockConfigGetter struct {
	extendCalls int32
	policyCalls int32
	extend      string
	policy      string
	extendErr   error
	policyErr   error
}

func (m *mockConfigGetter) GetConfig(_ context.Context, key string) (string, error) {
	switch key {
	case proto.CodeModeExtendKey:
		atomic.AddInt32(&m.extendCalls, 1)
		return m.extend, m.extendErr
	case proto.CodeModeConfigKey:
		atomic.AddInt32(&m.policyCalls, 1)
		return m.policy, m.policyErr
	default:
		return "", nil
	}
}

func TestLoadPolicies(t *testing.T) {
	getter := &mockConfigGetter{
		extend: `[]`,
		policy: `[{"mode_name":"EC6P6","min_size":0,"max_size":1,"size_ratio":1,"enable":true}]`,
	}
	policies, err := LoadPolicies(context.Background(), getter)
	require.NoError(t, err)
	require.Len(t, policies, 1)
	require.Equal(t, int32(1), atomic.LoadInt32(&getter.extendCalls))
	require.Equal(t, int32(1), atomic.LoadInt32(&getter.policyCalls))
}

func TestEnsureExtendBuiltin(t *testing.T) {
	getter := &mockConfigGetter{extend: `[]`}
	require.NoError(t, EnsureExtend(context.Background(), getter, codemode.EC6P6))
	require.Equal(t, int32(0), atomic.LoadInt32(&getter.extendCalls))
}

func TestLoadPoliciesErrors(t *testing.T) {
	cases := []struct {
		name   string
		getter *mockConfigGetter
		// want is a substring of the expected error, empty means no error.
		want            string
		wantPolicyCalls int32
	}{
		{
			name:   "extend get failed",
			getter: &mockConfigGetter{extendErr: errors.New("cm unavailable")},
			want:   "load codemode extend failed",
		},
		{
			name:            "policy get failed",
			getter:          &mockConfigGetter{extend: `[]`, policyErr: errors.New("cm unavailable")},
			want:            "load codemode policy failed",
			wantPolicyCalls: 1,
		},
		{
			name:            "policy parse failed",
			getter:          &mockConfigGetter{extend: `[]`, policy: `not-a-json`},
			want:            "parse codemode policy failed",
			wantPolicyCalls: 1,
		},
		{
			name:            "unknown mode_name",
			getter:          &mockConfigGetter{extend: `[]`, policy: `[{"mode_name":"NOT_REGISTERED","min_size":0,"max_size":1,"size_ratio":1,"enable":true}]`},
			want:            "unknown mode_name",
			wantPolicyCalls: 1,
		},
		{
			name:            "empty policy is not rejected",
			getter:          &mockConfigGetter{extend: `[]`, policy: `[]`},
			want:            "",
			wantPolicyCalls: 1,
		},
	}
	for _, cs := range cases {
		t.Run(cs.name, func(t *testing.T) {
			policies, err := LoadPolicies(context.Background(), cs.getter)
			// extend is applied before the policy is fetched, so it is always pulled once.
			require.Equal(t, int32(1), atomic.LoadInt32(&cs.getter.extendCalls))
			require.Equal(t, cs.wantPolicyCalls, atomic.LoadInt32(&cs.getter.policyCalls))
			if cs.want == "" {
				require.NoError(t, err)
				require.Empty(t, policies)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), cs.want)
			require.Nil(t, policies)
		})
	}
}

func TestEnsureExtendReload(t *testing.T) {
	const (
		reloadable   = codemode.CodeMode(241)
		stillInvalid = codemode.CodeMode(242)
		brokenReload = codemode.CodeMode(243)
	)
	extendRaw, err := json.Marshal([]codemode.ExtendCodeMode{{
		CodeMode: reloadable,
		Name:     "EC3P3ENSURE_TEST",
		Tactic:   codemode.Tactic{N: 3, M: 3, AZCount: 3, PutQuorum: 5, MinShardSize: 1 << 20},
	}})
	require.NoError(t, err)

	t.Run("reloads extend and registers the requested mode", func(t *testing.T) {
		unregistered := !reloadable.IsValid()
		getter := &mockConfigGetter{extend: string(extendRaw)}
		require.NoError(t, EnsureExtend(context.Background(), getter, reloadable))
		require.True(t, reloadable.IsValid())
		if unregistered {
			require.Equal(t, int32(1), atomic.LoadInt32(&getter.extendCalls))
		}
	})

	t.Run("returns error when the mode stays unregistered", func(t *testing.T) {
		getter := &mockConfigGetter{extend: string(extendRaw)}
		err := EnsureExtend(context.Background(), getter, stillInvalid)
		require.Error(t, err)
		require.Contains(t, err.Error(), "still invalid after extend reload")
		require.False(t, stillInvalid.IsValid())
		require.Equal(t, int32(1), atomic.LoadInt32(&getter.extendCalls))
	})

	t.Run("propagates extend reload failure", func(t *testing.T) {
		getter := &mockConfigGetter{extendErr: errors.New("cm unavailable")}
		err := EnsureExtend(context.Background(), getter, brokenReload)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cm unavailable")
		require.Equal(t, int32(1), atomic.LoadInt32(&getter.extendCalls))
	})
}
