// Copyright 2025 The CubeFS Authors.
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

package master

import (
	"encoding/json"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

// -------------------------------------------------------------------------
// ApiLimitInfo.InitLimiter
// -------------------------------------------------------------------------

func TestApiLimitInfo_InitLimiter(t *testing.T) {
	info := &ApiLimitInfo{
		ApiName:        "admingetcluster",
		QueryPath:      proto.AdminGetCluster,
		Limit:          10,
		LimiterTimeout: 5,
	}
	require.Nil(t, info.Limiter)
	info.InitLimiter()
	require.NotNil(t, info.Limiter)

	// The limiter should accept at least one token right away (burst = 1).
	require.True(t, info.Limiter.Allow())
}

// -------------------------------------------------------------------------
// newApiLimiter
// -------------------------------------------------------------------------

func TestNewApiLimiter(t *testing.T) {
	l := newApiLimiter()
	require.NotNil(t, l)
	require.Empty(t, l.limiterInfos)
}

// -------------------------------------------------------------------------
// clear (unexported) and Clear (exported)
// -------------------------------------------------------------------------

func TestApiLimiter_Clear(t *testing.T) {
	l := newApiLimiter()

	// Populate the internal map via the unexported clear helper first to
	// confirm it can delete entries.
	l.limiterInfos["key1"] = &ApiLimitInfo{}
	l.limiterInfos["key2"] = &ApiLimitInfo{}
	require.Len(t, l.limiterInfos, 2)

	l.clear()
	require.Empty(t, l.limiterInfos)
}

func TestApiLimiter_Clear_Public(t *testing.T) {
	l := newApiLimiter()
	l.limiterInfos[proto.AdminGetCluster] = &ApiLimitInfo{}
	require.Len(t, l.limiterInfos, 1)

	l.Clear()
	require.Empty(t, l.limiterInfos)
}

func TestApiLimiter_Clear_AlreadyEmpty(t *testing.T) {
	l := newApiLimiter()
	// Clearing an already-empty limiter must not panic.
	require.NotPanics(t, func() { l.Clear() })
}

// -------------------------------------------------------------------------
// Replace
// -------------------------------------------------------------------------

func TestApiLimiter_Replace_WithEntries(t *testing.T) {
	l := newApiLimiter()

	// Seed old data.
	l.limiterInfos["old"] = &ApiLimitInfo{}

	incoming := map[string]*ApiLimitInfo{
		proto.AdminGetCluster: {ApiName: "admingetcluster", QueryPath: proto.AdminGetCluster, Limit: 5},
		proto.AdminGetIP:      {ApiName: "admingetip", QueryPath: proto.AdminGetIP, Limit: 10},
	}

	l.Replace(incoming)

	require.Len(t, l.limiterInfos, 2)
	_, oldPresent := l.limiterInfos["old"]
	require.False(t, oldPresent, "old entry must be removed after Replace")
}

func TestApiLimiter_Replace_EmptyMap(t *testing.T) {
	l := newApiLimiter()
	l.limiterInfos["existing"] = &ApiLimitInfo{}

	l.Replace(map[string]*ApiLimitInfo{})
	require.Empty(t, l.limiterInfos)
}

// -------------------------------------------------------------------------
// IsApiNameValid
// -------------------------------------------------------------------------

func TestApiLimiter_IsApiNameValid_Known(t *testing.T) {
	l := newApiLimiter()

	// "admingetcluster" is a key in proto.GApiInfo.
	normalizedName, qPath, err := l.IsApiNameValid("admingetcluster")
	require.NoError(t, err)
	require.Equal(t, "admingetcluster", normalizedName)
	require.Equal(t, proto.AdminGetCluster, qPath)
}

func TestApiLimiter_IsApiNameValid_CaseInsensitive(t *testing.T) {
	l := newApiLimiter()

	// The input is uppercased; IsApiNameValid should normalise it.
	_, qPath, err := l.IsApiNameValid("AdminGetCluster")
	require.NoError(t, err)
	require.Equal(t, proto.AdminGetCluster, qPath)
}

func TestApiLimiter_IsApiNameValid_Unknown(t *testing.T) {
	l := newApiLimiter()

	_, _, err := l.IsApiNameValid("__notavalidapi__")
	require.Error(t, err)
}

// -------------------------------------------------------------------------
// SetLimiter
// -------------------------------------------------------------------------

func TestApiLimiter_SetLimiter_ValidAPI(t *testing.T) {
	l := newApiLimiter()

	err := l.SetLimiter("admingetcluster", 100, 5)
	require.NoError(t, err)
	require.Len(t, l.limiterInfos, 1)

	info, ok := l.limiterInfos[proto.AdminGetCluster]
	require.True(t, ok)
	require.Equal(t, uint32(100), info.Limit)
	require.Equal(t, uint32(5), info.LimiterTimeout)
	require.NotNil(t, info.Limiter)
}

func TestApiLimiter_SetLimiter_InvalidAPI(t *testing.T) {
	l := newApiLimiter()

	err := l.SetLimiter("__invalid_api__", 10, 5)
	require.Error(t, err)
	require.Empty(t, l.limiterInfos)
}

func TestApiLimiter_SetLimiter_OverwriteExisting(t *testing.T) {
	l := newApiLimiter()

	require.NoError(t, l.SetLimiter("admingetcluster", 50, 3))
	require.NoError(t, l.SetLimiter("admingetcluster", 200, 10))

	info := l.limiterInfos[proto.AdminGetCluster]
	require.Equal(t, uint32(200), info.Limit)
	require.Equal(t, uint32(10), info.LimiterTimeout)
}

func TestApiLimiter_SetLimiter_MultipleAPIs(t *testing.T) {
	l := newApiLimiter()

	require.NoError(t, l.SetLimiter("admingetcluster", 10, 1))
	require.NoError(t, l.SetLimiter("admingetip", 20, 2))
	require.NoError(t, l.SetLimiter("clientdatapartitions", 30, 3))

	require.Len(t, l.limiterInfos, 3)
}

// -------------------------------------------------------------------------
// RmLimiter
// -------------------------------------------------------------------------

func TestApiLimiter_RmLimiter_ExistingEntry(t *testing.T) {
	l := newApiLimiter()

	require.NoError(t, l.SetLimiter("admingetcluster", 10, 1))
	require.Len(t, l.limiterInfos, 1)

	err := l.RmLimiter("admingetcluster")
	require.NoError(t, err)
	require.Empty(t, l.limiterInfos)
}

func TestApiLimiter_RmLimiter_NonExistentEntry(t *testing.T) {
	l := newApiLimiter()

	// Removing an entry that was never added must not error (delete is a no-op).
	err := l.RmLimiter("admingetcluster")
	require.NoError(t, err)
}

func TestApiLimiter_RmLimiter_InvalidAPI(t *testing.T) {
	l := newApiLimiter()

	err := l.RmLimiter("__not_a_valid_api__")
	require.Error(t, err)
}

// -------------------------------------------------------------------------
// Wait
// -------------------------------------------------------------------------

func TestApiLimiter_Wait_NoLimiterRegistered(t *testing.T) {
	l := newApiLimiter()

	// When no limiter is registered for the path, Wait returns nil immediately.
	err := l.Wait(proto.AdminGetCluster)
	require.NoError(t, err)
}

func TestApiLimiter_Wait_TokenAvailable(t *testing.T) {
	l := newApiLimiter()
	require.NoError(t, l.SetLimiter("admingetcluster", 1000, 5))

	// High-limit limiter has tokens available; Wait must succeed promptly.
	err := l.Wait(proto.AdminGetCluster)
	require.NoError(t, err)
}

func TestApiLimiter_Wait_TimeoutExpired(t *testing.T) {
	l := newApiLimiter()

	// Limit = 1 QPS, LimiterTimeout = 0  →  context deadline is 0 seconds,
	// which expires before a token can be acquired.
	require.NoError(t, l.SetLimiter("admingetcluster", 1, 0))

	// Consume the single available burst token first.
	info := l.limiterInfos[proto.AdminGetCluster]
	info.Limiter.Allow()

	// Now there are no tokens and the timeout is 0 s → Wait must fail.
	err := l.Wait(proto.AdminGetCluster)
	require.Error(t, err)
}

// -------------------------------------------------------------------------
// IsFollowerLimiter
// -------------------------------------------------------------------------

func TestApiLimiter_IsFollowerLimiter_AdminGetIP(t *testing.T) {
	l := newApiLimiter()
	require.True(t, l.IsFollowerLimiter(proto.AdminGetIP))
}

func TestApiLimiter_IsFollowerLimiter_ClientDataPartitions(t *testing.T) {
	l := newApiLimiter()
	require.True(t, l.IsFollowerLimiter(proto.ClientDataPartitions))
}

func TestApiLimiter_IsFollowerLimiter_Other(t *testing.T) {
	l := newApiLimiter()
	require.False(t, l.IsFollowerLimiter(proto.AdminGetCluster))
	require.False(t, l.IsFollowerLimiter(""))
	require.False(t, l.IsFollowerLimiter("/some/other/path"))
}

// -------------------------------------------------------------------------
// updateLimiterInfoFromLeader
// -------------------------------------------------------------------------

func TestApiLimiter_UpdateLimiterInfoFromLeader_ValidJSON(t *testing.T) {
	l := newApiLimiter()

	// Build a valid JSON payload with one limiter entry.
	payload := map[string]*ApiLimitInfo{
		proto.AdminGetCluster: {
			ApiName:        "admingetcluster",
			QueryPath:      proto.AdminGetCluster,
			Limit:          50,
			LimiterTimeout: 3,
		},
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	l.updateLimiterInfoFromLeader(data)

	require.Len(t, l.limiterInfos, 1)
	info := l.limiterInfos[proto.AdminGetCluster]
	require.NotNil(t, info)
	// InitLimiter must have been called by updateLimiterInfoFromLeader.
	require.NotNil(t, info.Limiter)
}

func TestApiLimiter_UpdateLimiterInfoFromLeader_MultipleEntries(t *testing.T) {
	l := newApiLimiter()

	payload := map[string]*ApiLimitInfo{
		proto.AdminGetCluster: {ApiName: "admingetcluster", QueryPath: proto.AdminGetCluster, Limit: 10},
		proto.AdminGetIP:      {ApiName: "admingetip", QueryPath: proto.AdminGetIP, Limit: 20},
	}
	data, _ := json.Marshal(payload)
	l.updateLimiterInfoFromLeader(data)

	require.Len(t, l.limiterInfos, 2)
	// Both entries must have their Limiter initialised.
	for _, info := range l.limiterInfos {
		require.NotNil(t, info.Limiter)
	}
}

func TestApiLimiter_UpdateLimiterInfoFromLeader_InvalidJSON(t *testing.T) {
	l := newApiLimiter()

	// Seed an existing entry to confirm it is NOT replaced on JSON error.
	l.limiterInfos[proto.AdminGetCluster] = &ApiLimitInfo{}

	// Invalid JSON → unmarshal fails → map is left unchanged.
	l.updateLimiterInfoFromLeader([]byte("not-json"))
	require.Len(t, l.limiterInfos, 1, "map must be unchanged on unmarshal error")
}

func TestApiLimiter_UpdateLimiterInfoFromLeader_EmptyPayload(t *testing.T) {
	l := newApiLimiter()

	// Valid JSON for an empty map → limiterInfos is replaced with empty map.
	l.updateLimiterInfoFromLeader([]byte("{}"))
	require.Empty(t, l.limiterInfos)
}

// -------------------------------------------------------------------------
// Concurrency smoke test
// -------------------------------------------------------------------------

func TestApiLimiter_ConcurrentSetAndWait(t *testing.T) {
	l := newApiLimiter()

	// Concurrently set a limiter and call Wait to exercise lock paths.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			l.SetLimiter("admingetcluster", 1000, 5) //nolint:errcheck
		}
	}()

	for i := 0; i < 20; i++ {
		l.Wait(proto.AdminGetCluster) //nolint:errcheck
	}
	<-done
}
