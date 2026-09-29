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

package config

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestLoadConfigLoadExtendCodemode(t *testing.T) {
	const extendCodeMode = codemode.CodeMode(250)

	extends := []codemode.ExtendCodeMode{{
		CodeMode: extendCodeMode,
		Name:     "EC2P2",
		Tactic: codemode.Tactic{
			N: 2, M: 2, L: 0, AZCount: 1,
			PutQuorum: 3, GetQuorum: 0, MinShardSize: 2048,
		},
	}}
	raw, err := json.Marshal(extends)
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/config/get", r.URL.Path)
		require.Equal(t, proto.CodeModeExtendKey, r.URL.Query().Get("key"))

		resp, err := json.Marshal(string(raw))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(resp)
	}))
	defer srv.Close()

	confPath := filepath.Join(t.TempDir(), "cli.conf")
	confFile, err := os.Create(confPath)
	require.NoError(t, err)
	require.NoError(t, json.NewEncoder(confFile).Encode(map[string]interface{}{
		"default_cluster_id": 1,
		"cm_cluster": map[string][]string{
			"1": {srv.URL},
		},
	}))
	require.NoError(t, confFile.Close())

	require.NotPanics(t, func() { LoadConfig(confPath) })

	require.True(t, extendCodeMode.IsValid())
	require.Equal(t, "EC2P2", extendCodeMode.String())
	require.Equal(t, 2, extendCodeMode.Tactic().N)
	require.Equal(t, 2, extendCodeMode.Tactic().M)
}
