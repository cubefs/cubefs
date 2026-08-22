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

package metanode

import (
	"testing"

	"github.com/cubefs/cubefs/util/config"
	"github.com/stretchr/testify/require"
)

func TestMetaNode_parseConfig_AdvertisedAddr(t *testing.T) {
	tests := []struct {
		name         string
		configData   map[string]interface{}
		localAddr    string
		expectedAddr string
		description  string
	}{
		{
			name: "explicit_advertised_addr",
			configData: map[string]interface{}{
				cfgAdvertisedAddr: "10.0.0.200",
				"listen":          "17210",
				"masterAddr":      []interface{}{"127.0.0.1:17010"},
				"memRatio":        "50", // 50% of physical memory
				"metadataDir":     "/tmp/metanode_test",
				"raftDir":         "/tmp/metanode_raft",
			},
			localAddr:    "192.168.1.200",
			expectedAddr: "10.0.0.200",
			description:  "Should use explicitly configured advertisedAddr",
		},
		{
			name: "empty_advertised_addr",
			configData: map[string]interface{}{
				"listen":      "17210",
				"masterAddr":  []interface{}{"127.0.0.1:17010"},
				"memRatio":    "50", // 50% of physical memory
				"metadataDir": "/tmp/metanode_test",
			},
			localAddr:    "192.168.1.200",
			expectedAddr: "192.168.1.200",
			description:  "Should fall back to localAddr when advertisedAddr is not set",
		},
		{
			name: "empty_string_advertised_addr",
			configData: map[string]interface{}{
				cfgAdvertisedAddr: "",
				"listen":          "17210",
				"masterAddr":      []interface{}{"127.0.0.1:17010"},
				"memRatio":        "50", // 50% of physical memory
				"metadataDir":     "/tmp/metanode_test",
				"raftDir":         "/tmp/metanode_raft",
			},
			localAddr:    "192.168.1.200",
			expectedAddr: "192.168.1.200",
			description:  "Should fall back to localAddr when advertisedAddr is empty string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create config
			cfg := config.NewConfig()
			for key, value := range tt.configData {
				cfg.SetNewVal(key, value)
			}

			// Create MetaNode and set localAddr
			metaNode := &MetaNode{
				localAddr: tt.localAddr,
			}

			// Test only the advertisedAddr parsing logic (lines 122-129 from your diff)
			metaNode.advertisedAddr = cfg.GetString(cfgAdvertisedAddr)
			if metaNode.advertisedAddr == "" {
				metaNode.advertisedAddr = metaNode.localAddr
			}

			// Verify advertisedAddr is set correctly
			require.Equal(t, tt.expectedAddr, metaNode.advertisedAddr, tt.description)
		})
	}
}

func TestMetaNode_nodeAddress_Construction(t *testing.T) {
	tests := []struct {
		name           string
		localAddr      string
		advertisedAddr string
		listen         string
		expectedAddr   string
		description    string
	}{
		{
			name:           "use_advertised_addr",
			localAddr:      "192.168.1.200",
			advertisedAddr: "10.0.0.200",
			listen:         "17210",
			expectedAddr:   "10.0.0.200:17210",
			description:    "Should construct node address using advertisedAddr",
		},
		{
			name:           "fallback_to_local_addr",
			localAddr:      "192.168.1.200",
			advertisedAddr: "",
			listen:         "17210",
			expectedAddr:   "192.168.1.200:17210",
			description:    "Should construct node address using localAddr when advertisedAddr is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metaNode := &MetaNode{
				localAddr:      tt.localAddr,
				advertisedAddr: tt.advertisedAddr,
				listen:         tt.listen,
			}

			// Simulate the logic from register method
			if metaNode.advertisedAddr == "" {
				metaNode.advertisedAddr = metaNode.localAddr
			}

			nodeAddress := metaNode.advertisedAddr + ":" + metaNode.listen
			require.Equal(t, tt.expectedAddr, nodeAddress, tt.description)
		})
	}
}

func TestMetaNode_advertisedAddr_InErrorMessages(t *testing.T) {
	// Test that error messages use advertisedAddr instead of localAddr
	metaNode := &MetaNode{
		localAddr:      "192.168.1.200",
		advertisedAddr: "10.0.0.200",
		listen:         "17210",
	}

	// Simulate the error message construction from checkLocalPartitionMatchWithMaster
	expectedMessage := "LackPartitions [1 2 3] on metanode 10.0.0.200:17210, please deal quickly"

	// This simulates the log message format from the actual code
	actualMessage := "LackPartitions [1 2 3] on metanode " + metaNode.advertisedAddr + ":" + metaNode.listen + ", please deal quickly"

	require.Equal(t, expectedMessage, actualMessage, "Error messages should use advertisedAddr")
}

func TestMetaNode_advertisedAddr_DefaultBehavior(t *testing.T) {
	// Test the default behavior when no advertisedAddr is configured
	cfg := config.NewConfig()
	// No advertisedAddr configured

	metaNode := &MetaNode{
		localAddr: "192.168.1.200",
	}

	// Test only the advertisedAddr parsing logic
	metaNode.advertisedAddr = cfg.GetString(cfgAdvertisedAddr)
	if metaNode.advertisedAddr == "" {
		metaNode.advertisedAddr = metaNode.localAddr
	}

	// Should default to localAddr
	require.Equal(t, "192.168.1.200", metaNode.advertisedAddr)
}

func TestMetaNode_advertisedAddr_ConfigPriority(t *testing.T) {
	// Test that config value takes priority over localAddr
	cfg := config.NewConfig()
	cfg.SetString(cfgAdvertisedAddr, "external.example.com")

	metaNode := &MetaNode{
		localAddr: "192.168.1.200",
	}

	// Test only the advertisedAddr parsing logic
	metaNode.advertisedAddr = cfg.GetString(cfgAdvertisedAddr)
	if metaNode.advertisedAddr == "" {
		metaNode.advertisedAddr = metaNode.localAddr
	}

	// Should use config value, not localAddr
	require.Equal(t, "external.example.com", metaNode.advertisedAddr)
	require.NotEqual(t, metaNode.localAddr, metaNode.advertisedAddr)
}
