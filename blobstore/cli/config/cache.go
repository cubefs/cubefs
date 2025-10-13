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

import "strconv"

var cacher = make(map[string]interface{})

// All returns all cache
func All() map[string]interface{} {
	return cacher
}

// Set cache setter
func Set(key string, value interface{}) {
	switch key {
	case "Key-ClusterMgrCluster":
		cacher[key] = mapStringSlice(value)
	default:
		cacher[key] = value
	}
}

func mapStringSlice(value interface{}) map[string][]string {
	got, isconf := value.(map[string][]string)
	if isconf {
		return got
	}
	fromcmd := value.(map[string]interface{})
	got = make(map[string][]string)
	for k, v := range fromcmd {
		vl := v.([]interface{})
		for _, vv := range vl {
			got[k] = append(got[k], vv.(string))
		}
	}
	return got
}

// SetFrom value from string
func SetFrom(key, value string) {
	if valuer, ok := keyValuer[key]; ok {
		Set(key, valuer.Valuer(value))
	} else {
		Set(key, value)
	}
}

// Del del cache of key
func Del(key string) {
	delete(cacher, key)
}

// Get cache getter
func Get(key string) interface{} {
	return cacher[key]
}

// Verbose returns app verbose
func Verbose() bool {
	if val := Get("Flag-Verbose"); val != nil {
		if v, ok := val.(bool); ok {
			return v
		}
	}
	return false
}

// Vverbose returns app verbose verbose
func Vverbose() bool {
	if val := Get("Flag-Vverbose"); val != nil {
		if v, ok := val.(bool); ok {
			return v
		}
	}
	return false
}

// Clusters returns cluster manager clusters
func Clusters() (clusters map[string][]string) {
	clusters = Get("Key-ClusterMgrCluster").(map[string][]string)
	if len(clusters) == 0 {
		if addr := ClusterConsul(); addr != "" && Region() != "" {
			clusters = getClustersFromConsul(addr, Region())
		}
		Set("Key-ClusterMgrCluster", clusters)
	}

	if Get("Key-DefaultClusterID") == 0 {
		for id := range clusters {
			clusterID, _ := strconv.Atoi(id)
			Set("Key-DefaultClusterID", int(clusterID))
			break
		}
	}
	return
}
func ClusterMgrSecret() string { return Get("Key-ClusterMgrSecret").(string) }

func SDKConfigPath() string         { return Get("Key-SDK-ConfigPath").(string) }
func AccessConnMode() uint8         { return Get("Key-Access-ConnMode").(uint8) }
func AccessConsulAddr() string      { return Get("Key-Access-ConsulAddr").(string) }
func AccessServiceIntervalS() int   { return Get("Key-Access-ServiceIntervalS").(int) }
func AccessPriorityAddrs() []string { return Get("Key-Access-PriorityAddrs").([]string) }
func AccessMaxSizePutOnce() int64   { return Get("Key-Access-MaxSizePutOnce").(int64) }
func AccessMaxPartRetry() int       { return Get("Key-Access-MaxPartRetry").(int) }
func AccessMaxHostRetry() int       { return Get("Key-Access-MaxHostRetry").(int) }
func AccessFailRetryIntervalS() int { return Get("Key-Access-FailRetryIntervalS").(int) }
func AccessMaxFailsPeriodS() int    { return Get("Key-Access-MaxFailsPeriodS").(int) }
func AccessHostTryTimes() int       { return Get("Key-Access-HostTryTimes").(int) }

func Region() string        { return Get("Key-Region").(string) }
func ClusterConsul() string { return Get("Key-ClusterConsul").(string) }
func DefaultClusterID() int {
	if id := Get("Key-DefaultClusterID").(int); id > 0 {
		return id
	}
	Clusters()
	return Get("Key-DefaultClusterID").(int)
}
