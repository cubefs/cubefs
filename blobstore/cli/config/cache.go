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

import "strings"

var cacher = make(map[string]interface{})

// All returns all cache
func All() map[string]interface{} {
	return cacher
}

// Set cache setter
func Set(key string, value interface{}) {
	cacher[key] = value
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
	clusters = make(map[string][]string)
	if addr := AccessConsulAddr(); addr != "" && Region() != "" {
		return getClustersFromConsul(addr, Region())
	}
	cs := Get("Key-ClusterMgrCluster").(map[string]string)
	for key, value := range cs {
		clusters[key] = strings.Split(value, " ")
	}
	return
}
func ClusterMgrSecret() string { return Get("Key-ClusterMgrSecret").(string) }

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
func DefaultClusterID() int { return Get("Key-DefaultClusterID").(int) }
