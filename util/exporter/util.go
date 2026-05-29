// Copyright 2018 The CubeFS Authors.
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

package exporter

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"
)

const (
	maxStackLabelKeys = 16
	fnvOffset64       = 2166136261
	fnvPrime64        = 1099511628211
)

var (
	metricKeyMu     sync.RWMutex
	metricKeyByHash = make(map[uint64]string)

	labelKeysPool = sync.Pool{New: func() any {
		s := make([]string, 0, 2*maxStackLabelKeys)
		return &s
	}}
)

func fnv64AddString(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return h
}

func fnv64AddByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= fnvPrime64
	return h
}

// sortedLabelKeys returns sorted label keys. For n<=maxStackLabelKeys the backing
// array is stack-allocated; otherwise a slice from labelKeysPool is used.
// poolSlice is non-nil when the caller must call releaseLabelKeys after use.
func sortedLabelKeys(labels map[string]string) (keys []string, poolSlice *[]string) {
	n := len(labels)
	if n <= maxStackLabelKeys {
		var stackKeys [maxStackLabelKeys]string
		keys = stackKeys[:0]
	} else {
		poolSlice = labelKeysPool.Get().(*[]string)
		*poolSlice = (*poolSlice)[:0]
		keys = *poolSlice
	}
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, poolSlice
}

func releaseLabelKeys(poolSlice *[]string) {
	if poolSlice == nil {
		return
	}
	*poolSlice = (*poolSlice)[:0]
	labelKeysPool.Put(poolSlice)
}

func hashNameAndLabels(name string, keys []string, labels map[string]string) uint64 {
	h := fnv64AddString(fnvOffset64, name)
	for _, k := range keys {
		h = fnv64AddString(h, k)
		h = fnv64AddByte(h, 0)
		h = fnv64AddString(h, labels[k])
		h = fnv64AddByte(h, 0)
	}
	return h
}

func buildLabelsFingerprint(keys []string, labels map[string]string) string {
	var b strings.Builder
	b.Grow(len(labels) * 16)
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
		b.WriteByte(0)
	}
	return b.String()
}

func joinNameLabelsFP(name, labelsFP string) string {
	var b strings.Builder
	b.Grow(len(name) + len(labelsFP) + 1)
	b.WriteString(name)
	b.WriteByte(0)
	b.WriteString(labelsFP)
	return b.String()
}

// labelsMetricKey returns a cached stable string key for sync.Map.
// Cache hits avoid heap allocation; the key string is built on heap only once per unique name+labels.
func labelsMetricKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}

	keys, poolSlice := sortedLabelKeys(labels)
	if poolSlice != nil {
		defer releaseLabelKeys(poolSlice)
	}

	h := hashNameAndLabels(name, keys, labels)

	metricKeyMu.RLock()
	key, ok := metricKeyByHash[h]
	metricKeyMu.RUnlock()
	if ok {
		return key
	}

	labelsFP := buildLabelsFingerprint(keys, labels)
	key = joinNameLabelsFP(name, labelsFP)

	metricKeyMu.Lock()
	if existing, ok := metricKeyByHash[h]; ok {
		metricKeyMu.Unlock()
		return existing
	}
	metricKeyByHash[h] = key
	metricKeyMu.Unlock()
	return key
}

func stringMapToString(m map[string]string) string {
	mjson, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}

	return string(mjson)
}
