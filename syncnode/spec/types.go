// Copyright 2026 The CubeFS Authors.
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

// Package spec carries the on-the-wire and on-disk configuration shapes for
// syncnode rules. Pulled out of the `syncnode` package so subpackages
// (rules, tasks, scheduler) can reference these types without an import
// cycle back into syncnode itself.
//
// These are plain data types — no logic, no validation, no I/O. Validation
// lives in syncnode/config.go (operating on these types via aliases).
package spec

// RuleConfig is the on-disk schema for a single sync rule.
type RuleConfig struct {
	ID                 string          `json:"id"`
	Type               string          `json:"type"`
	Schedule           string          `json:"schedule"`
	Src                EndpointConfig  `json:"src"`
	Dst                EndpointConfig  `json:"dst"`
	Filter             FilterConfig    `json:"filter"`
	Retention          RetentionConfig `json:"retention"`
	AfterCopy          string          `json:"afterCopy"`
	DownloadStrategy   string          `json:"downloadStrategy"`
	OnMismatch         string          `json:"onMismatch"`
	SampleStrategy     string          `json:"sampleStrategy"`
	SampleRate         float64         `json:"sampleRate"`
	BandwidthLimitMBps int             `json:"bandwidthLimitMBps"`
	Parallelism        int             `json:"parallelism"`
	ShardingStrategy   string          `json:"shardingStrategy"`
}

// EndpointConfig describes one source or destination of a rule. The fields
// used depend on Kind: cfs uses Vol+Path; s3 uses Bucket+Prefix+Endpoint+
// Region+StorageClass; local uses Path + buffer hints.
type EndpointConfig struct {
	Kind string `json:"kind"`
	// cfs fields
	Vol  string `json:"vol"`
	Path string `json:"path"`
	// s3 fields
	Bucket       string `json:"bucket"`
	Prefix       string `json:"prefix"`
	Endpoint     string `json:"endpoint"`
	Region       string `json:"region"`
	StorageClass string `json:"storageClass"`
	// local fields (any host-mounted POSIX path)
	BufferSizeKiB     int  `json:"bufferSizeKiB"`
	Concurrency       int  `json:"concurrency"`
	DirectIO          bool `json:"directIO"`
	FadviseSequential bool `json:"fadviseSequential"`
}

// FilterConfig is the JSON shape of executor.Filter. Sizes / durations are
// strings ("1MB", "30s") at the boundary; ParseFilter converts.
type FilterConfig struct {
	Include []string `json:"include"`
	Exclude []string `json:"exclude"`
	MinSize string   `json:"minSize"`
	MaxSize string   `json:"maxSize"`
	MinAge  string   `json:"minAge"`
	MaxAge  string   `json:"maxAge"`
}

// RetentionConfig is the JSON shape of executor.Retention.
type RetentionConfig struct {
	Pattern    string `json:"pattern"`
	KeepLast   int    `json:"keepLast"`
	KeepWithin string `json:"keepWithin"`
}
