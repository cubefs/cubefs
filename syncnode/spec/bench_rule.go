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

package spec

// BenchStorageType identifies which storage system the benchmark targets.
type BenchStorageType string

const (
	BenchStorageS3    BenchStorageType = "s3"
	BenchStoragePosix BenchStorageType = "posix"
	BenchStorageSDK   BenchStorageType = "sdk"
)

// BenchRule is the persisted configuration for a distributed benchmark task.
// StorageType selects the executor path (S3/SDK object ops vs POSIX fio).
// Parallelism controls the number of shards dispatched across nodes.
type BenchRule struct {
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	StorageType BenchStorageType `json:"storageType"` // "s3" | "posix" | "sdk"
	Parallelism int              `json:"parallelism"` // shard count
	Output      BenchOutput      `json:"output"`
	CreatedAt   int64            `json:"createdAt"`
	UpdatedAt   int64            `json:"updatedAt"`

	// S3 / SDK only
	BackendID string     `json:"backendID,omitempty"`
	KeyPrefix string     `json:"keyPrefix,omitempty"`
	Stages    []ObjStage `json:"stages,omitempty"`

	// POSIX only
	MountPath   string     `json:"mountPath,omitempty"`
	FIODefaults FIOConfig  `json:"fioDefaults,omitempty"`
	FIOStages   []FIOStage `json:"fioStages,omitempty"`

	// BackendEndpoint is resolved at dispatch time by the master / dashboard
	// from BackendID. The syncnode uses it to build the actual backend client.
	// Not persisted; populated transiently in the dispatch payload.
	BackendEndpoint *EndpointConfig `json:"backendEndpoint,omitempty"`
}

// ObjStage is one phase of an S3/SDK benchmark (e.g. write, read, delete).
type ObjStage struct {
	Name       string   `json:"name"`
	Ops        []ObjOp  `json:"ops"`
	NumJobs    int      `json:"numjobs"`
	Runtime    int      `json:"runtime"`
	NumObjects int      `json:"numObjects"`
	ObjectSize ObjSize  `json:"objectSize"`
	DeleteAll  bool     `json:"deleteAll,omitempty"`
}

// ObjOp is one operation type within a stage, with a relative weight used
// to distribute worker goroutines across op types.
type ObjOp struct {
	Type   string `json:"type"`   // "put" | "get" | "delete" | "head" | "list"
	Weight int    `json:"weight"`
}

// ObjSize configures object sizes for a stage. Exactly one of Fixed or
// (Min+Max+Dist) should be set.
type ObjSize struct {
	Fixed int64  `json:"fixed,omitempty"`
	Min   int64  `json:"min,omitempty"`
	Max   int64  `json:"max,omitempty"`
	Dist  string `json:"dist,omitempty"` // "fixed" | "uniform" | "pareto"
}

// FIOConfig holds default fio parameters applied to all POSIX stages unless
// a stage provides its own override value.
type FIOConfig struct {
	IOEngine         string `json:"ioengine"`
	IODepth          int    `json:"iodepth"`
	NumJobs          int    `json:"numjobs"`
	BS               string `json:"bs,omitempty"`
	Size             string `json:"size"`
	Runtime          int    `json:"runtime"`
	Direct           int    `json:"direct"`
	ExtraArgs        string `json:"extraArgs,omitempty"`
	CleanupAfterDone bool   `json:"cleanupAfterDone,omitempty"`
}

// FIOStage is one fio workload phase. Non-zero / non-empty fields override
// the corresponding FIODefaults value.
type FIOStage struct {
	Name        string `json:"name"`
	RW          string `json:"rw"`
	BS          string `json:"bs"`
	RWMixRead   int    `json:"rwmixread,omitempty"`
	ReuseFiles  bool   `json:"reuseFiles"`
	SourceStage string `json:"sourceStage,omitempty"`
	Skip        bool   `json:"skip,omitempty"`
	IODepth     int    `json:"iodepth,omitempty"`
	NumJobs     int    `json:"numjobs,omitempty"`
	Size        string `json:"size,omitempty"`
	Runtime     int    `json:"runtime,omitempty"`
	Direct      int    `json:"direct,omitempty"`
}

// BenchOutput specifies which percentiles to include in the reported results.
type BenchOutput struct {
	Percentiles []float64 `json:"percentiles"`
}
