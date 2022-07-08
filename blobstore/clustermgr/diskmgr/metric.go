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

package diskmgr

import (
	"context"
	"reflect"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

var (
	spaceStatInfoMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "space_stat_info",
			Help:      "cluster space info",
		},
		[]string{"region", "cluster", "item", "is_leader"},
	)
	diskStatInfoMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "disk_stat_info",
			Help:      "cluster disk info",
		},
		[]string{"region", "cluster", "idc", "item", "is_leader"},
	)
	chunkStatInfoMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "blobstore",
			Subsystem: "clusterMgr",
			Name:      "chunk_stat_info",
			Help:      "cluster chunk info",
		},
		[]string{"region", "cluster", "idc", "item", "is_leader"},
	)
)

func init() {
	prometheus.MustRegister(spaceStatInfoMetric)
	prometheus.MustRegister(diskStatInfoMetric)
	prometheus.MustRegister(chunkStatInfoMetric)
}

func (d *DiskMgr) Report(ctx context.Context, region string, clusterID proto.ClusterID, isLeader string) {
	vec := spaceStatInfoMetric
	vec.Reset()
	spaceStatInfo := d.Stat(ctx)
	reflectTyes := reflect.TypeOf(*spaceStatInfo)
	reflectVals := reflect.ValueOf(*spaceStatInfo)
	for i := 0; i < reflectTyes.NumField(); i++ {
		kind := reflectTyes.Field(i).Type.Kind()
		if kind != reflect.Int64 {
			continue
		}
		fieldName := reflectTyes.Field(i).Name
		vec.WithLabelValues(region, clusterID.ToString(), fieldName, isLeader).Set(float64(reflectVals.FieldByName(fieldName).Interface().(int64)))
	}

	vecDisk := diskStatInfoMetric
	vecDisk.Reset()
	vecChunk := chunkStatInfoMetric
	vecChunk.Reset()
	for _, diskStatInfo := range spaceStatInfo.DisksStatInfos {
		reflectTyes = reflect.TypeOf(diskStatInfo)
		reflectVals = reflect.ValueOf(diskStatInfo)
		for i := 0; i < reflectTyes.NumField(); i++ {
			fieldName := reflectTyes.Field(i).Name
			kind := reflectTyes.Field(i).Type.Kind()

			switch kind {
			case reflect.Int:
				vecDisk.WithLabelValues(region, clusterID.ToString(), diskStatInfo.IDC, fieldName, isLeader).Set(float64(reflectVals.FieldByName(fieldName).Int()))
			case reflect.Int64:
				vecChunk.WithLabelValues(region, clusterID.ToString(), diskStatInfo.IDC, fieldName, isLeader).Set(float64(reflectVals.FieldByName(fieldName).Interface().(int64)))
			default:
				continue
			}
		}
	}
}
