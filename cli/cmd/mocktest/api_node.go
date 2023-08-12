// Copyright 2023 The CubeFS Authors.
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

package mocktest

import (
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
)

type mockNodeAPI struct {
	master.NodeAPI
}

func (api *mockNodeAPI) GetDataNode(serverHost string) (node *proto.DataNodeInfo, err error) {
	return &proto.DataNodeInfo{
		Total:                     0,
		Used:                      0,
		AvailableSpace:            0,
		ID:                        0,
		ZoneName:                  "",
		Addr:                      "",
		DomainAddr:                "",
		ReportTime:                time.Time{},
		IsActive:                  false,
		IsWriteAble:               false,
		UsageRatio:                0,
		SelectedTimes:             0,
		Carry:                     0,
		DataPartitionReports:      []*proto.PartitionReport{},
		DataPartitionCount:        0,
		NodeSetID:                 0,
		PersistenceDataPartitions: []uint64{},
		BadDisks:                  []string{},
		RdOnly:                    false,
		MaxDpCntLimit:             0,
	}, nil
}

func (api *mockNodeAPI) GetMetaNode(serverHost string) (node *proto.MetaNodeInfo, err error) {
	return &proto.MetaNodeInfo{
		ID:                        0,
		Addr:                      "",
		DomainAddr:                "",
		IsActive:                  false,
		IsWriteAble:               false,
		ZoneName:                  "",
		MaxMemAvailWeight:         0,
		Total:                     0,
		Used:                      0,
		Ratio:                     0,
		SelectCount:               0,
		Carry:                     0,
		Threshold:                 0,
		ReportTime:                time.Time{},
		MetaPartitionCount:        0,
		NodeSetID:                 0,
		PersistenceMetaPartitions: nil,
		RdOnly:                    false,
	}, nil
}

func (api *mockNodeAPI) DataNodeDecommission(nodeAddr string, count int) (err error) {
	return nil
}

func (api *mockNodeAPI) MetaNodeDecommission(nodeAddr string, count int) (err error) {
	return nil
}

func (api *mockNodeAPI) DataNodeMigrate(srcAddr string, targetAddr string, count int) (err error) {
	return nil
}

func (api *mockNodeAPI) MetaNodeMigrate(srcAddr string, targetAddr string, count int) (err error) {
	return nil
}
