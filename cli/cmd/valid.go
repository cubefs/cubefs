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

package cmd

import (
	"github.com/cubefs/cubefs/proto"
	sdk "github.com/cubefs/cubefs/sdk/master"
)

func validVols(client, complete interface{}) []string {
	var (
		validVols []string
		vols      []*proto.VolInfo
		err       error
	)
	clientSdk := client.(*sdk.MasterClient)
	completeStr := complete.(string)
	if vols, err = clientSdk.AdminAPI().ListVols(completeStr); err != nil {
		errout("Error: %v\n", err)
	}
	for _, vol := range vols {
		validVols = append(validVols, vol.Name)
	}
	return validVols
}

func validDataNodes(client *sdk.MasterClient, toComplete string) []string {
	var (
		validDataNodes []string
		clusterView    *proto.ClusterView

		err error
	)
	if clusterView, err = client.AdminAPI().GetCluster(); err != nil {
		errout("Error: %v\n", err)
	}
	for _, dn := range clusterView.DataNodes {
		validDataNodes = append(validDataNodes, dn.Addr)
	}
	return validDataNodes
}

func validMetaNodes(client *sdk.MasterClient, toComplete string) []string {
	var (
		validMetaNodes []string
		clusterView    *proto.ClusterView
		err            error
	)
	if clusterView, err = client.AdminAPI().GetCluster(); err != nil {
		errout("Error: %v\n", err)
	}
	for _, mn := range clusterView.MetaNodes {
		validMetaNodes = append(validMetaNodes, mn.Addr)
	}
	return validMetaNodes
}

func validUsers(client *sdk.MasterClient, toComplete string) []string {
	var (
		validUsers []string
		users      []*proto.UserInfo
		err        error
	)
	if users, err = client.UserAPI().ListUsers(toComplete); err != nil {
		errout("Error: %v\n", err)
	}
	for _, user := range users {
		validUsers = append(validUsers, user.UserID)
	}
	return validUsers
}

func validZones(client *sdk.MasterClient, toComplete string) []string {
	var (
		validZones []string
		zones      []*proto.ZoneView
		err        error
	)
	if zones, err = client.AdminAPI().ListZones(); err != nil {
		errout("Error: %v\n", err)
	}
	for _, zone := range zones {
		validZones = append(validZones, zone.Name)
	}
	return validZones
}
