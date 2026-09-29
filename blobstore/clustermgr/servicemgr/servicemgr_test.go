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

package servicemgr

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/stretchr/testify/require"
)

func TestServiceMgr(t *testing.T) {
	tmpDBPath := "/tmp/tmpservicenormaldb" + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := normaldb.OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()
	serviceTbl := normaldb.OpenServiceTable(db)

	testServiceName := "testService"
	otherTestServiceName := "otherTestService"
	testHostPrefix := "testHost-"
	for i := 1; i <= 10; i++ {
		host := testHostPrefix + strconv.Itoa(i)
		service := serviceNode{
			ClusterID: 1,
			Name:      testServiceName,
			Host:      host,
			Idc:       "",
			Timeout:   30,
			Expires:   time.Now().Add(time.Duration(60) * time.Second),
		}
		data, err := json.Marshal(service)
		require.NoError(t, err)

		err = serviceTbl.Put(testServiceName, host, data)
		require.NoError(t, err)
		err = serviceTbl.Put(otherTestServiceName, host, data)
		require.NoError(t, err)
	}

	serviceMgr := NewServiceMgr(serviceTbl)
	serviceMgr.SetModuleName("")
	serviceMgr.GetModuleName()
	serviceMgr.NotifyLeaderChange(context.Background(), 0, "")

	info := serviceMgr.GetServiceInfo(testServiceName)
	require.Equal(t, 10, len(info.Nodes))
}

func TestServiceMgr_Apply(t *testing.T) {
	tmpDBPath := "/tmp/tmpservicenormaldb" + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := normaldb.OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()
	serviceTbl := normaldb.OpenServiceTable(db)
	serviceMgr := NewServiceMgr(serviceTbl)

	serviceName := "testService"
	hostPrefix := "testHost-"

	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	{
		operTypes := make([]int32, 0)
		datas := make([][]byte, 0)
		ctxs := make([]base.ProposeContext, 0)
		for i := 0; i < 3; i++ {
			data, err := json.Marshal(&clustermgr.RegisterArgs{
				ServiceNode: clustermgr.ServiceNode{ClusterID: 1, Name: serviceName, Host: hostPrefix + strconv.Itoa(i), Idc: ""},
				Timeout:     30,
			})
			require.NoError(t, err)
			datas = append(datas, data)
			operTypes = append(operTypes, OpRegister)
			ctxs = append(ctxs, base.ProposeContext{ReqID: span.TraceID()})
		}

		err = serviceMgr.Apply(ctx, operTypes, datas, ctxs)
		require.NoError(t, err)

		info := serviceMgr.GetServiceInfo(serviceName)
		require.Equal(t, 3, len(info.Nodes))
	}

	{
		data, err := json.Marshal(&clustermgr.UnregisterArgs{
			Name: serviceName,
			Host: hostPrefix + strconv.Itoa(1),
		})
		require.NoError(t, err)

		err = serviceMgr.Apply(ctx, []int32{OpUnregister}, [][]byte{data}, []base.ProposeContext{{ReqID: span.TraceID()}})
		require.NoError(t, err)

		info := serviceMgr.GetServiceInfo(serviceName)
		require.Equal(t, 2, len(info.Nodes))
	}

	{
		data, err := json.Marshal(&clustermgr.HeartbeatArgs{
			Name: serviceName,
			Host: hostPrefix + strconv.Itoa(2),
		})
		require.NoError(t, err)

		err = serviceMgr.Apply(ctx, []int32{OpHeartbeat}, [][]byte{data}, []base.ProposeContext{{ReqID: span.TraceID()}})
		require.NoError(t, err)
	}

	err = serviceMgr.Flush(ctx)
	require.NoError(t, err)
}

func TestGetServiceInfoExcludesExpired(t *testing.T) {
	tmpDBPath := "/tmp/tmpservicenormaldb" + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := normaldb.OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()

	svcMgr := NewServiceMgr(normaldb.OpenServiceTable(db))

	const svcName = "testSvc"
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	onlineHosts := []string{"host-online-1", "host-online-2", "host-online-3"}
	expiredHosts := []string{"host-expired-1", "host-expired-2"}

	for _, host := range append(onlineHosts, expiredHosts...) {
		data, err := json.Marshal(&clustermgr.RegisterArgs{
			ServiceNode: clustermgr.ServiceNode{Name: svcName, Host: host},
			Timeout:     60,
		})
		require.NoError(t, err)
		err = svcMgr.Apply(ctx, []int32{OpRegister}, [][]byte{data}, []base.ProposeContext{{ReqID: span.TraceID()}})
		require.NoError(t, err)
	}

	// simulate expiry: push Expires into the past
	v, ok := svcMgr.cache.Load(svcName)
	require.True(t, ok)
	sv := v.(*service)
	sv.Lock()
	for _, host := range expiredHosts {
		node := sv.nodes[host]
		node.Expires = time.Now().Add(-time.Second)
		sv.nodes[host] = node
	}
	sv.Unlock()

	// GetServiceInfo must exclude expired nodes
	info := svcMgr.GetServiceInfo(svcName)
	require.Equal(t, len(onlineHosts), len(info.Nodes))
	for _, n := range info.Nodes {
		require.Contains(t, n.Host, "online", "expired node must not appear in GetServiceInfo")
	}
}

func TestListServiceInfoMarksExpired(t *testing.T) {
	tmpDBPath := "/tmp/tmpservicenormaldb" + strconv.Itoa(rand.Intn(1000000000))
	defer os.RemoveAll(tmpDBPath)

	db, err := normaldb.OpenNormalDB(tmpDBPath)
	require.NoError(t, err)
	defer db.Close()

	svcMgr := NewServiceMgr(normaldb.OpenServiceTable(db))

	const svcName = "testSvc"
	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	onlineHosts := []string{"host-online-1", "host-online-2"}
	expiredHosts := []string{"host-expired-1", "host-expired-2", "host-expired-3"}

	for _, host := range append(onlineHosts, expiredHosts...) {
		data, err := json.Marshal(&clustermgr.RegisterArgs{
			ServiceNode: clustermgr.ServiceNode{Name: svcName, Host: host},
			Timeout:     60,
		})
		require.NoError(t, err)
		err = svcMgr.Apply(ctx, []int32{OpRegister}, [][]byte{data}, []base.ProposeContext{{ReqID: span.TraceID()}})
		require.NoError(t, err)
	}

	expiredAt := time.Now().Add(-time.Second)
	v, ok := svcMgr.cache.Load(svcName)
	require.True(t, ok)
	sv := v.(*service)
	sv.Lock()
	for _, host := range expiredHosts {
		node := sv.nodes[host]
		node.Expires = expiredAt
		sv.nodes[host] = node
	}
	sv.Unlock()

	now := time.Now().Unix()
	listInfo, err := svcMgr.ListServiceInfo()
	require.NoError(t, err)
	require.Equal(t, len(onlineHosts)+len(expiredHosts), len(listInfo.Nodes))

	onlineCount, expiredCount := 0, 0
	for _, n := range listInfo.Nodes {
		if n.ExpireAt == 0 {
			// online node: ExpireAt == 0
			onlineCount++
			require.Contains(t, n.Host, "online")
		} else {
			// expired node: ExpireAt > 0 and in the past
			expiredCount++
			require.Contains(t, n.Host, "expired")
			require.LessOrEqual(t, n.ExpireAt, now, "ExpireAt of an expired node must be <= now")
		}
	}
	require.Equal(t, len(onlineHosts), onlineCount)
	require.Equal(t, len(expiredHosts), expiredCount)
}
