// Copyright 2024 The CubeFS Authors.
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

package clustermgr

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/catalogdb"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// generate 10 spaces in db
func initServiceWithSpaceData() (*Service, func()) {
	cfg := *testServiceCfg

	cfg.DBPath = os.TempDir() + "/space" + uuid.NewString() + strconv.FormatInt(rand.Int63n(math.MaxInt64), 10)
	cfg.ShardNodeDiskMgrConfig.HeartbeatExpireIntervalS = 600
	cfg.ClusterReportIntervalS = 3
	cfg.ShardCodeModeName = codemode.Replica3.Name()
	cfg.RaftConfig.ServerConfig.ListenPort = GetFreePort()
	cfg.RaftConfig.ServerConfig.Members = []raftserver.Member{
		{NodeID: 1, Host: fmt.Sprintf("127.0.0.1:%d", GetFreePort()), Learner: false},
	}

	os.Mkdir(cfg.DBPath, 0o755)
	err := generateSpace(cfg.DBPath + "/catalogdb")
	if err != nil {
		panic("generate space error: " + err.Error())
	}

	testService, _ := New(&cfg)
	return testService, func() {
		go func() {
			cleanTestService(testService)
		}()
	}
}

func generateSpace(catalogDBPath string) error {
	catalogDB, err := catalogdb.Open(catalogDBPath)
	if err != nil {
		return err
	}
	defer catalogDB.Close()
	catalogTable, err := catalogdb.OpenCatalogTable(catalogDB)
	if err != nil {
		return err
	}

	fildMeta := clustermgr.FieldMeta{
		ID:          1,
		Name:        "fildName1",
		FieldType:   proto.FieldTypeBool,
		IndexOption: proto.IndexOptionIndexed,
	}
	for i := 1; i <= 10; i++ {
		space := &catalogdb.SpaceInfoRecord{
			SpaceID:    proto.SpaceID(i),
			Name:       fmt.Sprintf("spaceName%d", i),
			Status:     proto.SpaceStatusNormal,
			FieldMetas: []clustermgr.FieldMeta{fildMeta},
			AccessKey:  fmt.Sprintf("ak%d", i),
			SecretKey:  fmt.Sprintf("sk%d", i),
		}
		err = catalogTable.CreateSpace(space)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestService_Space(t *testing.T) {
	testService, clean := initServiceWithSpaceData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()
	{
		ret, err := cmClient.GetSpaceByName(ctx, &clustermgr.GetSpaceByNameArgs{Name: "spaceName1"})
		require.NoError(t, err)
		require.NotNil(t, ret)
	}

	// list space
	{
		listArgs := &clustermgr.ListSpaceArgs{
			Marker: proto.SpaceID(0),
			Count:  1,
		}
		list, err := cmClient.ListSpace(ctx, listArgs)
		require.NoError(t, err)
		require.Equal(t, 1, len(list.Spaces))
		require.Equal(t, proto.SpaceID(1), list.Spaces[0].SpaceID)

		listArgs.Marker = proto.SpaceID(1)
		listArgs.Count = 4
		list, err = cmClient.ListSpace(ctx, listArgs)
		require.NoError(t, err)
		require.Equal(t, 4, len(list.Spaces))
		require.Equal(t, proto.SpaceID(2), list.Spaces[0].SpaceID)
		require.Equal(t, proto.SpaceID(5), list.Spaces[3].SpaceID)
	}

	// auth space
	{
		auth := &clustermgr.AuthInfo{
			AccessKey: "ak",
			SecretKey: "sk",
		}
		token, err := clustermgr.EncodeAuthInfo(auth)
		require.NoError(t, err)
		authArgs := &clustermgr.AuthSpaceArgs{
			Name:  "spaceName1",
			Token: token,
		}
		err = cmClient.AuthSpace(ctx, authArgs)
		require.Error(t, err)

		auth = &clustermgr.AuthInfo{
			AccessKey: "ak1",
			SecretKey: "sk1",
		}
		token, err = clustermgr.EncodeAuthInfo(auth)
		require.NoError(t, err)
		authArgs = &clustermgr.AuthSpaceArgs{
			Name:  "spaceName1",
			Token: token,
		}
		err = cmClient.AuthSpace(ctx, authArgs)
		require.NoError(t, err)
	}

	// get space
	{
		getByNameArgs := &clustermgr.GetSpaceByNameArgs{
			Name: "spaceName1",
		}
		ret, err := cmClient.GetSpaceByName(ctx, getByNameArgs)
		require.NoError(t, err)
		require.Equal(t, getByNameArgs.Name, ret.Name)
		require.Equal(t, proto.SpaceID(1), ret.SpaceID)
		require.Equal(t, "ak1", ret.AccessKey)
		require.Equal(t, "sk1", ret.SecretKey)

		getByIDArgs := &clustermgr.GetSpaceByIDArgs{
			SpaceID: 1,
		}
		ret, err = cmClient.GetSpaceByID(ctx, getByIDArgs)
		require.NoError(t, err)
		require.Equal(t, getByIDArgs.SpaceID, ret.SpaceID)
		require.Equal(t, "spaceName1", ret.Name)
		require.Equal(t, "ak1", ret.AccessKey)
		require.Equal(t, "sk1", ret.SecretKey)

		getByIDArgs.SpaceID = proto.SpaceID(100)
		_, err = cmClient.GetSpaceByID(ctx, getByIDArgs)
		require.Error(t, err)
	}

	// create space
	{
		fildMeta := clustermgr.FieldMeta{
			Name:        "fildName1",
			FieldType:   proto.FieldTypeBool,
			IndexOption: proto.IndexOptionIndexed,
		}
		createArgs := &clustermgr.CreateSpaceArgs{
			Name:       "spaceName66",
			FieldMetas: []clustermgr.FieldMeta{fildMeta},
		}
		err := cmClient.CreateSpace(ctx, createArgs)
		require.NoError(t, err)

		getByNameArgs := &clustermgr.GetSpaceByNameArgs{
			Name: "spaceName66",
		}
		_, err = cmClient.GetSpaceByName(ctx, getByNameArgs)
		require.NoError(t, err)
	}
}
