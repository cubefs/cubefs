package clustermgr

import (
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/raftdb"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func TestManage(t *testing.T) {
	testService := initTestService(t)
	defer clear(testService)
	defer testService.Close()
	testClusterClient := initTestClusterClient(testService)

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// test member add or remove
	{
		err := testClusterClient.AddMember(ctx, &clustermgr.AddMemberArgs{PeerID: 2, Host: "127.0.0.1", NodeHost: "127.0.0.2", MemberType: clustermgr.MemberTypeMin})
		assert.NotNil(t, err)

		err = testClusterClient.AddMember(ctx, &clustermgr.AddMemberArgs{PeerID: 2, Host: "127.0.0.1", NodeHost: "127.0.0.2", MemberType: clustermgr.MemberTypeNormal})
		assert.NoError(t, err)

		err = testClusterClient.RemoveMember(ctx, 10)
		assert.Equal(t, apierrors.ErrIllegalArguments.Error(), err.Error())

		err = testClusterClient.TransferLeadership(ctx, 2)
		assert.NoError(t, err)

		err = testClusterClient.RemoveMember(ctx, 1)
		assert.Equal(t, apierrors.ErrRequestNotAllow.Error(), err.Error())

		err = testClusterClient.AddMember(ctx, &clustermgr.AddMemberArgs{PeerID: 2, Host: "127.0.0.1", NodeHost: "127.0.0.2", MemberType: clustermgr.MemberTypeNormal})
		assert.Equal(t, apierrors.CodeDuplicatedMemberInfo, err.(rpc.HTTPError).StatusCode())

	}

	// test stat
	{
		statInfo, err := testClusterClient.Stat(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, statInfo)
	}

	// test snapshot dump
	{
		snapshotDBs := make(map[string]base.SnapshotDB)
		uuid, err := uuid.NewUUID()
		assert.NoError(t, err)
		tmpNormalDBPath := os.TempDir() + "/snapshot-normaldb-" + uuid.String()
		tmpVolumeDBPath := os.TempDir() + "/snapshot-volumedb-" + uuid.String()
		tmpRaftDBPath := os.TempDir() + "/snapshot-raftdb-" + uuid.String()
		os.RemoveAll(tmpNormalDBPath)
		os.RemoveAll(tmpVolumeDBPath)
		os.RemoveAll(tmpRaftDBPath)
		defer os.RemoveAll(tmpVolumeDBPath)
		defer os.RemoveAll(tmpNormalDBPath)
		defer os.RemoveAll(tmpRaftDBPath)

		normalDB, err := normaldb.OpenNormalDB(tmpNormalDBPath, false)
		assert.NoError(t, err)
		defer normalDB.Close()
		volumeDB, err := volumedb.Open(tmpVolumeDBPath, false)
		assert.NoError(t, err)
		defer volumeDB.Close()
		raftDB, err := raftdb.OpenRaftDB(tmpRaftDBPath, false)
		assert.NoError(t, err)
		defer raftDB.Close()

		snapshotDBs["volume"] = volumeDB
		snapshotDBs["normal"] = normalDB

		resp, err := testClusterClient.Snapshot(ctx)
		assert.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		index, err := strconv.ParseUint(resp.Header.Get(clustermgr.RaftSnapshotIndexHeaderKey), 10, 64)
		assert.NoError(t, err)

		for {
			snapshotData, err := base.DecodeSnapshotData(resp.Body)
			if err != nil {
				assert.Equal(t, io.EOF, err)
				break
			}
			dbName := snapshotData.Header.DbName
			cfName := snapshotData.Header.CfName

			if snapshotData.Header.CfName != "" {
				err = snapshotDBs[dbName].Table(cfName).Put(kvstore.KV{Key: snapshotData.Key, Value: snapshotData.Value})
			} else {
				err = snapshotDBs[dbName].Put(kvstore.KV{Key: snapshotData.Key, Value: snapshotData.Value})
			}
			assert.NoError(t, err)
		}
		indexValue := make([]byte, 8)
		binary.BigEndian.PutUint64(indexValue, index)
		err = raftDB.Put(base.ApplyIndexKey, indexValue)
		assert.NoError(t, err)
	}
}
