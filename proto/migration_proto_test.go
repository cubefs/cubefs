package proto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateExtentKeyAfterMigrationRequestJSONContainsGeneration(t *testing.T) {
	req := &UpdateExtentKeyAfterMigrationRequest{
		Inode:        100,
		LeaseExpire:  200,
		Generation:   300,
		StorageClass: StorageClass_Replica_HDD,
		PoolId:       2,
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded UpdateExtentKeyAfterMigrationRequest
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, req.Generation, decoded.Generation)
	require.Equal(t, req.LeaseExpire, decoded.LeaseExpire)
}

func TestScanDentryJSONContainsGeneration(t *testing.T) {
	dentry := &ScanDentry{
		Inode:       101,
		LeaseExpire: 201,
		Generation:  301,
	}

	data, err := json.Marshal(dentry)
	require.NoError(t, err)

	var decoded ScanDentry
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, dentry.Generation, decoded.Generation)
	require.Equal(t, dentry.LeaseExpire, decoded.LeaseExpire)
}
