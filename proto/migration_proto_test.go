package proto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateExtentKeyAfterMigrationRequestJSONRoundTrip(t *testing.T) {
	req := &UpdateExtentKeyAfterMigrationRequest{
		Inode:        100,
		LeaseExpire:  200,
		StorageClass: StorageClass_Replica_HDD,
		PoolId:       2,
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var decoded UpdateExtentKeyAfterMigrationRequest
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, req.LeaseExpire, decoded.LeaseExpire)
	require.Equal(t, req.Inode, decoded.Inode)
}

func TestScanDentryJSONRoundTrip(t *testing.T) {
	dentry := &ScanDentry{
		Inode:       101,
		LeaseExpire: 201,
	}

	data, err := json.Marshal(dentry)
	require.NoError(t, err)
	require.NotContains(t, string(data), `"generation"`)

	var decoded ScanDentry
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, dentry.Inode, decoded.Inode)
	require.Equal(t, dentry.LeaseExpire, decoded.LeaseExpire)
}
