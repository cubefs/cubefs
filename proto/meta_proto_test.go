package proto

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScanInodeByPoolResponseJSONRoundTrip(t *testing.T) {
	resp := &ScanInodeByPoolResponse{
		Inodes:       []uint64{101, 202, 303},
		NextInode:    304,
		HasMore:      true,
		TotalScanned: 500,
	}

	data, err := json.Marshal(resp)
	require.NoError(t, err)

	var decoded ScanInodeByPoolResponse
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, resp.Inodes, decoded.Inodes)
	require.Equal(t, resp.NextInode, decoded.NextInode)
	require.Equal(t, resp.HasMore, decoded.HasMore)
	require.Equal(t, resp.TotalScanned, decoded.TotalScanned)
}
