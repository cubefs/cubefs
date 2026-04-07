package datanode

import (
	"testing"

	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/stretchr/testify/require"
)

func TestCompareExtentsBySizeMatchesFileIDsAndSizes(t *testing.T) {
	base := []*storage.ExtentInfo{newExtentInfo(1, 128), newExtentInfo(2, 256)}
	toCompare := []*storage.ExtentInfo{newExtentInfo(2, 256), newExtentInfo(1, 128)}

	require.True(t, compareExtentsBySize(100, toCompare, base))
}

func TestCompareExtentsBySizeMissingFileID(t *testing.T) {
	base := []*storage.ExtentInfo{newExtentInfo(1, 128)}
	toCompare := []*storage.ExtentInfo{newExtentInfo(2, 128)}

	require.False(t, compareExtentsBySize(101, toCompare, base))
}

func TestCompareExtentsBySizeSizeMismatch(t *testing.T) {
	base := []*storage.ExtentInfo{newExtentInfo(1, 128)}
	toCompare := []*storage.ExtentInfo{newExtentInfo(1, 256)}

	require.False(t, compareExtentsBySize(102, toCompare, base))
}

func newExtentInfo(fileID, size uint64) *storage.ExtentInfo {
	info := &storage.ExtentInfo{FileID: fileID}
	info.SetSize(size)
	return info
}
