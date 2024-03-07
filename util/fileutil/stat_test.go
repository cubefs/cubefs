package fileutil_test

import (
	"os"
	"testing"

	"github.com/cubefs/cubefs/util/fileutil"
	"github.com/stretchr/testify/require"
)

func TestStat(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.Remove(dir)
	ino, err := fileutil.Stat(dir)
	require.NoError(t, err)
	require.NotEqual(t, 0, ino)
}
