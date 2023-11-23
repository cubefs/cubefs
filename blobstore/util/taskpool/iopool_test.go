package taskpool

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/blobnode/sys"
)

//go:generate mockgen -destination=./iopool_mock.go -package=taskpool -mock_names IoPool=MockIoPool github.com/cubefs/cubefs/blobstore/util/taskpool IoPool

func TestIoPoolSimple(t *testing.T) {
	writePool := NewWritePool(4, 16)
	readPool := NewReadPool(1, 16)
	defer writePool.Close()
	defer readPool.Close()

	content := "test content"
	name := fmt.Sprintf("%s/tmp_file.txt", t.TempDir()) // /tmp/TestIoPoolSimple2305704985/001/tmp_file.txt
	t.Logf("Test file in %v", name)
	file, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o777)
	require.NoError(t, err)
	fi, _ := file.Stat()
	require.Equal(t, int64(0), fi.Size())
	defer os.Remove(name)
	defer file.Close()

	// close
	{
		n := 0
		closePool := NewReadPool(1, 2)
		go closePool.Submit(1, func() {
			time.Sleep(time.Millisecond * 10)
			n++
		})
		go closePool.Submit(2, func() {
			time.Sleep(time.Millisecond * 10)
			n++
		})

		time.Sleep(time.Second)
		closePool.Close()
		closePool.Submit(3, func() {
			time.Sleep(time.Millisecond * 10)
			n++
		})
		require.Equal(t, 2, n) // two task func
	}

	// alloc
	{
		chunkId := uint64(1)
		taskFn := func() {
			err = sys.PreAllocate(file.Fd(), 0, 4096)
		}
		writePool.Submit(chunkId, taskFn)

		require.NoError(t, err)
		fi1, _ := file.Stat()
		require.Equal(t, int64(4096), fi1.Size())
	}

	// write
	{
		data := []byte(content)
		n := 0
		chunkId := uint64(2)
		taskFn := func() {
			n, err = file.WriteAt(data, 0)
		}

		writePool.Submit(chunkId, taskFn)
		require.NoError(t, err)
		require.Equal(t, len(content), n)
	}

	// sync
	{
		chunkId := uint64(3)
		taskFn := func() {
			err = file.Sync()
		}

		writePool.Submit(chunkId, taskFn)
		require.NoError(t, err)
	}

	// read
	{
		n := 0
		data := make([]byte, len(content))
		chunkId := uint64(4)
		taskFn := func() {
			n, err = file.ReadAt(data, 0)
		}

		require.Equal(t, uint8(0), data[0])
		readPool.Submit(chunkId, taskFn)
		require.NoError(t, err)
		require.Equal(t, len(content), n)
		require.Equal(t, content, string(data))
	}

	// punch hole
	{
		data := make([]byte, len(content))
		_, err = file.Read(data)
		require.NoError(t, err)
		require.Equal(t, content, string(data))

		chunkId := uint64(5)
		taskFn := func() {
			err = sys.PunchHole(file.Fd(), 0, 4096)
		}
		writePool.Submit(chunkId, taskFn)

		require.NoError(t, err)
		_, err = file.Read(data)
		require.NoError(t, err)
		require.NotEqual(t, content, string(data))
		for _, val := range data {
			require.Equal(t, uint8(0), val)
		}
	}
}
