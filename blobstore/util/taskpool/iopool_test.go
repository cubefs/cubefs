package taskpool

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/blobnode/sys"
)

func TestIoPoolSimple(t *testing.T) {
	metricConf := IoPoolMetricConf{
		ClusterID: 1,
		IDC:       "idc",
		Rack:      "rack",
		Host:      "host",
		DiskID:    101,
		Namespace: "bs",
		Subsystem: "bn",
	}

	writePool := NewWritePool(4, 16, metricConf)
	readPool := NewReadPool(1, 16, metricConf)
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
		ch := make(chan struct{}, 2)
		closePool := NewReadPool(1, 2, metricConf)
		task := IoPoolTaskArgs{
			BucketId: 1,
			Tm:       time.Now(),
			TaskFn: func() {
				ch <- struct{}{}
				n++
			},
		}
		task2, task3 := task, task
		task2.BucketId = 2
		task3.BucketId = 3
		go closePool.Submit(task)
		go closePool.Submit(task2)

		<-ch
		closePool.Close()
		closePool.Submit(task3)
		require.Equal(t, 3, n) // two task func
	}

	// alloc
	{
		chunkId := uint64(1)
		taskFn := func() { err = sys.PreAllocate(file.Fd(), 0, 4096) }
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
		}
		writePool.Submit(task)

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
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
		}

		writePool.Submit(task)
		require.NoError(t, err)
		require.Equal(t, len(content), n)
	}

	// sync
	{
		chunkId := uint64(3)
		taskFn := func() {
			err = file.Sync()
		}
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
		}

		writePool.Submit(task)
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
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
		}

		require.Equal(t, uint8(0), data[0])
		readPool.Submit(task)
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
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
		}
		writePool.Submit(task)

		require.NoError(t, err)
		_, err = file.Read(data)
		require.NoError(t, err)
		require.NotEqual(t, content, string(data))
		for _, val := range data {
			require.Equal(t, uint8(0), val)
		}
	}

	// ctx cancel, before submit
	{
		data := []byte(content)
		n := 0
		chunkId := uint64(2)
		ctx, cancel := context.WithCancel(context.Background())
		taskFn := func() {
			select {
			case <-ctx.Done():
				n, err = 0, ctx.Err()
				return
			default:
			}
			n, err = file.WriteAt(data, 0)
		}
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
			Ctx:      ctx,
		}

		// cancel before submit
		cancel()

		writePool.Submit(task)
		require.NoError(t, err)
		require.Equal(t, 0, n) // not write
	}

	// ctx cancel, before doWork
	{
		data := []byte(content)
		n := -1
		chunkId := uint64(2)
		ctx, cancel := context.WithCancel(context.Background())

		taskFn := func() {
			select {
			case <-ctx.Done():
				n, err = 0, ctx.Err()
				return
			default:
			}
			n, err = file.WriteAt(data, 0)
		}
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
			Ctx:      ctx,
		}

		taskLongTime := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn: func() {
				cancel()
				n = 1
			},
			Ctx: nil,
		}

		ch := make(chan struct{}, 1)
		allDone := make(chan struct{}, 1)
		go func() {
			ch <- struct{}{}
			writePool.Submit(taskLongTime)
		}()
		go func() {
			<-ch
			writePool.Submit(task)
			allDone <- struct{}{}
		}()

		<-allDone
		require.NoError(t, err)
		require.Equal(t, 1, n) // long time task, not raw data
	}
}
