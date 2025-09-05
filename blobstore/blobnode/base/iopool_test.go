package base

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/blobnode/sys"
)

func TestIoPoolSimple(t *testing.T) {
	// normal
	metricConf := IoPoolMetricConf{
		ClusterID: 1,
		Host:      "host",
		DiskID:    101,
	}

	writePool := NewIOPool(2, 16, "write", metricConf)
	readPool := NewIOPool(1, 16, "read", metricConf)
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

	// alloc
	{
		chunkId := uint64(1)
		taskFn := func() error { err = sys.PreAllocate(file.Fd(), 0, 4096); return err }
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
		}
		_err := writePool.Submit(task)
		require.NoError(t, _err)

		require.NoError(t, err)
		fi1, _ := file.Stat()
		require.Equal(t, int64(4096), fi1.Size())
	}

	// write
	{
		data := []byte(content)
		n := 0
		chunkId := uint64(2)
		taskFn := func() error {
			n, err = file.WriteAt(data, 0)
			return err
		}
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
		}

		_err := writePool.Submit(task)
		require.NoError(t, _err)
		require.NoError(t, err)
		require.Equal(t, len(content), n)
	}

	// sync
	{
		chunkId := uint64(3)
		taskFn := func() error { err = file.Sync(); return err }
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
		}

		_err := writePool.Submit(task)
		require.NoError(t, _err)
		require.NoError(t, err)
	}

	// read
	{
		n := 0
		data := make([]byte, len(content))
		chunkId := uint64(4)
		taskFn := func() error {
			n, err = file.ReadAt(data, 0)
			return err
		}
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
		}

		require.Equal(t, uint8(0), data[0])
		_err := readPool.Submit(task)
		require.NoError(t, _err)
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
		taskFn := func() error { err = sys.PunchHole(file.Fd(), 0, 4096); return err }
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
		}
		_err := writePool.Submit(task)

		require.NoError(t, _err)
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
		taskFn := func() error {
			select {
			case <-ctx.Done():
				n, err = 0, ctx.Err()
				return err
			default:
			}
			n, err = file.WriteAt(data, 0)
			return err
		}
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
			Ctx:      ctx,
		}

		// cancel before submit
		cancel()

		_err := writePool.Submit(task)
		require.ErrorIs(t, _err, context.Canceled)
		require.NoError(t, err)
		require.Equal(t, 0, n) // not write
	}

	// ctx cancel, before doWork - test context cancellation timing
	{
		data := []byte(content)
		n := int32(0)
		chunkId := uint64(2)
		ctx, cancel := context.WithCancel(context.Background())

		taskFn := func() error {
			select {
			case <-ctx.Done():
				atomic.StoreInt32(&n, 0)
				err = ctx.Err()
				return err
			default:
			}
			atomic.AddInt32(&n, 1) // indicate task executed
			_, err = file.WriteAt(data, 0)
			return err
		}
		task := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn:   taskFn,
			Ctx:      ctx,
		}

		ch := make(chan struct{})
		taskLongTime := IoPoolTaskArgs{
			BucketId: chunkId,
			Tm:       time.Now(),
			TaskFn: func() error {
				cancel() // cancel context after this task starts
				ch <- struct{}{}
				atomic.AddInt32(&n, 1)
				return nil
			},
			Ctx: nil,
		}

		allDone := make(chan struct{}, 1)
		go func() {
			_err := writePool.Submit(taskLongTime)
			require.NoError(t, _err)
		}()
		go func() {
			<-ch
			// context canceled, so this task will not be executed
			_err := writePool.Submit(task)
			require.ErrorIs(t, _err, context.Canceled)
			allDone <- struct{}{}
		}()

		<-allDone
		require.NoError(t, err)
		require.Equal(t, int32(1), n) // long time task, not raw data
	}

	// empty, not limit write
	emptyPool := newCommonIoPool(0, 0, IoPoolMetricConf{})
	require.True(t, emptyPool.notLimit())
	defer emptyPool.Close()

	n := 0
	data := []byte(content)
	task := IoPoolTaskArgs{
		TaskFn: func() error { n, err = file.WriteAt(data, 0); return err },
	}

	_err := emptyPool.Submit(task)
	require.NoError(t, _err)
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	// empty, not limit read
	n = 0
	data = make([]byte, len(content))
	task = IoPoolTaskArgs{
		TaskFn: func() error { n, err = file.ReadAt(data, 0); return err },
	}
	require.Equal(t, uint8(0), data[0])
	require.Equal(t, 0, n)

	_err = emptyPool.Submit(task)
	require.NoError(t, _err)
	require.NoError(t, err)
	require.Equal(t, len(content), n)
	require.Equal(t, content, string(data))
}

func TestSubmitAfterClose(t *testing.T) {
	metricConf := IoPoolMetricConf{ClusterID: 1, Host: "host", DiskID: 101}

	// close, and then submit
	{
		n := 0
		ch := make(chan struct{}, 1)
		closePool := NewIOPool(1, 2, "read", metricConf)
		task := IoPoolTaskArgs{
			BucketId: 1,
			Tm:       time.Now(),
			TaskFn: func() error {
				n++
				ch <- struct{}{}
				return nil
			},
		}

		closePool.Close()
		err := closePool.Submit(task)
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}

	// close test - ensure all submitted tasks complete
	{
		n := int32(0)
		ch := make(chan struct{}, 2)
		closePool := NewIOPool(1, 2, "read", metricConf)
		task := IoPoolTaskArgs{
			BucketId: 1,
			Tm:       time.Now(),
			TaskFn: func() error {
				atomic.AddInt32(&n, 1)
				ch <- struct{}{}
				return nil
			},
		}
		task2, task3 := task, task
		task2.BucketId = 2
		task3.BucketId = 3
		go closePool.Submit(task)
		go closePool.Submit(task2)

		<-ch
		<-ch
		closePool.Close()

		var err error
		go func() {
			err = closePool.Submit(task3)
		}()

		select {
		case <-ch:
			require.NoError(t, err)
			require.Equal(t, int32(3), n) // all task should be executed
		case <-time.After(time.Second):
			t.Fatal("unexpect, task time out")
		}
	}

	// mock many tasks(long time work) are submitted, and then close, and then submit, all task expect done
	pool := NewIOPool(2, 4, "test", IoPoolMetricConf{ClusterID: 1, Host: "host", DiskID: 101})
	taskCompleted := make(chan int, 3)

	submitTask := func(id int, done chan<- struct{}) {
		defer func() { done <- struct{}{} }()
		err := pool.Submit(IoPoolTaskArgs{
			BucketId: uint64(id),
			Tm:       time.Now(),
			TaskFn: func() error {
				// mock do some work
				for i := 0; i < 100; i++ {
					_ = i
				}
				taskCompleted <- id
				return nil
			},
		})
		require.NoError(t, err)
	}

	backgroundDone := make(chan struct{}, 2)
	go submitTask(1, backgroundDone)
	go submitTask(2, backgroundDone)

	<-backgroundDone
	pool.Close()
	<-backgroundDone

	err := pool.Submit(IoPoolTaskArgs{
		BucketId: 3,
		Tm:       time.Now(),
		TaskFn: func() error {
			for i := 0; i < 100; i++ {
				_ = i
			}
			taskCompleted <- 3
			return nil
		},
	})
	require.NoError(t, err)

	// wait all task done
	completedTasks := make(map[int]bool)
	for i := 0; i < 3; i++ {
		select {
		case id := <-taskCompleted:
			completedTasks[id] = true
		case <-time.After(time.Second * 2):
			t.Fatal("unexpect, task time out")
		}
	}

	for _, id := range []int{1, 2, 3} {
		require.True(t, completedTasks[id])
	}
}
