package datanode

import (
	"context"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/datanode/mock"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/testutil"
)

func TestSpaceManager_CreatePartition(t *testing.T) {
	var testPath = testutil.InitTempTestPath(t)
	defer testPath.Cleanup()

	var err error

	var diskPath = path.Join(testPath.Path(), "disk")
	if err = os.MkdirAll(diskPath, os.ModePerm); err != nil {
		t.Fatalf("Make disk path %v failed: %v", diskPath, err)
	}

	var space = NewSpaceManager(&DataNode{})
	space.SetClusterID("test")
	space.SetRaftStore(mock.NewMockRaftStore())
	if err = space.LoadDisk(diskPath, 0, 1); err != nil {
		t.Fatalf("Load disk %v failed: %v", diskPath, err)
	}

	const (
		numOfInitPartitions     = 5
		numOfAccessWorker       = 10
		numOfIncreasePartitions = 5
	)

	var nextPartitionID uint64 = 1
	var initPartitionIDs = make([]uint64, 0, numOfInitPartitions)
	for i := 1; i <= numOfInitPartitions; i++ {
		if _, err = space.CreatePartition(&proto.CreateDataPartitionRequest{
			PartitionId:   nextPartitionID,
			PartitionSize: 1024 * 1024,
			VolumeId:      "test_volume",
		}); err != nil {
			t.Fatalf("Create partition failed: %v", err)
		}
		initPartitionIDs = append(initPartitionIDs, uint64(i))
		nextPartitionID++
	}

	var (
		ctx, cancel          = context.WithCancel(context.Background())
		accessWorkerLaunchWG = new(sync.WaitGroup)
		accessWorkerFinishWG = new(sync.WaitGroup)
		accessCount          int64
	)

	accessWorkerLaunchWG.Add(1)
	for i := 0; i < numOfAccessWorker; i++ {
		accessWorkerFinishWG.Add(1)
		go func() {
			defer accessWorkerFinishWG.Done()
			accessWorkerLaunchWG.Wait()
			var r = rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				var pid = initPartitionIDs[r.Intn(len(initPartitionIDs))]
				space.Partition(pid)
				atomic.AddInt64(&accessCount, 1)
			}
		}()
	}

	var startTime = time.Now()
	accessWorkerLaunchWG.Done()
	for i := 0; i < numOfIncreasePartitions; i++ {
		if _, err = space.CreatePartition(&proto.CreateDataPartitionRequest{
			PartitionId:   nextPartitionID,
			PartitionSize: 1024 * 1024,
			VolumeId:      "test_volume",
		}); err != nil {
			t.Fatalf("Create partition failed: %v", err)
		}
		nextPartitionID++
	}

	cancel()
	accessWorkerFinishWG.Wait()
	var creationElapsed = time.Now().Sub(startTime)

	t.Logf("Statistics: creation elapsed %v, access count: %v", creationElapsed, atomic.LoadInt64(&accessCount))
}
