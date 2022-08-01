// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blobnode

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	api "github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/limit/count"
)

type mBlobNodeCli struct{}

func (m *mBlobNodeCli) StatChunk(ctx context.Context, location proto.VunitLocation) (ci *client.ChunkInfo, err error) {
	return
}

func (m *mBlobNodeCli) StatShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID) (*client.ShardInfo, error) {
	return nil, nil
}

func (m *mBlobNodeCli) ListShards(ctx context.Context, location proto.VunitLocation) ([]*client.ShardInfo, error) {
	return nil, nil
}

func (m *mBlobNodeCli) GetShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, ioType bnapi.IOType) (io.ReadCloser, uint32, error) {
	return nil, 0, nil
}

func (m *mBlobNodeCli) GetShards(ctx context.Context, location proto.VunitLocation, Bids []proto.BlobID) (content map[proto.BlobID]io.Reader, err error) {
	return nil, nil
}

func (m *mBlobNodeCli) PutShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, size int64, body io.Reader, ioType bnapi.IOType) (err error) {
	return
}

type mScheCli struct {
	id                int
	inspectID         int
	repairTaskCnt     int
	diskDropTaskCnt   int
	balanceTaskCnt    int
	acquireInspectCnt int
}

func (m *mScheCli) AcquireTask(ctx context.Context, args *api.AcquireArgs) (ret *api.WorkerTask, err error) {
	m.id++
	mode := codemode.EC6P10L2
	srcReplicas, _ := genMockVol(1, mode)
	destVuid, _ := proto.NewVuid(1, uint8(0), 2)
	dst := proto.VunitLocation{
		Vuid:   destVuid,
		Host:   "http://127.0.0.1",
		DiskID: 1,
	}

	task := api.WorkerTask{}
	switch m.id % 3 {
	case 0:
		task.Task = proto.MigrateTask{
			TaskID:      fmt.Sprintf("repair_%d", m.id),
			TaskType:    proto.TaskTypeDiskRepair,
			CodeMode:    mode,
			Destination: dst,
			Sources:     srcReplicas,
		}
		m.repairTaskCnt++
	case 1:
		task.Task = proto.MigrateTask{
			TaskID:      fmt.Sprintf("balance_%d", m.id),
			TaskType:    proto.TaskTypeBalance,
			CodeMode:    mode,
			Destination: dst,
			Sources:     srcReplicas,
		}
		m.balanceTaskCnt++
	case 2:
		task.Task = proto.MigrateTask{
			TaskID:      fmt.Sprintf("disk_drop_%d", m.id),
			TaskType:    proto.TaskTypeDiskDrop,
			CodeMode:    mode,
			Destination: dst,
			Sources:     srcReplicas,
		}
		m.diskDropTaskCnt++
	}
	return &task, nil
}

func (m *mScheCli) AcquireInspectTask(ctx context.Context) (ret *api.WorkerInspectTask, err error) {
	m.inspectID++
	m.acquireInspectCnt++
	ret = &api.WorkerInspectTask{}
	task := &proto.VolumeInspectTask{}
	task.Mode = codemode.EC6P10L2
	ret.Task = task
	ret.Task.TaskId = fmt.Sprintf("inspect_%d", m.inspectID)
	return
}

func (m *mScheCli) ReclaimTask(ctx context.Context, args *api.ReclaimTaskArgs) error {
	return nil
}

func (m *mScheCli) CancelTask(ctx context.Context, args *api.CancelTaskArgs) error {
	return nil
}

func (m *mScheCli) CompleteTask(ctx context.Context, args *api.CompleteTaskArgs) error {
	return nil
}

func (m *mScheCli) CompleteInspect(ctx context.Context, args *api.CompleteInspectArgs) error {
	return nil
}

// report doing tasks
func (m *mScheCli) RenewalTask(ctx context.Context, args *api.TaskRenewalArgs) (ret *api.TaskRenewalRet, err error) {
	ret = &api.TaskRenewalRet{}
	return
}

func (m *mScheCli) ReportTask(ctx context.Context, args *api.TaskReportArgs) (err error) {
	return nil
}

var (
	workerServer *httptest.Server
	once         sync.Once
	schedulerCli = &mScheCli{}
	blobnodeCli  = &mBlobNodeCli{}
)

func getDefaultConfig() WorkerConfig {
	cfg := WorkerConfig{}
	cfg.checkAndFix()
	return cfg
}

func newMockWorkService() *Service {
	scheduler := schedulerCli
	blobnode := blobnodeCli
	workSvr := &WorkerService{
		Closer: closer.New(),

		shardRepairLimit: count.New(1),
		schedulerCli:     scheduler,
		blobNodeCli:      blobnode,
		WorkerConfig:     WorkerConfig{AcquireIntervalMs: 1},

		taskRunnerMgr:  NewTaskRunnerMgr("z0", getDefaultConfig().WorkerConfigMeter, scheduler, NewMockMigrateWorker),
		inspectTaskMgr: NewInspectTaskMgr(1, blobnode, scheduler),
	}
	return &Service{WorkerService: workSvr}
}

func runMockService(s *Service) string {
	once.Do(func() {
		workerServer = httptest.NewServer(NewHandler(s))
	})
	return workerServer.URL
}

func TestServiceAPI(t *testing.T) {
	runMockService(newMockWorkService())
	workerCli := bnapi.New(&bnapi.Config{})
	testCases := []struct {
		args *bnapi.ShardRepairArgs
		code int
	}{
		{
			args: &bnapi.ShardRepairArgs{
				Task: proto.ShardRepairTask{},
			},
			code: 704,
		},
	}
	for _, tc := range testCases {
		err := workerCli.RepairShard(context.Background(), workerServer.URL, tc.args)
		require.Equal(t, tc.code, rpc.DetectStatusCode(err))
	}

	_, err := workerCli.WorkerStats(context.Background(), workerServer.URL)
	require.NoError(t, err)
}

func TestSvr(t *testing.T) {
	svr := newMockWorkService()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		svr.WorkerService.Run()
		wg.Done()
	}()

	time.Sleep(100 * time.Millisecond)
	svr.WorkerService.Close()
	wg.Wait()

	require.Equal(t, schedulerCli.repairTaskCnt, len(svr.WorkerService.taskRunnerMgr.repair))
	require.Equal(t, schedulerCli.balanceTaskCnt, len(svr.WorkerService.taskRunnerMgr.balance))
	require.Equal(t, schedulerCli.diskDropTaskCnt, len(svr.WorkerService.taskRunnerMgr.diskDrop))
}

func TestNewWorkService(t *testing.T) {
	cfg := &WorkerConfig{}
	clusterMgr := cmapi.New(&cmapi.Config{})
	_, err := NewWorkerService(cfg, clusterMgr, proto.ClusterID(1), "z0")
	require.NoError(t, err)
}

func TestFixConfigItem(t *testing.T) {
	var item int
	item = 0
	fixConfigItemInt(&item, 100)
	require.Equal(t, item, 100)

	item = -1
	fixConfigItemInt(&item, 100)
	require.Equal(t, item, 100)

	item = 10
	fixConfigItemInt(&item, 100)
	require.Equal(t, item, 10)
}

func TestCfgFix(t *testing.T) {
	cfg := getDefaultConfig()
	fixConfigItemInt(&cfg.AcquireIntervalMs, 500)
	fixConfigItemInt(&cfg.MaxTaskRunnerCnt, 1)
	fixConfigItemInt(&cfg.RepairConcurrency, 1)
	fixConfigItemInt(&cfg.BalanceConcurrency, 1)
	fixConfigItemInt(&cfg.DiskDropConcurrency, 1)
	fixConfigItemInt(&cfg.ShardRepairConcurrency, 1)
	fixConfigItemInt(&cfg.InspectConcurrency, 1)
	fixConfigItemInt(&cfg.DownloadShardConcurrency, 10)

	require.Equal(t, 500, cfg.AcquireIntervalMs)
	require.Equal(t, 1, cfg.MaxTaskRunnerCnt)
	require.Equal(t, 1, cfg.RepairConcurrency)
	require.Equal(t, 1, cfg.BalanceConcurrency)
	require.Equal(t, 1, cfg.DiskDropConcurrency)
	require.Equal(t, 1, cfg.ShardRepairConcurrency)
	require.Equal(t, 1, cfg.InspectConcurrency)
	require.Equal(t, 10, cfg.DownloadShardConcurrency)

	cfg.AcquireIntervalMs = 600
	cfg.MaxTaskRunnerCnt = 100
	cfg.RepairConcurrency = 100
	cfg.BalanceConcurrency = 100
	cfg.DiskDropConcurrency = 100
	cfg.ShardRepairConcurrency = 100
	cfg.InspectConcurrency = 100
	cfg.DownloadShardConcurrency = 100

	require.Equal(t, 600, cfg.AcquireIntervalMs)
	require.Equal(t, 100, cfg.MaxTaskRunnerCnt)
	require.Equal(t, 100, cfg.RepairConcurrency)
	require.Equal(t, 100, cfg.BalanceConcurrency)
	require.Equal(t, 100, cfg.DiskDropConcurrency)
	require.Equal(t, 100, cfg.ShardRepairConcurrency)
	require.Equal(t, 100, cfg.InspectConcurrency)
	require.Equal(t, 100, cfg.DownloadShardConcurrency)
}
