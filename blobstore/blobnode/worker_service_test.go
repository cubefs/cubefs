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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/cubefs/cubefs/blobstore/blobnode/client"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/limit/count"
)

func getDefaultConfig() WorkerConfig {
	cfg := WorkerConfig{}
	cfg.checkAndFix()
	return cfg
}

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

func (m *mBlobNodeCli) PutShard(ctx context.Context, location proto.VunitLocation, bid proto.BlobID, size int64, body io.Reader, ioType bnapi.IOType) (err error) {
	return
}

type mockScheCli struct {
	*mocks.MockIScheduler

	migrateID       int
	repairTaskCnt   int
	diskDropTaskCnt int
	balanceTaskCnt  int

	inspectID  int
	inspectCnt int
}

func (m *mockScheCli) AcquireTask(ctx context.Context, args *scheduler.AcquireArgs) (ret *proto.MigrateTask, err error) {
	m.migrateID++
	mode := codemode.EC6P10L2
	srcReplicas := genMockVol(1, mode)
	destVuid, _ := proto.NewVuid(1, uint8(0), 2)
	dst := proto.VunitLocation{
		Vuid:   destVuid,
		Host:   "http://127.0.0.1",
		DiskID: 1,
	}

	task := proto.MigrateTask{
		CodeMode:    mode,
		Destination: dst,
		Sources:     srcReplicas,
	}
	switch m.migrateID % 4 {
	case 0:
		task.TaskType = proto.TaskTypeDiskRepair
		task.TaskID = fmt.Sprintf("repair_%d", m.migrateID)
		m.repairTaskCnt++
	case 1:
		task.TaskType = proto.TaskTypeBalance
		task.TaskID = fmt.Sprintf("balance_%d", m.migrateID)
		m.balanceTaskCnt++
	case 2:
		task.TaskType = proto.TaskTypeDiskDrop
		task.TaskID = fmt.Sprintf("disk_drop_%d", m.migrateID)
		m.diskDropTaskCnt++
	}
	return &task, nil
}

func (m *mockScheCli) AcquireInspectTask(ctx context.Context) (ret *proto.VolumeInspectTask, err error) {
	m.inspectID++
	m.inspectCnt++

	mode := codemode.EC6P10L2
	ret = &proto.VolumeInspectTask{TaskID: fmt.Sprintf("inspect_%d", m.inspectID), Mode: mode}
	if m.inspectID%2 == 0 {
		ret.Replicas = genMockVol(1, mode)
	}
	return
}

func newMockWorkService(t *testing.T) (*Service, *mockScheCli) {
	cli := mocks.NewMockIScheduler(C(t))
	schedulerCli := &mockScheCli{MockIScheduler: cli}
	schedulerCli.EXPECT().CompleteInspectTask(A, A).AnyTimes().Return(nil)
	blobnodeCli := &mBlobNodeCli{}

	workSvr := &WorkerService{
		Closer: closer.New(),
		WorkerConfig: WorkerConfig{
			WorkerConfigMeter: WorkerConfigMeter{
				MaxTaskRunnerCnt:   100,
				InspectConcurrency: 1,
			},
			AcquireIntervalMs: 1,
		},

		shardRepairLimit: count.New(1),
		schedulerCli:     schedulerCli,
		blobNodeCli:      blobnodeCli,

		taskRunnerMgr:  NewTaskRunnerMgr("z0", getDefaultConfig().WorkerConfigMeter, NewMockMigrateWorker, schedulerCli, schedulerCli),
		inspectTaskMgr: NewInspectTaskMgr(1, blobnodeCli, schedulerCli),
	}
	return &Service{WorkerService: workSvr}, schedulerCli
}

func TestServiceAPI(t *testing.T) {
	service, _ := newMockWorkService(t)
	defer service.WorkerService.Close()
	workerServer := httptest.NewServer(NewHandler(service))
	workerCli := bnapi.New(&bnapi.Config{})
	testCases := []struct {
		args *proto.ShardRepairTask
		code int
	}{
		{args: &proto.ShardRepairTask{}, code: 704},
	}
	for _, tc := range testCases {
		err := workerCli.RepairShard(context.Background(), workerServer.URL, tc.args)
		require.Equal(t, tc.code, rpc.DetectStatusCode(err))
	}

	_, err := workerCli.WorkerStats(context.Background(), workerServer.URL)
	require.NoError(t, err)
}

func TestWorkerService(t *testing.T) {
	svr, schedulerCli := newMockWorkService(t)
	closed := make(chan struct{})
	go func() {
		svr.WorkerService.Run()
		close(closed)
	}()
	time.Sleep(100 * time.Millisecond)
	svr.WorkerService.Close()
	<-closed

	typeMgr := svr.WorkerService.taskRunnerMgr.typeMgr
	require.Equal(t, schedulerCli.repairTaskCnt, len(typeMgr[proto.TaskTypeDiskRepair]))
	require.Equal(t, schedulerCli.balanceTaskCnt, len(typeMgr[proto.TaskTypeBalance]))
	require.Equal(t, schedulerCli.diskDropTaskCnt, len(typeMgr[proto.TaskTypeDiskDrop]))
	require.Less(t, 2, schedulerCli.inspectCnt)
}

func TestNewWorkService(t *testing.T) {
	clusterMgr := cmapi.New(&cmapi.Config{})
	svr, err := NewWorkerService(&WorkerConfig{}, clusterMgr, 1, "z0")
	require.NoError(t, err)
	svr.Close()
}

func TestFixConfigItem(t *testing.T) {
	var item int
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
