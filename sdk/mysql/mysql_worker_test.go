package mysql

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"testing"
	"time"
)

func TestSelectWorkerNode(t *testing.T) {
	// check worker node exist

	var (
		localIP1                        = "10.18.109.101"
		localIP2                        = "10.18.109.102"
		err                             error
		res                             bool
		workerId1, workerId2, workerId3 uint64
	)
	workerNode1 := &proto.WorkerNode{
		WorkerType: proto.WorkerTypeSmartVolume,
		WorkerAddr: localIP1,
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
	}
	workerNode2 := &proto.WorkerNode{
		WorkerType: proto.WorkerTypeSmartVolume,
		WorkerAddr: localIP2,
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
	}
	workerNode3 := &proto.WorkerNode{
		WorkerType: proto.WorkerTypeCompact,
		WorkerAddr: localIP2,
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
	}
	res, err = CheckWorkerExist(workerNode1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res {
		fmt.Printf("worker node exist, workerAddr: %s, workerType: %v\n", workerNode1.WorkerAddr, workerNode1.WorkerType)
	} else {
		workerId1, err = AddWorker(workerNode1)
		workerNode1.WorkerId = workerId1
	}

	res, err = CheckWorkerExist(workerNode2)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res {
		fmt.Printf("worker node exist, workerAddr: %s, workerType: %v\n", workerNode2.WorkerAddr, workerNode2.WorkerType)
	} else {
		workerId2, err = AddWorker(workerNode2)
		workerNode2.WorkerId = workerId2
	}

	res, err = CheckWorkerExist(workerNode3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res {
		fmt.Printf("worker node exist, workerAddr: %s, workerType: %v\n", workerNode3.WorkerAddr, workerNode3.WorkerType)
	} else {
		workerId3, err = AddWorker(workerNode3)
		workerNode3.WorkerId = workerId3
	}

	// select
	workerNode, err := SelectWorkerNode(proto.WorkerTypeSmartVolume, localIP2)
	if workerNode == nil {
		fmt.Println("selected worker node is empty")
		return
	}
	fmt.Printf("workerid: %v, workerTyle: %v, workerAddr: %v\n", workerNode.WorkerId, workerNode.WorkerType, workerNode.WorkerAddr)
	if err != nil {
		t.Fatalf(fmt.Sprintf("select worker node failed cause : %s\n", err.Error()))
	}
	time.Sleep(time.Second * 1)
	if err := UpdateWorkerHeartbeat(workerNode); err != nil {
		t.Errorf(fmt.Sprintf("update worker heartbeat failed cause : %s\n", err.Error()))
	}

	// select worker nodes via worker type
	workers, err := SelectAllWorkers(int(proto.WorkerTypeSmartVolume))
	if err != nil {
		t.Fatalf(fmt.Sprintf("select workers failed cause : %s\n", err.Error()))
	}
	if len(workers) < 2 {
		t.Fatalf(fmt.Sprintf("selected workers size less then expected, workers size: %v\n", len(workers)))
	}

	// select condition is optional
	workers, err = SelectWorkers(0, "", 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("select workers failed cause : %s\n", err.Error()))
	}
	if len(workers) < 3 {
		t.Fatalf(fmt.Sprintf("selected workers size less then expected, workers size: %v\n", len(workers)))
	}
	workers, err = SelectWorkers(int(proto.WorkerTypeCompact), "", 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("select workers failed cause : %s\n", err.Error()))
	}
	if len(workers) < 1 {
		t.Fatalf(fmt.Sprintf("selected workers size less then expected, workers size: %v\n", len(workers)))
	}

	time.Sleep(time.Second * 1)
	// select exception workers that not update for a long time(1 second for test),
	workers, err = SelectExceptionWorkers(int(proto.WorkerTypeSmartVolume), 1)
	err = AddExceptionWorkersToHistory(workers)
	if err != nil {
		t.Fatalf(fmt.Sprintf("add exceptioned workers to history failed cause : %s\n", err.Error()))
	}
	// delete workers
	err = DeleteExceptionWorkers(workers)
	if err != nil {
		t.Fatalf(fmt.Sprintf("delete exceptioned workers from worker table failed cause : %s\n", err.Error()))
	}

	workers, err = SelectExceptionWorkers(int(proto.WorkerTypeCompact), 1)
	fmt.Printf("selected workers size: %v\n", len(workers))
	err = AddExceptionWorkersToHistory(workers)
	if err != nil {
		t.Fatalf(fmt.Sprintf("add exceptioned workers to history failed cause : %s\n", err.Error()))
	}
	// delete workers
	err = DeleteExceptionWorkers(workers)
	if err != nil {
		t.Fatalf(fmt.Sprintf("delete exceptioned workers from worker table failed cause : %s\n", err.Error()))
	}
	fmt.Println("delete all exception worker nodes")
}

// select all the workers of specified worker type
func TestSelectWorker(t *testing.T) {
	var workerType = proto.WorkerTypeSmartVolume
	var limit = 3
	var offset = 0
	var count int
	for {
		workers, err := SelectWorker(int(workerType), limit, offset)
		if len(workers) == 0 {
			fmt.Println("finished.")
			break
		}
		for _, worker := range workers {
			fmt.Println(fmt.Sprintf("workerId: %v, workerType: %v, localIP: %v, createTime: %v, updateTime: %v",
				worker.WorkerId, worker.WorkerType, worker.WorkerAddr, worker.CreateTime, worker.UpdateTime))
		}
		count += len(workers)
		if err != nil {
			t.Fatalf(err.Error())
		}
		offset += len(workers)
	}
	fmt.Println(fmt.Sprintf("count: %d", count))
}
