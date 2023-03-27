package scheduler

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/mysql"
	"testing"
	"time"
)

func TestGetWorkerNodesDiff(t *testing.T) {
	wn1 := &proto.WorkerNode{
		WorkerAddr: "100.124.22.101",
	}
	wn2 := &proto.WorkerNode{
		WorkerAddr: "100.124.22.102",
	}
	wn3 := &proto.WorkerNode{
		WorkerAddr: "100.124.22.103",
	}

	workerNodeNew := []*proto.WorkerNode{wn1, wn2}
	workerNodeOld := []*proto.WorkerNode{wn2, wn3}
	wns1 := getWorkerNodesDiff(workerNodeNew, workerNodeOld)
	if wns1 == nil || len(wns1) == 0 {
		t.Fatalf("not found")
	}
	for _, wn := range wns1 {
		fmt.Println(wn.WorkerAddr)
	}

	fmt.Println("---")
	workerNodeNew2 := []*proto.WorkerNode{wn1, wn2, wn3}
	workerNodeOld2 := []*proto.WorkerNode{wn1, wn2}
	wns2 := getWorkerNodesDiff(workerNodeNew2, workerNodeOld2)
	if wns2 == nil || len(wns2) == 0 {
		t.Fatalf("not found")
	}
	for _, wn := range wns2 {
		fmt.Println(wn.WorkerAddr)
	}
}

func TestGetLowestPayloadWorker(t *testing.T) {
	scheduleNode := NewScheduleNode()

	workerAddr := "11.67.206.145"
	maxNum := int64(1)
	key := flowControlKey(proto.WorkerTypeSmartVolume, proto.FlowTypeWorker, workerAddr)
	flow, _ := proto.NewFlowControl(proto.WorkerTypeSmartVolume, proto.FlowTypeWorker, workerAddr, maxNum)
	scheduleNode.flowControl.Store(key, flow)

	workerNodes := []*proto.WorkerNode{
		{
			WorkerId:   76,
			WorkerType: proto.WorkerTypeSmartVolume,
			WorkerAddr: "11.67.206.145",
			Tasks: []*proto.Task{
				{
					TaskId:   1,
					TaskType: proto.WorkerTypeSmartVolume,
					Cluster:  "spark",
					VolName:  "smartVolume",
					DpId:     101,
				},
			},
		},
		{
			WorkerId:   81,
			WorkerType: proto.WorkerTypeSmartVolume,
			WorkerAddr: "11.67.233.54",
			Tasks: []*proto.Task{
				{
					TaskId:   2,
					TaskType: proto.WorkerTypeSmartVolume,
					Cluster:  "spark",
					VolName:  "smartVolume",
					DpId:     101,
				},
				{
					TaskId:   3,
					TaskType: proto.WorkerTypeSmartVolume,
					Cluster:  "spark",
					VolName:  "smartVolume",
					DpId:     102,
				},
			},
		},
	}
	scheduleNode.workerNodes.Store(proto.WorkerTypeSmartVolume, workerNodes)
	wn := scheduleNode.getLowestPayloadWorker(proto.WorkerTypeSmartVolume)
	fmt.Println(wn.WorkerAddr)
	if wn.WorkerAddr != "11.67.233.54" {
		t.Errorf("selected worker node is not expected")
	}
}

func TestTasks(t *testing.T) {
	// store single task
	task1 := proto.NewDataTask(proto.WorkerTypeSmartVolume, "spark", "smartVolume", 101, 0, "test")
	task2 := proto.NewDataTask(proto.WorkerTypeSmartVolume, "spark", "smartVolume", 102, 0, "test")
	task3 := proto.NewDataTask(proto.WorkerTypeSmartVolume, "spark", "smartVolume", 103, 0, "test")
	task4 := proto.NewDataTask(proto.WorkerTypeSmartVolume, "spark", "smartVolume", 104, 0, "test")
	task1.TaskId = 101
	task2.TaskId = 102
	task3.TaskId = 103
	task4.TaskId = 104
	scheduleNode := NewScheduleNode()
	scheduleNode.tasks = make(map[string][]*proto.Task)
	scheduleNode.storeTasks(proto.WorkerTypeSmartVolume, []*proto.Task{task1, task2, task3})

	// store multiple tasks
	scheduleNode.storeClusterTasks(proto.WorkerTypeSmartVolume, "spark", []*proto.Task{task2, task3, task4})

	// get tasks
	tasks := scheduleNode.getRunningTasks(proto.WorkerTypeSmartVolume, "spark")
	for _, task := range tasks {
		fmt.Printf("taskId(%v), workerType(%v), cluster(%v), dpId(%v), taskInfo(%v)\n", task.TaskId, task.TaskType, task.Cluster, task.DpId, task.TaskInfo)
	}

	// remove
	scheduleNode.removeTask(proto.WorkerTypeSmartVolume, task1)
	scheduleNode.removeTask(proto.WorkerTypeSmartVolume, task2)
	scheduleNode.removeTask(proto.WorkerTypeSmartVolume, task3)
	scheduleNode.removeTask(proto.WorkerTypeSmartVolume, task4)

	// get tasks
	fmt.Printf("list after remove...\n")
	tasks2 := scheduleNode.getRunningTasks(proto.WorkerTypeSmartVolume, "spark")
	for _, task := range tasks2 {
		fmt.Printf("taskId(%v), workerType(%v), cluster(%v), dpId(%v), taskInfo(%v)\n", task.TaskId, task.TaskType, task.Cluster, task.DpId, task.TaskInfo)
	}
}

func TestRemoveTaskFromWorkerNode(t *testing.T) {
	cluster := "spark"
	volume := "smartVolume"
	scheduleNode := NewScheduleNode()
	scheduleNode.tasks = make(map[string][]*proto.Task)

	task1 := &proto.Task{
		TaskId:   1,
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  volume,
		DpId:     101,
	}
	task2 := &proto.Task{
		TaskId:   2,
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  volume,
		DpId:     102,
	}
	task3 := &proto.Task{
		TaskId:   3,
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  volume,
		DpId:     103,
	}
	task4 := &proto.Task{
		TaskId:   4,
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  volume,
		DpId:     104,
	}
	task5 := &proto.Task{
		TaskId:   5,
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  volume,
		DpId:     105,
	}
	task6 := &proto.Task{
		TaskId:   6,
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  volume,
		DpId:     106,
	}

	workerNodes := []*proto.WorkerNode{
		{
			WorkerId:   76,
			WorkerType: proto.WorkerTypeSmartVolume,
			WorkerAddr: "11.67.206.145",
			Tasks:      []*proto.Task{task1, task2, task6},
		},
		{
			WorkerId:   81,
			WorkerType: proto.WorkerTypeSmartVolume,
			WorkerAddr: "11.67.233.54",
			Tasks:      []*proto.Task{task3, task4, task5},
		},
	}
	scheduleNode.workerNodes.Store(proto.WorkerTypeSmartVolume, workerNodes)
	scheduleNode.storeTasks(proto.WorkerTypeSmartVolume, []*proto.Task{task1, task2, task3, task4, task5, task6})

	taskKey := flowControlKey(proto.WorkerTypeSmartVolume, proto.FlowTypeCluster, cluster)
	scheduleNode.removeTaskFromScheduleNode(proto.WorkerTypeSmartVolume, []*proto.Task{task5, task3})
	tasks := scheduleNode.tasks[taskKey]
	for _, t := range tasks {
		fmt.Printf("TaskId(%v), Cluster(%v), VolName(%v), DpId(%v)\n", t.TaskId, t.Cluster, t.VolName, t.DpId)
	}
}

func TestExceptionTask(t *testing.T) {
	cluster := "spark"
	volume := "smartVolume"
	u1 := "2022-06-29 03:44:46"
	u2 := "2022-06-29 05:44:47"
	u3 := "2022-06-29 09:48:32"
	var t1, t2, t3 time.Time
	var err error
	t1, err = mysql.FormatTime(u1)
	t2, err = mysql.FormatTime(u2)
	t3, err = mysql.FormatTime(u3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	task1 := &proto.Task{
		TaskId:     1,
		TaskType:   proto.WorkerTypeSmartVolume,
		Cluster:    cluster,
		VolName:    volume,
		DpId:       101,
		UpdateTime: t1,
	}
	task2 := &proto.Task{
		TaskId:     2,
		TaskType:   proto.WorkerTypeSmartVolume,
		Cluster:    cluster,
		VolName:    volume,
		DpId:       102,
		UpdateTime: t2,
	}
	task3 := &proto.Task{
		TaskId:     3,
		TaskType:   proto.WorkerTypeSmartVolume,
		Cluster:    cluster,
		VolName:    volume,
		DpId:       103,
		UpdateTime: t3,
	}

	var longExcTasks []*proto.Task
	tasks := []*proto.Task{task1, task2, task3}
	for _, task := range tasks {
		n := time.Now()
		s := task.UpdateTime
		fmt.Println(s)
		diff := n.Sub(task.UpdateTime).Seconds()
		if diff > DefaultExceptionTaskHandlePeriod {
			longExcTasks = append(longExcTasks, task)
		}
	}
	for _, task := range longExcTasks {
		fmt.Printf("taskId(%v), dpId(%v)\n", task.TaskId, task.DpId)
	}
}
