package mysql

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"testing"
	"time"
)

func TestTaskHistory(t *testing.T) {
	var (
		TaskId   = 1000
		cluster  = "sparkcfs.jd.local"
		VolName  = "vol_liyubo"
		TaskInfo = "testTaskInfo"
		DPId     = 100
	)
	task := &proto.Task{
		TaskId:     uint64(TaskId),
		TaskType:   proto.WorkerTypeSmartVolume,
		Cluster:    cluster,
		VolName:    VolName,
		DpId:       uint64(DPId),
		TaskInfo:   TaskInfo,
		Status:     proto.TaskStatusUnallocated,
		CreateTime: time.Now(),
		UpdateTime: time.Now(),
	}

	err := AddTaskToHistory(task)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// init exception tasks
	task1 := &proto.Task{
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  VolName,
		DpId:     uint64(DPId+1),
		TaskInfo: TaskInfo,
		Status:   proto.TaskStatusFailed,
	}
	task2 := &proto.Task{
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  VolName,
		DpId:     uint64(DPId+2),
		TaskInfo: TaskInfo,
		Status:   proto.TaskStatusFailed,
	}
	task3 := &proto.Task{
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  VolName,
		DpId:     uint64(DPId+3),
		TaskInfo: TaskInfo,
		Status:   proto.TaskStatusFailed,
	}

	var taskId1, taskId2, taskId3 uint64
	taskId1, err = AddTask(task1)
	if err != nil {
		t.Fatalf(fmt.Sprintf("add task1 failed cause : %s\n", err.Error()))
	}
	fmt.Println(fmt.Sprintf("add task1 success, taskId1： %v", taskId1))
	taskId2, err = AddTask(task2)
	if err != nil {
		t.Fatalf(fmt.Sprintf("add task2 failed cause : %s\n", err.Error()))
	}
	fmt.Println(fmt.Sprintf("add task2 success, taskId2： %v", taskId2))
	taskId3, err = AddTask(task3)
	if err != nil {
		t.Fatalf(fmt.Sprintf("add task3 failed cause : %s\n", err.Error()))
	}
	fmt.Println(fmt.Sprintf("add task3 success, taskId3： %v", taskId3))
	task1.TaskId = taskId1
	task2.TaskId = taskId2
	task3.TaskId = taskId3

	tasks, err := SelectExceptionTasks(int(proto.WorkerTypeSmartVolume), 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("select exception tasks failed cause : %s\n", err.Error()))
	}
	if len(tasks) < 3 {
		t.Fatalf(fmt.Sprintf("selected tasks size less then expected, tasks size: %v\n", len(tasks)))
	}
	if len(tasks) > 0 {
		if err := AddTasksToHistory(tasks); err != nil {
			t.Fatalf(err.Error())
		}
	}

	// select history tasks
	_, err = SelectTaskHistory(cluster, VolName, 0, 0, 0, 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("select history tasks failed cause : %s\n", err.Error()))
	}
	_, err = SelectTaskHistory(cluster, VolName, uint64(DPId), 0, 0, 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("select history tasks failed cause : %s\n", err.Error()))
	}

	// delete tasks
	err = DeleteTasks(tasks)
	if err != nil {
		t.Fatalf(fmt.Sprintf("delete tasks failed cause : %s\n", err.Error()))
	}
	fmt.Println("clean all tasks")

	// delete task histories
	var ths []*proto.TaskHistory
	ths = append(ths, &proto.TaskHistory{Task: *task})
	ths = append(ths, &proto.TaskHistory{Task: *task1})
	ths = append(ths, &proto.TaskHistory{Task: *task2})
	ths = append(ths, &proto.TaskHistory{Task: *task3})
	err = DeleteTaskHistories(ths)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println("clean all task histories")
}

func TestSelectTaskHistory(t *testing.T) {
	cluster := "spark"
	volume := "smartTest"
	taskType := 0
	dpId := 0
	mpId := 0
	limit := 10
	offset := 0
	taskHistories, err := SelectTaskHistory(cluster, volume, uint64(dpId), uint64(mpId), taskType, limit, offset)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Printf("result rows : %v\n", len(taskHistories))
	for _, history := range taskHistories {
		fmt.Printf("TaskId: %v, Cluster: %v, VolName: %v, DpId: %v, MpId: %v, CreateTime: %v, UpdateTime: %v \n",
			history.TaskId, history.Cluster, history.VolName, history.DpId, history.MpId, history.CreateTime, history.UpdateTime)
	}
}
