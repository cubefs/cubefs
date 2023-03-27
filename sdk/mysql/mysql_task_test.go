package mysql

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"testing"
)

func TestTaskCommands(t *testing.T) {
	var (
		cluster       = "sparkcfs.jd.local"
		volumeName    = "vol_liyubo"
		dpId1         = 101
		dpId2         = 102
		dpId3         = 103
		taskInfo      = "testTaskInfo"
		taskInfoNew   = "testTaskInfoNew"
		exceptionInfo = "testExceptionInfo"
		err           error
		workerAddr    = "10.18.109.101"
	)
	// add task
	task1 := &proto.Task{
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  volumeName,
		DpId:     uint64(dpId1),
		TaskInfo: taskInfo,
		Status:   proto.TaskStatusUnallocated,
	}
	task2 := &proto.Task{
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  volumeName,
		DpId:     uint64(dpId2),
		TaskInfo: taskInfo,
		Status:   proto.TaskStatusUnallocated,
	}
	task3 := &proto.Task{
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  volumeName,
		DpId:     uint64(dpId3),
		TaskInfo: taskInfo,
		Status:   proto.TaskStatusUnallocated,
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

	// allocate task1
	err = AllocateTask(task1, workerAddr)
	if err != nil {
		t.Fatalf(fmt.Sprintf("allocate task1 failed cause : %s\n", err.Error()))
	}

	// select allocated tasks
	var tasks []*proto.Task
	tasks, err = SelectAllocatedTask(workerAddr, int(proto.WorkerTypeSmartVolume), 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("select allocated tasks failed cause : %s\n", err.Error()))
	}
	if len(tasks) < 1 {
		t.Fatalf(fmt.Sprintf("selected allocated tasks less then expected, tasks size : %v\n", len(tasks)))
	}

	tasks, err = SelectUnallocatedTasks(int(proto.WorkerTypeSmartVolume), 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("select unallocated tasks failed cause : %s\n", err.Error()))
	}
	if len(tasks) < 2 {
		t.Fatalf(fmt.Sprintf("selected unallocated tasks less then expected, tasks size : %v\n", len(tasks)))
	}
	var containTask2, containTask3 bool
	for _, task := range tasks {
		if task.TaskId == taskId2 {
			containTask2 = true
		}
		if task.TaskId == taskId3 {
			containTask3 = true
		}
	}
	if !containTask2 {
		t.Fatalf(fmt.Sprintf("task2 not found via selected unallocated tasks\n"))
	}
	if !containTask3 {
		t.Fatalf(fmt.Sprintf("task3 not found via selected unallocated tasks\n"))
	}

	// update task info
	err = UpdateTaskInfo(task1.TaskId, taskInfoNew)
	if err != nil {
		t.Fatalf(fmt.Sprintf("update task1 task info failed cause : %s\n", err.Error()))
	}

	// update task status via source status and target status
	err = UpdateTasksStatus(tasks, proto.TaskStatusUnallocated, proto.TaskStatusAllocated)
	if err != nil {
		t.Fatalf(fmt.Sprintf("update tasks status via source and target status failed cause : %s\n", err.Error()))
	}

	// update task status via task instance
	task2.Status = proto.TaskStatusPending
	err = UpdateTaskStatus(task2)
	if err != nil {
		t.Fatalf(fmt.Sprintf("update task2 status to pengding failed cause : %s\n", err.Error()))
	}
	task3.Status = proto.TaskStatusPending
	err = UpdateTaskStatus(task3)
	if err != nil {
		t.Fatalf(fmt.Sprintf("update task3 status to pengding failed cause : %s\n", err.Error()))
	}

	// select task
	task2Selected, err := SelectTask(int64(task2.TaskId))
	if err != nil {
		t.Fatalf(fmt.Sprintf("selected task2 via task id failed cause : %s\n", err.Error()))
	}
	if task2Selected.Status != proto.TaskStatusPending {
		t.Fatalf(fmt.Sprintf("selected task2 status is not expected, status: %v, expected: %v\n", task2Selected.Status, proto.TaskStatusPending))
	}

	// select task via task type
	tasks, err = SelectTasksWithType(int(proto.WorkerTypeSmartVolume), 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("select tasks via task type failed cause : %s\n", err.Error()))
	}
	if len(tasks) < 3 {
		t.Fatalf(fmt.Sprintf("selected tasks via type less then expected, tasks size : %v\n", len(tasks)))
	}

	// allocate task2 and task3
	err = AllocateTask(task2, workerAddr)
	if err != nil {
		t.Fatalf(fmt.Sprintf("allocate task2 failed cause : %s\n", err.Error()))
	}
	err = AllocateTask(task3, workerAddr)
	if err != nil {
		t.Fatalf(fmt.Sprintf("allocate task3 failed cause : %s\n", err.Error()))
	}

	// select running tasks
	tasks, err = SelectRunningTasks(int(proto.WorkerTypeSmartVolume))
	if err != nil {
		t.Fatalf(fmt.Sprintf("select tasks via task type failed cause : %s\n", err.Error()))
	}
	if len(tasks) < 3 {
		t.Fatalf(fmt.Sprintf("selected tasks via type less then expected, tasks size : %v\n", len(tasks)))
	}

	// update task status via source status and target status
	err = UpdateTasksStatus(tasks, proto.TaskStatusAllocated, proto.TaskStatusFailed)
	if err != nil {
		t.Fatalf(fmt.Sprintf("update tasks status via source and target status failed cause : %s\n", err.Error()))
	}
	err = UpdateTaskFailed(task2, exceptionInfo)
	if err != nil {
		t.Fatalf(fmt.Sprintf("update task2 failed info failed cause : %s\n", err.Error()))
	}
	err = UpdateTaskFailed(task3, exceptionInfo)
	if err != nil {
		t.Fatalf(fmt.Sprintf("update task3 failed info failed cause : %s\n", err.Error()))
	}
	// select exception tasks via worker node
	tasks, err = SelectExceptionTasks(int(proto.WorkerTypeSmartVolume), 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("select failed tasks failed cause : %s\n", err.Error()))
	}

	// update task status via source status and target status
	err = UpdateTasksStatus(tasks, proto.TaskStatusFailed, proto.TaskStatusSucceed)
	if err != nil {
		t.Fatalf(fmt.Sprintf("update tasks status via source and target status failed cause : %s\n", err.Error()))
	}
	// select exception tasks via worker node
	tasks, err = SelectSucceedTasks(int(proto.WorkerTypeSmartVolume), 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("select succeed tasks failed cause : %s\n", err.Error()))
	}

	// list tasks by page
	tasks, err = SelectTasks(cluster, "", 0, 0, int(proto.WorkerTypeSmartVolume), 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("list tasks via cluster failed cause : %s\n", err.Error()))
	}
	tasks, err = SelectTasks(cluster, volumeName, uint64(dpId1), 0, int(proto.WorkerTypeSmartVolume), 100, 0)
	if err != nil {
		t.Fatalf(fmt.Sprintf("list tasks via dpId failed cause : %s\n", err.Error()))
	}

	// check dp task existed
	res, _, err := CheckDPTaskExist(cluster, volumeName, int(proto.WorkerTypeSmartVolume), uint64(dpId1))
	if err != nil {
		t.Fatalf(fmt.Sprintf("check task is whether exist via dpId failed cause : %s\n", err.Error()))
	}
	if !res {
		t.Fatalf(fmt.Sprintf("task not exist check via dpId\n"))
	}

	// delete task
	err = DeleteTask(int64(task1.TaskId))
	if err != nil {
		t.Fatalf(fmt.Sprintf("delete task1 failed cause : %s\n", err.Error()))
	}
	// delete tasks
	err = DeleteTasks(tasks)
	if err != nil {
		t.Fatalf(fmt.Sprintf("delete tasks failed cause : %s\n", err.Error()))
	}
	fmt.Println("clean all tasks")
}

func TestUpdateTaskStatus(t *testing.T) {
	var (
		cluster       = "sparkcfs.jd.local"
		volumeName    = "vol_liyubo"
		dpId2         = 102
		taskInfo      = "testTaskInfo"
		exceptionInfo = "testExceptionInfo"
		err           error
		taskId        uint64
		taskQ         *proto.Task
	)

	task := &proto.Task{
		TaskType:      proto.WorkerTypeSmartVolume,
		Cluster:       cluster,
		VolName:       volumeName,
		DpId:          uint64(dpId2),
		TaskInfo:      taskInfo,
		ExceptionInfo: exceptionInfo,
		Status:        proto.TaskStatusUnallocated,
	}
	taskId, err = AddTask(task)
	if err != nil {
		t.Fatalf(fmt.Sprintf("add task failed cause : %s\n", err.Error()))
	}

	task.TaskId = taskId
	err = UpdateTaskFailed(task, exceptionInfo)
	if err != nil {
		t.Fatalf(fmt.Sprintf("updata task to be failed has exception cause : %s\n", err.Error()))
	}

	task.Status = proto.TaskStatusSucceed
	err = UpdateTaskStatus(task)
	if err != nil {
		t.Fatalf(fmt.Sprintf("update task status failed cause : %s\n", err.Error()))
	}

	taskQ, err = SelectTask(int64(taskId))
	if err != nil {
		t.Fatalf(fmt.Sprintf("select task failed cause : %s\n", err.Error()))
	}
	if taskQ == nil {
		t.Fatalf(fmt.Sprintf("selected task is nil\n"))
	}
	if taskQ.ExceptionInfo != "" {
		t.Fatalf(fmt.Sprintf("can not clean task exception info\n"))
	}

	// delete task
	err = DeleteTask(int64(taskId))
	if err != nil {
		t.Fatalf(fmt.Sprintf("delete task failed cause : %s\n", err.Error()))
	}
}

func TestUpdateTaskWorkerAddr(t *testing.T) {
	var (
		cluster    = "sparkcfs.jd.local"
		volumeName = "vol_liyubo"
		dpId       = 100
		taskInfo   = "testTaskInfo"
		workerAddr = "10.18.109.101"
	)
	task := &proto.Task{
		TaskType: proto.WorkerTypeSmartVolume,
		Cluster:  cluster,
		VolName:  volumeName,
		DpId:     uint64(dpId),
		TaskInfo: taskInfo,
		Status:   proto.TaskStatusUnallocated,
	}
	taskId, err := AddTask(task)
	if err != nil {
		t.Fatalf(fmt.Sprintf("add task1 failed cause : %s\n", err.Error()))
	}
	task.TaskId = taskId
	task.WorkerAddr = workerAddr
	if err := UpdateTaskWorkerAddr(task); err != nil {
		t.Fatalf(fmt.Sprintf("update task worker addr failed cause : %s\n", err.Error()))
	}
}

func TestSelectNotFinishedTask(t *testing.T) {
	workerAddr := "100.124.22.127"
	taskType := proto.WorkerTypeSmartVolume

	var limit = 2
	var offset = 0
	var count int
	for {
		tasks, err := SelectNotFinishedTask(workerAddr, int(taskType), limit, offset)
		if len(tasks) == 0 {
			fmt.Println("finished.")
			break
		}
		for _, task := range tasks {
			fmt.Println(fmt.Sprintf("taskId: %v, taskType: %v, taskStatus: %v, clusterName: %v, volName: %v, createTime: %v, updateTime: %v",
				task.TaskId, task.TaskType, task.Status, task.Cluster, task.VolName, task.CreateTime, task.UpdateTime))
		}
		count += len(tasks)
		if err != nil {
			t.Fatalf(err.Error())
		}
		offset += len(tasks)
	}
	fmt.Println(fmt.Sprintf("count: %d", count))
}
