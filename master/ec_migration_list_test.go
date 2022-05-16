package master

import (
	"testing"
)

func TestEC_MigrateList(t *testing.T) {
	d := NewMigrateList()
	if d == nil {
		t.Errorf("newMigrateList error")
	}

	d.Push(&MigrateTask{
		Status:      EcTaskRetry,
		RetryTimes:  0,
		PartitionID: 1,
	})

	d.Push(&MigrateTask{
		Status:      EcTaskFail,
		RetryTimes:  0,
		PartitionID: 3,
	})

	if _, exist := d.Exists(1); !exist {
		t.Errorf("push error")
	}

	if allTasks := d.GetAllTask(); allTasks == nil {
		t.Errorf("get allTask error")
	}
	if failTasks := d.GetFailTask(); failTasks == nil {
		t.Errorf("get failTask error")
	}
	if retryTasks := d.GetRetryTask(); retryTasks == nil {
		t.Errorf("get retryTask error")
	}

	if d.Len() == 0 {
		t.Errorf("push task error")
	}

	d.Remove(1)
	if _, exist := d.Exists(1); exist {
		t.Errorf("push error")
	}

	d.Clear()
	if d.Len() > 0 {
		t.Errorf("clear error")
	}
}
