package master

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEC_MigrateList(t *testing.T) {
	d := NewMigrateList()
	assert.NotNil(t, d, "newMigrateList error")

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

	_, exist := d.Exists(1)
	assert.True(t, exist, "push error")

	allTasks := d.GetAllTask()
	assert.NotNil(t, allTasks, "get allTask error")

	failTasks := d.GetFailTask()
	assert.NotNil(t, failTasks, "get failTasks error")

	retryTasks := d.GetRetryTask()
	assert.NotNil(t, retryTasks, "get retryTasks error")

	assert.NotZero(t, d.Len(), "push task error")

	d.Remove(1)

	_, exist = d.Exists(1)
	assert.False(t, exist, "push error")

	d.Clear()
	assert.LessOrEqual(t, d.Len(), 0, "clear error")
}
