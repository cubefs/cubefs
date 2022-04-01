package mysql

import (
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

func AddCompactSummary(task *proto.Task, cmpEkCnt, newEkCnt, cmpInodeCnt, cmpCnt, cmpSize, cmpErrCnt uint64, cmpErrMsg string) (err error) {
	sqlCmd := "insert into compact_summary(task_id, task_type, cluster_name, vol_name, dp_id, mp_id, worker_addr, cmp_ek_cnt, new_ek_cnt, cmp_inode_cnt, cmp_cnt, cmp_size, cmp_err_cnt, cmp_err_msg, task_create_time, task_update_time) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	args := make([]interface{}, 0)
	args = append(args, task.TaskId)
	args = append(args, int8(task.TaskType))
	args = append(args, task.Cluster)
	args = append(args, task.VolName)
	args = append(args, task.DpId)
	args = append(args, task.MpId)
	args = append(args, task.WorkerAddr)
	args = append(args, cmpEkCnt)
	args = append(args, newEkCnt)
	args = append(args, cmpInodeCnt)
	args = append(args, cmpCnt)
	args = append(args, cmpSize)
	args = append(args, cmpErrCnt)
	args = append(args, cmpErrMsg)
	args = append(args, task.CreateTime)
	args = append(args, task.UpdateTime)
	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("[AddCompactSummary] add compact summary failed, cluster(%v), volName(%v), taskInfo(%v), err(%v)", task.Cluster, task.VolName, task.TaskInfo, err)
		return
	}
	return
}
