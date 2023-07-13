package mysql

import (
	"database/sql"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func AddCheckRule(rule *proto.CheckRule) (err error) {
	sqlCmd := "insert into check_rules(task_type, cluster_id, rule_type, rule_value)" +
		" values(?, ?, ?, ?)"
	args := make([]interface{}, 0)
	args = append(args, int8(rule.WorkerType))
	args = append(args, rule.ClusterID)
	args = append(args, rule.RuleType)
	args = append(args, rule.RuleValue)


	if _, err = Transaction(sqlCmd, args); err != nil {
		log.LogErrorf("")
		return
	}
	return
}

func SelectCheckRule(workerType int, clusterID string) (rules []*proto.CheckRule, err error) {
	var rows *sql.Rows
	sqlCmd := fmt.Sprintf("select task_type, cluster_id, rule_type, rule_value from check_rules where cluster_id = ? and task_type = ?")
	rows, err = db.Query(sqlCmd, clusterID, workerType)
	if rows == nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		rule := &proto.CheckRule{}
		err = rows.Scan(&rule.WorkerType, &rule.ClusterID, &rule.RuleType, &rule.RuleValue)
		if err != nil {
			return
		}
		rules = append(rules, rule)
	}
	return
}