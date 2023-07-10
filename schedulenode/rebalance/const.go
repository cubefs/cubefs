package rebalance

import "time"

const (
	RBStart  = "/rebalance/start"
	RBStop   = "/rebalance/stop"
	RBStatus = "/rebalance/status"
	RBReset  = "/rebalance/reset"
)

const (
	ParamCluster              = "cluster"
	ParamZoneName             = "zoneName"
	ParamHighRatio            = "highRatio"
	ParamLowRatio             = "lowRatio"
	ParamAverageRatio         = "averageRatio"
	ParamGoalRatio            = "goalRatio"
	ParamClusterMaxBatchCount = "maxBatchCount"
	ParamMigrateLimitPerDisk  = "migrateLimit"
)

const (
	_ Status = iota
	StatusStop
	StatusRunning
	StatusTerminating
)

const (
	defaultMinWritableDPNum     = 2
	defaultClusterMaxBatchCount = 50
	defaultInterval             = time.Minute * 5
)

const (
	ReadOnly    = 1
	ReadWrite   = 2
	Unavailable = -1
)
