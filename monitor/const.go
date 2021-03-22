package monitor

const (
	colonSplit        = ":"
	ConfigCluster     = "cluster"
	ConfigListenPort  = "listen"
	ConfigHBaseZK     = "zkQuorum"
	ConfigZKRoot      = "zkRoot"
	ConfigNamespace   = "namespace"
	ConfigQueryIP     = "queryIP"
	ConfigTopic       = "topic"
	ConfigJMQAddress  = "jmqAddr"
	ConfigJMQClientID = "jmqClientId"

	nodeIPKey   = "ip"
	moduleKey   = "module"
	opKey       = "op"
	timeKey     = "time"
	timeUnitKey = "unit"
	volKey      = "vol"
	pidKey      = "pid"

	tableKey = "table"
	startKey = "start"
	endKey   = "end"
	limitKey = "limit"
)

const (
	defaultTimeUnit = "minute"
	defaultTable    = "minute"
	defaultLimit    = 10

	defaultZkRoot    = "/hbase"
	defaultNamespace = "default"
	defaultQueryIP   = "11.56.17.222"

	topK       = 10
	timeLayout = "20060102150405"
)
