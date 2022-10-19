package monitor

const (
	colonSplit        = ":"
	ConfigCluster     = "cluster"
	ConfigListenPort  = "listen"
	ConfigThriftAddr  = "thriftAddr"
	ConfigNamespace   = "namespace"
	ConfigQueryIP     = "queryIP"
	ConfigTopic       = "topic"
	ConfigJMQAddress  = "jmqAddr"
	ConfigJMQClientID = "jmqClientId"
	ConfigProducerNum = "producerNum"
	ConfigExpiredDay  = "tableExpiredDay"
	ConfigSplitRegion = "splitRegion"

	nodeIPKey    = "ip"
	moduleKey    = "module"
	opKey        = "op"
	timeKey      = "time"
	groupKey     = "group"
	orderKey     = "order"
	volKey       = "vol"
	pidKey       = "pid"
	clusterKey   = "cluster"
	tableUnitKey = "tableUnit"
	tableKey     = "table"
	startKey     = "start"
	endKey       = "end"
	limitKey     = "limit"
)

const (
	defaultTableUnit	= "minute"
	defaultLimit    	= 10

	defaultThriftAddr	= "127.0.0.1:9091"
	defaultNamespace 	= "default"
	defaultQueryIP   	= "11.56.17.222"
	defaultProducerNum 	= 3

	timeLayout 		= "20060102150405"
	dayTimeLayout 	= "20060102000000"
)
