package convertnode

const (
	colonSplit        	= ":"
	ConfigCluster     	= "clusters"
	ConfigListenPort  	= "listen"
	ConfigProcessorNum	= "processorNum"
	ConfigMySQLDB       = "mysqlConfig"
)

const (
	defaultMultiTask			= 4
	defaultMaxMultiTask 		= 10
	defaultProcessorInterval 	= 5   //minute
)

const (
	CONVERTLIST   		= "/list"
	CONVERTINFO			= "/info"
	CONVERTSTART		= "/start"
	CONVERTSTOP			= "/stop"
	CONVERTADD			= "/add"
	CONVERTDEL			= "/del"
	CONVERTSETPROCESSOR = "/setProcessor"

)

const (
	CLIObjTypeKey				= "objectType"
	CLIObjClusterType  		    = "cluster"
	CLIObjProcessorType     	= "processor"
	CLIObjTaskType		  		= "task"
	CLIObjServerType			= "server"
)

const defaultMinus = 5
