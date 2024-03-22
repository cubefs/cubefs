package proto

import (
	"flag"
	"fmt"
	"strconv"

	"github.com/cubefs/cubefs/util/auth"
	"github.com/cubefs/cubefs/util/config"
)

// For client
const (
	// Mandatory
	MountPoint int = iota
	VolName
	Owner
	Master
	// Optional
	LogDir
	WarnLogDir
	LogLevel
	ProfPort
	IcacheTimeout
	LookupValid
	AttrValid
	ReadRate
	WriteRate
	EnSyncWrite
	AutoInvalData
	Rdonly
	WriteCache
	KeepCache
	FollowerRead
	Authenticate
	ClientKey
	TicketHost
	EnableHTTPS
	CertFile
	AccessKey
	SecretKey
	DisableDcache
	SubDir
	FsyncOnClose
	MaxCPUs
	EnableXattr
	NearRead
	EnablePosixACL
	EnableSummary
	EnableUnixPermission
	RequestTimeout

	// adls
	VolType
	EbsEndpoint
	EbsServerPath
	CacheAction
	EbsBlockSize
	EnableBcache
	BcacheDir
	BcacheFilterFiles
	BcacheBatchCnt
	BcacheCheckIntervalS
	ReadThreads
	WriteThreads
	MetaSendTimeout
	BuffersTotalLimit
	MaxStreamerLimit
	EnableAudit

	LocallyProf
	MinWriteAbleDataPartitionCnt
	FileSystemName

	// snapshot
	SnapshotReadVerSeq

	DisableMountSubtype
	PrefetchThread
	MaxBackground
	CongestionThresh
	Profile

	MaxMountOption
)

// For server
const (
	MasterAddr        = "masterAddr"
	ListenPort        = "listen"
	ObjectNodeDomain  = "objectNodeDomain"
	BindIpKey         = "bindIp"
	ProfileAiPrefetch = "ai_prefetch"
)

type MountOption struct {
	keyword      string
	description  string
	cmdlineValue string
	value        interface{}
	hasConfig    bool // is this option in config file
}

func NewMountOption(keyword string, description string, value interface{}) MountOption {
	return MountOption{keyword, description, "", value, false}
}

func (opt MountOption) String() string {
	return fmt.Sprintf("[%v] %T: %v", opt.keyword, opt.value, opt.value)
}

func NewMountOptions() []MountOption {
	opts := make([]MountOption, MaxMountOption)
	return opts
}

func InitMountOptions(opts []MountOption) {
	opts[MountPoint] = NewMountOption("mountPoint", "Mount Point", "")
	opts[VolName] = NewMountOption("volName", "Volume Name", "")
	opts[Owner] = NewMountOption("owner", "Owner", "")
	opts[Master] = NewMountOption(MasterAddr, "Master Address", "")
	opts[LogDir] = NewMountOption("logDir", "Log Path", "")
	opts[WarnLogDir] = NewMountOption("warnLogDir", "Warn Log Path", "")
	opts[LogLevel] = NewMountOption("logLevel", "Log Level", "")
	opts[ProfPort] = NewMountOption("profPort", "PProf Port", "")
	opts[LocallyProf] = NewMountOption("locallyProf", "Locally PProf", false)
	opts[IcacheTimeout] = NewMountOption("icacheTimeout", "Inode Cache Expiration Time", int64(-1))
	opts[LookupValid] = NewMountOption("lookupValid", "Lookup Valid Duration", int64(-1))
	opts[AttrValid] = NewMountOption("attrValid", "Attr Valid Duration", int64(-1))
	opts[ReadRate] = NewMountOption("readRate", "Read Rate Limit", int64(-1))
	opts[WriteRate] = NewMountOption("writeRate", "Write Rate Limit", int64(-1))
	opts[EnSyncWrite] = NewMountOption("enSyncWrite", "Enable Sync Write", int64(-1))
	opts[AutoInvalData] = NewMountOption("autoInvalData", "Auto Invalidate Data", int64(-1))
	opts[Rdonly] = NewMountOption("rdonly", "Mount as readonly", false)
	opts[WriteCache] = NewMountOption("writecache", "Enable FUSE writecache feature", false)
	opts[KeepCache] = NewMountOption("keepcache", "Enable FUSE keepcache feature", false)
	opts[FollowerRead] = NewMountOption("followerRead", "Enable read from follower", false)
	opts[NearRead] = NewMountOption("nearRead", "Enable read from nearest node", false)

	opts[Authenticate] = NewMountOption("authenticate", "Enable Authenticate", false)
	opts[ClientKey] = NewMountOption("clientKey", "Client Key", "")
	opts[TicketHost] = NewMountOption("ticketHost", "Ticket Host", "")
	opts[EnableHTTPS] = NewMountOption("enableHTTPS", "Enable HTTPS", false)
	opts[CertFile] = NewMountOption("certFile", "Cert File", "")

	opts[AccessKey] = NewMountOption("accessKey", "Access Key", "")
	opts[SecretKey] = NewMountOption("secretKey", "Secret Key", "")

	opts[DisableDcache] = NewMountOption("disableDcache", "Disable Dentry Cache", false)
	opts[SubDir] = NewMountOption("subdir", "Mount sub directory", "")
	opts[FsyncOnClose] = NewMountOption("fsyncOnClose", "Perform fsync upon file close", true)
	opts[MaxCPUs] = NewMountOption("maxcpus", "The maximum number of CPUs that can be executing", int64(-1))
	opts[EnableXattr] = NewMountOption("enableXattr", "Enable xattr support", false)
	opts[EnablePosixACL] = NewMountOption("enablePosixACL", "enable posix ACL support", false)
	opts[EnableSummary] = NewMountOption("enableSummary", "Enable content summary", false)
	opts[EnableUnixPermission] = NewMountOption("enableUnixPermission", "Enable unix permission check(e.g: 777/755)", false)

	opts[VolType] = NewMountOption("volType", "volume type", int64(0))
	opts[EbsEndpoint] = NewMountOption("ebsEndpoint", "Ebs service address", "")
	opts[EbsServerPath] = NewMountOption("ebsServerPath", "Ebs service path", "")
	opts[CacheAction] = NewMountOption("cacheAction", "Cold cache action", int64(0))
	opts[EbsBlockSize] = NewMountOption("ebsBlockSize", "Ebs object size", "")
	// opts[EnableBcache] = MountOption{"enableBcache", "Enable block cache", "", false)
	opts[BcacheDir] = NewMountOption("bcacheDir", "block cache dir", "")
	opts[ReadThreads] = NewMountOption("readThreads", "Cold volume read threads", int64(10))
	opts[WriteThreads] = NewMountOption("writeThreads", "Cold volume write threads", int64(10))
	opts[MetaSendTimeout] = NewMountOption("metaSendTimeout", "Meta send timeout", int64(600))
	opts[BuffersTotalLimit] = NewMountOption("buffersTotalLimit", "Send/Receive packets memory limit", int64(32768)) // default 4G
	opts[MaxStreamerLimit] = NewMountOption("maxStreamerLimit", "The maximum number of streamers", int64(0))         // default 0
	opts[BcacheFilterFiles] = NewMountOption("bcacheFilterFiles", "The block cache filter files suffix", "py;pyx;sh;yaml;conf;pt;pth;log;out")
	opts[BcacheBatchCnt] = NewMountOption("bcacheBatchCnt", "The block cache get meta count", int64(100000))
	opts[BcacheCheckIntervalS] = NewMountOption("bcacheCheckIntervalS", "The block cache check interval", int64(300))
	opts[EnableAudit] = NewMountOption("enableAudit", "enable client audit logging", false)
	opts[RequestTimeout] = NewMountOption("requestTimeout", "The Request Expiration Time", int64(0))
	opts[MinWriteAbleDataPartitionCnt] = NewMountOption("minWriteAbleDataPartitionCnt",
		"Min writeable data partition count retained int dpSelector when update DataPartitionsView from master", int64(10))

	opts[FileSystemName] = NewMountOption("fileSystemName", "The explicit name of the filesystem", "")
	opts[SnapshotReadVerSeq] = NewMountOption("snapshotReadSeq", "Snapshot read seq", int64(0)) // default false
	opts[DisableMountSubtype] = NewMountOption("disableMountSubtype", "Disable Mount Subtype", false)

	opts[PrefetchThread] = NewMountOption("prefetchThread", "start multiple threads to prefetch files", int64(0))
	opts[MaxBackground] = NewMountOption("maxBackground", "Set the count of kernel background requests", int64(0))
	opts[CongestionThresh] = NewMountOption("congestionThresh", "Set the congestion threshold of kernel background requests", int64(0))
	opts[Profile] = NewMountOption("profile", "config group for different situations", "")

	for i := 0; i < MaxMountOption; i++ {
		flag.StringVar(&opts[i].cmdlineValue, opts[i].keyword, "", opts[i].description)
	}
}

func ParseMountOptions(opts []MountOption, cfg *config.Config) {
	for i := 0; i < MaxMountOption; i++ {
		switch v := opts[i].value.(type) {
		case string:
			if opts[i].cmdlineValue != "" {
				opts[i].value = opts[i].cmdlineValue
			} else {
				if value, present := cfg.CheckAndGetString(opts[i].keyword); present {
					opts[i].value = value
					opts[i].hasConfig = true
				} else {
					opts[i].value = v
				}
			}
			fmt.Printf("keyword[%v] value[%v] type[%T]\n", opts[i].keyword, opts[i].value, v)

		case int64:
			if opts[i].cmdlineValue != "" {
				opts[i].value = parseInt64(opts[i].cmdlineValue)
			} else {
				if present := cfg.HasKey(opts[i].keyword); present {
					opts[i].value = cfg.GetInt64(opts[i].keyword)
					opts[i].hasConfig = true
				} else {
					opts[i].value = v
				}
			}
			fmt.Printf("keyword[%v] value[%v] type[%T]\n", opts[i].keyword, opts[i].value, v)

		case bool:
			if opts[i].cmdlineValue != "" {
				opts[i].value = parseBool(opts[i].cmdlineValue)
			} else {
				if value, present := cfg.CheckAndGetBool(opts[i].keyword); present {
					opts[i].value = value
					opts[i].hasConfig = true
				} else {
					opts[i].value = v
				}
			}
			fmt.Printf("keyword[%v] value[%v] type[%T]\n", opts[i].keyword, opts[i].value, v)

		default:
			fmt.Printf("keyword[%v] unknown type[%T]\n", opts[i].keyword, v)
		}
	}
}

func parseInt64(s string) int64 {
	var ret int64 = -1

	if s != "" {
		val, err := strconv.Atoi(s)
		if err == nil {
			ret = int64(val)
		}
	}
	return ret
}

func parseBool(s string) bool {
	var ret bool = false

	if s == "true" {
		ret = true
	}
	return ret
}

func (opt *MountOption) GetString() string {
	val, ok := opt.value.(string)
	if !ok {
		return ""
	}
	return val
}

func (opt *MountOption) GetBool() bool {
	val, ok := opt.value.(bool)
	if !ok {
		return false
	}
	return val
}

func (opt *MountOption) GetInt64() int64 {
	val, ok := opt.value.(int64)
	if !ok {
		return int64(-1)
	}
	return val
}

func (opt *MountOption) HasConfig() bool {
	return opt.hasConfig
}

type MountOptions struct {
	Config                       *config.Config
	MountPoint                   string
	Volname                      string
	Owner                        string
	Master                       string
	Logpath                      string
	Loglvl                       string
	Profport                     string
	LocallyProf                  bool
	IcacheTimeout                int64
	LookupValid                  int64
	AttrValid                    int64
	ReadRate                     int64
	WriteRate                    int64
	EnSyncWrite                  int64
	AutoInvalData                int64
	UmpDatadir                   string
	Rdonly                       bool
	WriteCache                   bool
	KeepCache                    bool
	FollowerRead                 bool
	Authenticate                 bool
	TicketMess                   auth.TicketMess
	TokenKey                     string
	AccessKey                    string
	SecretKey                    string
	DisableDcache                bool
	SubDir                       string
	FsyncOnClose                 bool
	MaxCPUs                      int64
	EnableXattr                  bool
	NearRead                     bool
	EnablePosixACL               bool
	EnableQuota                  bool
	EnableTransaction            string
	TxTimeout                    int64
	TxConflictRetryNum           int64
	TxConflictRetryInterval      int64
	VolType                      int
	EbsEndpoint                  string
	EbsServicePath               string
	CacheAction                  int
	CacheThreshold               int
	EbsBlockSize                 int
	EnableBcache                 bool
	BcacheDir                    string
	BcacheFilterFiles            string
	BcacheCheckIntervalS         int64
	BcacheBatchCnt               int64
	ReadThreads                  int64
	WriteThreads                 int64
	EnableSummary                bool
	EnableUnixPermission         bool
	NeedRestoreFuse              bool
	MetaSendTimeout              int64
	BuffersTotalLimit            int64
	MaxStreamerLimit             int64
	EnableAudit                  bool
	RequestTimeout               int64
	MinWriteAbleDataPartitionCnt int
	FileSystemName               string
	VerReadSeq                   uint64
	// disable mount subtype
	DisableMountSubtype bool
	PrefetchThread			 	 int64
	MaxBackground			 	 int64
	CongestionThresh		 	 int64
	Profile                      string
}
