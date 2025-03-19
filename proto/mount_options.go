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
	StreamRetryTimeOut
	BufferChanSize
	BcacheOnlyForNotSSD
	// aheadread
	AheadReadEnable
	AheadReadTotalMemGB
	AheadReadBlockTimeOut
	AheadReadWindowCnt
	MaxMountOption
)

// For server
const (
	MasterAddr       = "masterAddr"
	ListenPort       = "listen"
	ObjectNodeDomain = "objectNodeDomain"
	BindIpKey        = "bindIp"
)

type MountOption struct {
	keyword      string
	description  string
	cmdlineValue string
	value        interface{}
}

func (opt MountOption) String() string {
	return fmt.Sprintf("[%v] %T: %v", opt.keyword, opt.value, opt.value)
}

func NewMountOptions() []MountOption {
	opts := make([]MountOption, MaxMountOption)
	return opts
}

func InitMountOptions(opts []MountOption) {
	opts[MountPoint] = MountOption{"mountPoint", "Mount Point", "", ""}
	opts[VolName] = MountOption{"volName", "Volume Name", "", ""}
	opts[Owner] = MountOption{"owner", "Owner", "", ""}
	opts[Master] = MountOption{MasterAddr, "Master Address", "", ""}
	opts[LogDir] = MountOption{"logDir", "Log Path", "", ""}
	opts[WarnLogDir] = MountOption{"warnLogDir", "Warn Log Path", "", ""}
	opts[LogLevel] = MountOption{"logLevel", "Log Level", "", ""}
	opts[ProfPort] = MountOption{"profPort", "PProf Port", "", ""}
	opts[LocallyProf] = MountOption{"locallyProf", "Locally PProf", "", false}
	opts[IcacheTimeout] = MountOption{"icacheTimeout", "Inode Cache Expiration Time", "", int64(-1)}
	opts[LookupValid] = MountOption{"lookupValid", "Lookup Valid Duration", "", int64(-1)}
	opts[AttrValid] = MountOption{"attrValid", "Attr Valid Duration", "", int64(-1)}
	opts[ReadRate] = MountOption{"readRate", "Read Rate Limit", "", int64(-1)}
	opts[WriteRate] = MountOption{"writeRate", "Write Rate Limit", "", int64(-1)}
	opts[EnSyncWrite] = MountOption{"enSyncWrite", "Enable Sync Write", "", int64(-1)}
	opts[AutoInvalData] = MountOption{"autoInvalData", "Auto Invalidate Data", "", int64(-1)}
	opts[Rdonly] = MountOption{"rdonly", "Mount as readonly", "", false}
	opts[WriteCache] = MountOption{"writecache", "Enable FUSE writecache feature", "", false}
	opts[KeepCache] = MountOption{"keepcache", "Enable FUSE keepcache feature", "", false}
	opts[FollowerRead] = MountOption{"followerRead", "Enable read from follower", "", false}
	opts[NearRead] = MountOption{"nearRead", "Enable read from nearest node", "", true}

	opts[Authenticate] = MountOption{"authenticate", "Enable Authenticate", "", false}
	opts[ClientKey] = MountOption{"clientKey", "Client Key", "", ""}
	opts[TicketHost] = MountOption{"ticketHost", "Ticket Host", "", ""}
	opts[EnableHTTPS] = MountOption{"enableHTTPS", "Enable HTTPS", "", false}
	opts[CertFile] = MountOption{"certFile", "Cert File", "", ""}

	opts[AccessKey] = MountOption{"accessKey", "Access Key", "", ""}
	opts[SecretKey] = MountOption{"secretKey", "Secret Key", "", ""}

	opts[DisableDcache] = MountOption{"disableDcache", "Disable Dentry Cache", "", false}
	opts[SubDir] = MountOption{"subdir", "Mount sub directory", "", ""}
	opts[FsyncOnClose] = MountOption{"fsyncOnClose", "Perform fsync upon file close", "", true}
	opts[MaxCPUs] = MountOption{"maxcpus", "The maximum number of CPUs that can be executing", "", int64(-1)}
	opts[EnableXattr] = MountOption{"enableXattr", "Enable xattr support", "", false}
	opts[EnablePosixACL] = MountOption{"enablePosixACL", "Enable posix ACL support", "", false}
	opts[EnableSummary] = MountOption{"enableSummary", "Enable content summary", "", false}
	opts[EnableUnixPermission] = MountOption{"enableUnixPermission", "Enable unix permission check(e.g: 777/755)", "", false}

	opts[VolType] = MountOption{"volType", "volume type", "", int64(0)}
	opts[EbsEndpoint] = MountOption{"ebsEndpoint", "Ebs service address", "", ""}
	opts[EbsServerPath] = MountOption{"ebsServerPath", "Ebs service path", "", ""}
	opts[CacheAction] = MountOption{"cacheAction", "Cold cache action", "", int64(0)}
	opts[EbsBlockSize] = MountOption{"ebsBlockSize", "Ebs object size", "", ""}
	// opts[EnableBcache] = MountOption{"enableBcache", "Enable block cache", "", false}
	opts[BcacheDir] = MountOption{"bcacheDir", "block cache dir", "", ""}
	opts[ReadThreads] = MountOption{"readThreads", "Cold volume read threads", "", int64(10)}
	opts[WriteThreads] = MountOption{"writeThreads", "Cold volume write threads", "", int64(10)}
	opts[MetaSendTimeout] = MountOption{"metaSendTimeout", "Meta send timeout", "", int64(600)}
	opts[BuffersTotalLimit] = MountOption{"buffersTotalLimit", "Send/Receive packets memory limit", "", int64(32768)} // default 4G
	opts[BufferChanSize] = MountOption{"buffersChanSize", "Send/Receive buffer chan size", "", int64(256)}            // default 256
	opts[MaxStreamerLimit] = MountOption{"maxStreamerLimit", "The maximum number of streamers", "", int64(0)}         // default 0
	opts[BcacheFilterFiles] = MountOption{"bcacheFilterFiles", "The block cache filter files suffix", "", "py;pyx;sh;yaml;conf;pt;pth;log;out"}
	opts[BcacheBatchCnt] = MountOption{"bcacheBatchCnt", "The block cache get meta count", "", int64(100000)}
	opts[BcacheCheckIntervalS] = MountOption{"bcacheCheckIntervalS", "The block cache check interval", "", int64(300)}
	opts[EnableAudit] = MountOption{"enableAudit", "enable client audit logging", "", true}
	opts[RequestTimeout] = MountOption{"requestTimeout", "The Request Expiration Time", "", int64(0)}
	opts[MinWriteAbleDataPartitionCnt] = MountOption{
		"minWriteAbleDataPartitionCnt",
		"Min writeable data partition count retained int dpSelector when update DataPartitionsView from master",
		"", int64(10),
	}

	opts[FileSystemName] = MountOption{"fileSystemName", "The explicit name of the filesystem", "", ""}
	opts[SnapshotReadVerSeq] = MountOption{"snapshotReadSeq", "Snapshot read seq", "", int64(0)} // default false
	opts[DisableMountSubtype] = MountOption{"disableMountSubtype", "Disable Mount Subtype", "", false}
	opts[StreamRetryTimeOut] = MountOption{"streamRetryTimeout", "max stream retry timeout, s", "", int64(0)}
	opts[BcacheOnlyForNotSSD] = MountOption{"enableBcacheOnlyForNotSSD", "Enable block cache only for not ssd", "", false}

	opts[AheadReadEnable] = MountOption{"aheadReadEnable", "enable ahead read", "", false}
	opts[AheadReadTotalMemGB] = MountOption{"aheadReadTotalMemGB", "ahead read total mem(GB)", "", int64(10)}
	opts[AheadReadBlockTimeOut] = MountOption{"aheadReadBlockTimeOut", "ahead read block expiration time", "", int64(3)}
	opts[AheadReadWindowCnt] = MountOption{"aheadReadWindowCnt", "ahead read window block count", "", int64(8)}

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
	BcacheOnlyForNotSSD          bool
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
	BufferChanSize               int64
	MaxStreamerLimit             int64
	EnableAudit                  bool
	RequestTimeout               int64
	MinWriteAbleDataPartitionCnt int
	FileSystemName               string
	// TrashInterval                       int64
	TrashDeleteExpiredDirGoroutineLimit int64
	TrashRebuildGoroutineLimit          int64

	VerReadSeq uint64
	// disable mount subtype
	DisableMountSubtype bool
	// stream retry timeout
	StreamRetryTimeout int

	// hybrid cloud
	VolStorageClass        uint32
	VolAllowedStorageClass []uint32
	VolCacheDpStorageClass uint32

	AheadReadEnable       bool
	AheadReadTotalMem     int64
	AheadReadBlockTimeOut int
	AheadReadWindowCnt    int
}
