package proto

import (
	"github.com/chubaofs/chubaofs/util/auth"
	"github.com/chubaofs/chubaofs/util/config"
)

const (
	// Mandatory
	MountPoint   = "mountPoint"
	VolName      = "volName"
	Owner        = "owner"
	MasterAddr   = "masterAddr"
	LogDir       = "logDir"
	WarnLogDir   = "warnLogDir"
	Authenticate = "authenticate"
	// Optional
	LogLevel      = "logLevel"
	ProfPort      = "profPort"
	IcacheTimeout = "icacheTimeout"
	LookupValid   = "lookupValid"
	AttrValid     = "attrValid"
	ReadRate      = "readRate"
	WriteRate     = "writeRate"
	EnSyncWrite   = "enSyncWrite"
	AutoInvalData = "autoInvalData"
	Rdonly        = "rdonly"
	WriteCache    = "writecache"
	KeepCache     = "keepcache"
	FollowerRead  = "followerRead"
	CertFile      = "certFile"
	ClientKey     = "clientKey"
	TicketHost    = "ticketHost"
	EnableHTTPS   = "enableHTTPS"

	ListenPort = "listen"
)

type MountOptions struct {
	Config        *config.Config
	MountPoint    string
	Volname       string
	Owner         string
	Master        string
	Logpath       string
	Loglvl        string
	Profport      string
	IcacheTimeout int64
	LookupValid   int64
	AttrValid     int64
	ReadRate      int64
	WriteRate     int64
	EnSyncWrite   int64
	AutoInvalData int64
	UmpDatadir    string
	Rdonly        bool
	WriteCache    bool
	KeepCache     bool
	FollowerRead  bool
	Authenticate  bool
	TicketMess    auth.TicketMess
}
