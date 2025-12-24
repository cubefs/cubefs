package flashgroupmanager

import (
	"fmt"
	syslog "log"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	cfsProto "github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
)

const (
	commaSplit = ","
	colonSplit = ":"
)

const (
	defaultFlashNodeHandleReadTimeout   = 1000
	defaultFlashNodeReadDataNodeTimeout = 3000
	defaultHttpReversePoolSize          = 1024
	defaultRetainLogs                   = 20000
	defaultFlashHotKeyMissCount         = 5
	defaultFlashReadFlowLimit           = 2147483648
	defaultFlashWriteFlowLimit          = 2147483648
	defaultFlashKeyFlowLimit            = 0
	defaultRemoteClientFlowLimit        = 0
)

const (
	cfgFlashNodeHandleReadTimeout   = "flashNodeHandleReadTimeout"
	cfgFlashNodeReadDataNodeTimeout = "flashNodeReadDataNodeTimeout"
	cfgRemoteCacheTTL               = "remoteCacheTTL"
	cfgRemoteCacheReadTimeout       = "remoteCacheReadTimeout"
	cfgRemoteCacheMultiRead         = "remoteCacheMultiRead"
	cfgFlashNodeTimeoutCount        = "flashNodeTimeoutCount"
	cfgRemoteCacheSameZoneTimeout   = "remoteCacheSameZoneTimeout"
	cfgRemoteCacheSameRegionTimeout = "remoteCacheSameRegionTimeout"
	cfgFlashHotKeyMissCount         = "flashHotKeyMissCount"
	cfgFlashReadFlowLimit           = "flashReadFlowLimit"
	cfgFlashWriteFlowLimit          = "flashWriteFlowLimit"
	cfgFlashKeyFlowLimit            = "flashKeyFlowLimit"
	cfgRemoteClientFlowLimit        = "remoteClientFlowLimit"
)

var AddrDatabase = make(map[uint64]string)

type clusterConfig struct {
	cfsProto.RemoteCacheConfig
	httpProxyPoolSize uint64
	heartbeatPort     int64
	replicaPort       int64
	peerAddrs         []string
	peers             []raftstore.PeerAddress
}

func newClusterConfig() (cfg *clusterConfig) {
	cfg = new(clusterConfig)

	cfg.FlashNodeHandleReadTimeout = defaultFlashNodeHandleReadTimeout
	cfg.FlashNodeReadDataNodeTimeout = defaultFlashNodeReadDataNodeTimeout
	cfg.RemoteCacheTTL = cfsProto.DefaultRemoteCacheTTL
	cfg.RemoteCacheReadTimeout = cfsProto.DefaultRemoteCacheClientReadTimeout
	cfg.FlashNodeTimeoutCount = cfsProto.DefaultFlashNodeTimeoutCount
	cfg.RemoteCacheSameZoneTimeout = cfsProto.DefaultRemoteCacheSameZoneTimeout
	cfg.RemoteCacheSameRegionTimeout = cfsProto.DefaultRemoteCacheSameRegionTimeout
	cfg.FlashHotKeyMissCount = defaultFlashHotKeyMissCount
	cfg.FlashReadFlowLimit = defaultFlashReadFlowLimit
	cfg.FlashWriteFlowLimit = defaultFlashWriteFlowLimit
	cfg.FlashKeyFlowLimit = defaultFlashKeyFlowLimit
	cfg.RemoteClientFlowLimit = defaultRemoteClientFlowLimit

	return
}

func parsePeerAddr(peerAddr string) (id uint64, ip string, port uint64, err error) {
	peerStr := strings.Split(peerAddr, colonSplit)
	id, err = strconv.ParseUint(peerStr[0], 10, 64)
	if err != nil {
		return
	}
	port, err = strconv.ParseUint(peerStr[2], 10, 64)
	if err != nil {
		return
	}
	ip = peerStr[1]
	return
}

func (cfg *clusterConfig) parsePeers(peerStr string) error {
	peerArr := strings.Split(peerStr, commaSplit)
	cfg.peerAddrs = peerArr
	hp := cfg.heartbeatPort
	rp := cfg.replicaPort

	if hp < 0 || hp > 65535 {
		return fmt.Errorf("invalid heartbeatPort: %d", hp)
	}
	if rp < 0 || rp > 65535 {
		return fmt.Errorf("invalid replicaPort: %d", rp)
	}
	for _, peerAddr := range peerArr {
		id, ip, port, err := parsePeerAddr(peerAddr)
		if err != nil {
			return err
		}
		cfg.peers = append(cfg.peers, raftstore.PeerAddress{Peer: proto.Peer{ID: id}, Address: ip, HeartbeatPort: int(hp), ReplicaPort: int(rp)})
		address := fmt.Sprintf("%v:%v", ip, port)
		syslog.Println(address)
		AddrDatabase[id] = address
	}
	return nil
}
