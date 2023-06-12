// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package authnode

import (
	"fmt"
	syslog "log"
	"net/http"
	"net/http/httputil"
	"os"
	"strconv"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/cryptoutil"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

const (
	LRUCacheSize    = 3 << 30
	WriteBufferSize = 4 * util.MB
)

// AuthProxy wraps the stuff for http and https
type AuthProxy struct {
	// for http proxy
	reverseProxy *httputil.ReverseProxy
	// for https redirect
	client *http.Client
}

// Server represents the server in a cluster
type Server struct {
	id           uint64
	clusterName  string
	ip           string
	port         string
	walDir       string
	storeDir     string
	retainLogs   uint64
	tickInterval int
	electionTick int
	leaderInfo   *LeaderInfo
	config       *clusterConfig
	cluster      *Cluster
	rocksDBStore *raftstore.RocksDBStore
	raftStore    raftstore.RaftStore
	fsm          *KeystoreFsm
	partition    raftstore.Partition
	wg           sync.WaitGroup
	authProxy    *AuthProxy
	metaReady    bool
}

// configuration keys
const (
	ClusterName       = "clusterName"
	ID                = "id"
	IP                = "ip"
	Port              = "port"
	LogLevel          = "logLevel"
	WalDir            = "walDir"
	StoreDir          = "storeDir"
	GroupID           = 1
	ModuleName        = "authnode"
	CfgRetainLogs     = "retainLogs"
	DefaultRetainLogs = 20000
	cfgTickInterval   = "tickInterval"
	cfgElectionTick   = "electionTick"
	AuthSecretKey     = "authServiceKey"
	AuthRootKey       = "authRootKey"
	EnableHTTPS       = "enableHTTPS"
)

// NewServer creates a new server
func NewServer() *Server {
	return &Server{}
}

func (m *Server) checkConfig(cfg *config.Config) (err error) {
	m.clusterName = cfg.GetString(ClusterName)
	m.ip = cfg.GetString(IP)
	m.port = cfg.GetString(Port)
	m.walDir = cfg.GetString(WalDir)
	m.storeDir = cfg.GetString(StoreDir)

	peerAddrs := cfg.GetString(cfgPeers)
	if m.ip == "" || m.port == "" || m.walDir == "" || m.storeDir == "" || m.clusterName == "" || peerAddrs == "" {
		return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, "one of (ip,port,walDir,storeDir,clusterName) is null")
	}
	if m.id, err = strconv.ParseUint(cfg.GetString(ID), 10, 64); err != nil {
		return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
	}
	m.config.heartbeatPort = cfg.GetInt64(heartbeatPortKey)
	m.config.replicaPort = cfg.GetInt64(replicaPortKey)
	if m.config.heartbeatPort <= 1024 {
		m.config.heartbeatPort = raftstore.DefaultHeartbeatPort
	}
	if m.config.replicaPort <= 1024 {
		m.config.replicaPort = raftstore.DefaultReplicaPort
	}
	syslog.Printf("heartbeatPort[%v],replicaPort[%v]\n", m.config.heartbeatPort, m.config.replicaPort)

	if err = m.config.parsePeers(peerAddrs); err != nil {
		return
	}
	retainLogs := cfg.GetString(CfgRetainLogs)
	if retainLogs != "" {
		if m.retainLogs, err = strconv.ParseUint(retainLogs, 10, 64); err != nil {
			return fmt.Errorf("%v,err:%v", proto.ErrInvalidCfg, err.Error())
		}
	}
	if m.retainLogs <= 0 {
		m.retainLogs = DefaultRetainLogs
	}
	syslog.Println("retainLogs=", m.retainLogs)

	m.tickInterval = int(cfg.GetFloat(cfgTickInterval))
	m.electionTick = int(cfg.GetFloat(cfgElectionTick))
	if m.tickInterval <= 300 {
		m.tickInterval = 500
	}
	if m.electionTick <= 3 {
		m.electionTick = 5
	}

	return
}

func (m *Server) initFsm() {
	m.fsm = newKeystoreFsm(m.rocksDBStore, m.retainLogs, m.raftStore.RaftServer())
	m.fsm.registerLeaderChangeHandler(m.handleLeaderChange)
	m.fsm.registerPeerChangeHandler(m.handlePeerChange)

	m.fsm.id = m.id

	// register the handlers for the interfaces defined in the Raft library
	m.fsm.registerApplySnapshotHandler(m.handleApplySnapshot)
	m.fsm.restore()
}

func (m *Server) createRaftServer(cfg *config.Config) (err error) {
	raftCfg := &raftstore.Config{
		NodeID:            m.id,
		RaftPath:          m.walDir,
		NumOfLogsToRetain: m.retainLogs,
		HeartbeatPort:     int(m.config.heartbeatPort),
		ReplicaPort:       int(m.config.replicaPort),
		TickInterval:      m.tickInterval,
		ElectionTick:      m.electionTick,
	}
	if m.raftStore, err = raftstore.NewRaftStore(raftCfg, cfg); err != nil {
		return errors.Trace(err, "NewRaftStore failed! id[%v] walPath[%v]", m.id, m.walDir)
	}
	m.initFsm()
	partitionCfg := &raftstore.PartitionConfig{
		ID:      GroupID,
		Peers:   m.config.peers,
		Applied: m.fsm.applied,
		SM:      m.fsm,
	}
	if m.partition, err = m.raftStore.CreatePartition(partitionCfg); err != nil {
		return errors.Trace(err, "CreatePartition failed")
	}
	return
}

// Start starts a server
func (m *Server) Start(cfg *config.Config) (err error) {
	m.config = newClusterConfig()
	m.leaderInfo = &LeaderInfo{}
	if err = m.checkConfig(cfg); err != nil {
		log.LogError(errors.Stack(err))
		return
	}
	if m.rocksDBStore, err = raftstore.NewRocksDBStore(m.storeDir, LRUCacheSize, WriteBufferSize); err != nil {
		log.LogErrorf("Start: init RocksDB fail: err(%v)", err)
		return
	}

	if err = m.createRaftServer(cfg); err != nil {
		log.LogError(errors.Stack(err))
		return
	}
	m.initCluster()
	m.cluster.partition = m.partition

	AuthSecretKey := cfg.GetString(AuthSecretKey)
	if m.cluster.AuthSecretKey, err = cryptoutil.Base64Decode(AuthSecretKey); err != nil {
		return fmt.Errorf("action[Start] failed %v,err: auth service Key invalid=%s", proto.ErrInvalidCfg, AuthSecretKey)
	}

	AuthRootKey := cfg.GetString(AuthRootKey)
	if m.cluster.AuthRootKey, err = cryptoutil.Base64Decode(AuthRootKey); err != nil {
		return fmt.Errorf("action[Start] failed %v,err: auth root Key invalid=%s", proto.ErrInvalidCfg, AuthRootKey)
	}

	if cfg.GetBool(EnableHTTPS) == true {
		m.cluster.PKIKey.EnableHTTPS = true
		if m.cluster.PKIKey.AuthRootPublicKey, err = os.ReadFile("/app/server.crt"); err != nil {
			return fmt.Errorf("action[Start] failed,err[%v]", err)
		}
		if m.cluster.PKIKey.AuthRootPrivateKey, err = os.ReadFile("/app/server.key"); err != nil {
			return fmt.Errorf("action[Start] failed,err[%v]", err)
		}
		// TODO: verify cert
	} else {
		m.cluster.PKIKey.EnableHTTPS = false
	}
	m.authProxy = m.newAuthProxy()

	m.cluster.scheduleTask()
	m.startHTTPService()
	m.wg.Add(1)
	return nil
}

// Shutdown closes the server
func (m *Server) Shutdown() {
	m.wg.Done()
}

// Sync waits for the execution termination of the server
func (m *Server) Sync() {
	m.wg.Wait()
}

func (m *Server) initCluster() {
	m.cluster = newCluster(m.clusterName, m.leaderInfo, m.fsm, m.partition, m.config)
	m.cluster.retainLogs = m.retainLogs
}
