// Copyright 2018 The Containerfs Authors.
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

package metanode

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	_ "net/http/pprof"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/exporter"
	"github.com/tiglabs/containerfs/util/log"
)

const partitionPrefix = "partition_"

// MetaManager manage all metaPartition and make mapping between volName and
// metaPartition.
type MetaManager interface {
	Start() error
	Stop()
	//CreatePartition(id string, start, end uint64, peers []proto.Peer) error
	HandleMetaOperation(conn net.Conn, p *Packet) error
	GetPartition(id uint64) (MetaPartition, error)
}

type MetaManagerConfig struct {
	NodeID    uint64
	RootDir   string
	RaftStore raftstore.RaftStore
}

type metaManager struct {
	nodeId     uint64
	rootDir    string
	raftStore  raftstore.RaftStore
	connPool   *util.ConnectPool
	state      uint32
	mu         sync.RWMutex
	partitions map[uint64]MetaPartition // Key: metaRangeId, Val: metaPartition
}

func (m *metaManager) HandleMetaOperation(conn net.Conn, p *Packet) (err error) {
	metric := exporter.RegistTp(p.GetOpMsg())
	defer metric.CalcTpMS()

	switch p.Opcode {
	case proto.OpMetaCreateInode:
		err = m.opCreateInode(conn, p)
	case proto.OpMetaLinkInode:
		err = m.opMetaLinkInode(conn, p)
	case proto.OpMetaDeleteInode:
		err = m.opDeleteInode(conn, p)
	case proto.OpMetaInodeGet:
		err = m.opMetaInodeGet(conn, p)
	case proto.OpMetaEvictInode:
		err = m.opMetaEvictInode(conn, p)
	case proto.OpMetaSetattr:
		err = m.opSetattr(conn, p)
	case proto.OpMetaCreateDentry:
		err = m.opCreateDentry(conn, p)
	case proto.OpMetaDeleteDentry:
		err = m.opDeleteDentry(conn, p)
	case proto.OpMetaUpdateDentry:
		err = m.opUpdateDentry(conn, p)
	case proto.OpMetaReadDir:
		err = m.opReadDir(conn, p)
	case proto.OpMetaOpen:
		err = m.opOpen(conn, p)
	case proto.OpCreateMetaPartition:
		err = m.opCreateMetaPartition(conn, p)
	case proto.OpMetaNodeHeartbeat:
		err = m.opMasterHeartbeat(conn, p)
	case proto.OpMetaExtentsAdd:
		err = m.opMetaExtentsAdd(conn, p)
	case proto.OpMetaExtentsList:
		err = m.opMetaExtentsList(conn, p)
	case proto.OpMetaExtentsDel:
		err = m.opMetaExtentsDel(conn, p)
	case proto.OpMetaTruncate:
		err = m.opMetaExtentsTruncate(conn, p)
	case proto.OpMetaLookup:
		err = m.opMetaLookup(conn, p)
	case proto.OpDeleteMetaPartition:
		err = m.opDeleteMetaPartition(conn, p)
	case proto.OpUpdateMetaPartition:
		err = m.opUpdateMetaPartition(conn, p)
	case proto.OpLoadMetaPartition:
		err = m.opLoadMetaPartition(conn, p)
	case proto.OpOfflineMetaPartition:
		err = m.opOfflineMetaPartition(conn, p)
	case proto.OpMetaBatchInodeGet:
		err = m.opMetaBatchInodeGet(conn, p)
	case proto.OpPing:
	default:
		err = fmt.Errorf("unknown Opcode: %d", p.Opcode)
	}
	if err != nil {
		err = errors.Errorf("[%s]: %s", p.GetOpMsg(), err.Error())
	}
	return
}

func (m *metaManager) Start() (err error) {
	if atomic.CompareAndSwapUint32(&m.state, StateStandby, StateStart) {
		defer func() {
			var newState uint32
			if err != nil {
				newState = StateStandby
			} else {
				newState = StateRunning
			}
			atomic.StoreUint32(&m.state, newState)
		}()
		err = m.onStart()
	}
	return
}

func (m *metaManager) Stop() {
	if atomic.CompareAndSwapUint32(&m.state, StateRunning, StateShutdown) {
		defer atomic.StoreUint32(&m.state, StateStopped)
		m.onStop()
	}
}

func (m *metaManager) onStart() (err error) {
	m.connPool = util.NewConnectPool()
	err = m.loadPartitions()
	return
}

func (m *metaManager) onStop() {
	if m.partitions != nil {
		for _, partition := range m.partitions {
			partition.Stop()
		}
	}
	return
}

// LoadMetaPartition returns metaPartition with specified volName if the mapping
// exist or report an error.
func (m *metaManager) getPartition(id uint64) (mp MetaPartition, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mp, ok := m.partitions[id]
	if ok {
		return
	}
	err = errors.New(fmt.Sprintf("unknown meta partition: %d", id))
	return
}

// Load meta manager snapshot from data file and restore all  meta range
// into this meta range manager.
func (m *metaManager) loadPartitions() (err error) {
	// Check metaDir directory
	fileInfo, err := os.Stat(m.rootDir)
	if err != nil {
		os.MkdirAll(m.rootDir, 0755)
		err = nil
		return
	}
	if !fileInfo.IsDir() {
		err = errors.New("metaDir must be directory")
		return
	}
	// Scan data directory.
	fileInfoList, err := ioutil.ReadDir(m.rootDir)
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	for _, fileInfo := range fileInfoList {
		if fileInfo.IsDir() && strings.HasPrefix(fileInfo.Name(), partitionPrefix) {
			wg.Add(1)
			go func(fileName string) {
				var err error
				defer func() {
					if r := recover(); r != nil {
						log.LogErrorf("loadPartitions partition: %s, "+
							"error: %s, failed: %v", fileName, err, r)
						log.LogFlush()
						panic(r)
					}
				}()
				defer wg.Done()
				if len(fileName) < 10 {
					log.LogWarnf("ignore unknown partition dir: %s", fileName)
					return
				}
				var id uint64
				partitionId := fileName[len(partitionPrefix):]
				id, err = strconv.ParseUint(partitionId, 10, 64)
				if err != nil {
					log.LogWarnf("ignore path: %s,not partition", partitionId)
					return
				}
				partitionConfig := &MetaPartitionConfig{
					NodeId:    m.nodeId,
					RaftStore: m.raftStore,
					RootDir:   path.Join(m.rootDir, fileName),
					ConnPool:  m.connPool,
				}
				partitionConfig.AfterStop = func() {
					m.detachPartition(id)
				}
				partition := NewMetaPartition(partitionConfig)
				err = m.attachPartition(id, partition)
				if err != nil {
					log.LogErrorf("load partition id=%d failed: %s.",
						id, err.Error())
				}
			}(fileInfo.Name())
		}
	}
	wg.Wait()
	return
}

func (m *metaManager) attachPartition(id uint64, partition MetaPartition) (err error) {
	if err = partition.Start(); err != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partitions[id] = partition
	log.LogDebugf("[attachPartition] add: %v", m.partitions)
	return
}

func (m *metaManager) detachPartition(id uint64) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, has := m.partitions[id]; has {
		delete(m.partitions, id)
	} else {
		err = fmt.Errorf("unknown partition: %d", id)
	}
	return
}

func (m *metaManager) createPartition(id uint64, volName string, start,
	end uint64, peers []proto.Peer) (err error) {
	/* Check Partition */
	if _, err = m.getPartition(id); err == nil {
		err = errors.Errorf("create partition id=%d is exsited!", id)
		return
	}
	err = nil
	/* Create metaPartition and add metaManager */
	partId := fmt.Sprintf("%d", id)
	mpc := &MetaPartitionConfig{
		PartitionId: id,
		VolName:     volName,
		Start:       start,
		End:         end,
		Cursor:      start,
		Peers:       peers,
		RaftStore:   m.raftStore,
		NodeId:      m.nodeId,
		RootDir:     path.Join(m.rootDir, partitionPrefix+partId),
		ConnPool:    m.connPool,
	}
	mpc.AfterStop = func() {
		m.detachPartition(id)
	}
	partition := NewMetaPartition(mpc)
	if err = partition.StoreMeta(); err != nil {
		err = errors.Errorf("[createPartition]->%s", err.Error())
		return
	}
	if err = m.attachPartition(id, partition); err != nil {
		os.RemoveAll(mpc.RootDir)
		err = errors.Errorf("[createPartition]->%s", err.Error())
		return
	}
	return
}

func (m *metaManager) deletePartition(id uint64) (err error) {
	m.detachPartition(id)
	return
}

func (m *metaManager) Range(f func(i uint64, p MetaPartition) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.partitions {
		if !f(k, v) {
			return
		}
	}
}

func (m *metaManager) GetPartition(id uint64) (mp MetaPartition, err error) {
	mp, err = m.getPartition(id)
	return
}

// PartitionsMarshalJSON only marshal base information of every partition
func (m *metaManager) MarshalJSON() (data []byte, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return json.Marshal(m.partitions)
}

func NewMetaManager(conf MetaManagerConfig) MetaManager {
	return &metaManager{
		nodeId:     conf.NodeID,
		rootDir:    conf.RootDir,
		raftStore:  conf.RaftStore,
		partitions: make(map[uint64]MetaPartition),
	}
}
