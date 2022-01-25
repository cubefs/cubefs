// Copyright 2018 The Chubao Authors.
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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/util/keystore"
	"github.com/cubefs/cubefs/util/log"
	"github.com/tiglabs/raft/proto"
)

// RaftCmd defines the Raft commands.
type RaftCmd struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

// Marshal converts the RaftCmd to a byte array.
func (m *RaftCmd) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts the byte array to a RaftCmd.
func (m *RaftCmd) Unmarshal(data []byte) (err error) {
	return json.Unmarshal(data, m)
}

func (m *RaftCmd) setOpType() {
	keyArr := strings.Split(m.K, keySeparator)
	if len(keyArr) < 2 {
		log.LogWarnf("action[setOpType] invalid length[%v]", keyArr)
		return
	}
	switch keyArr[1] {
	case keyAcronym:
		m.Op = opSyncAddKey
	case akAcronym:
		m.Op = opSyncAddKey
	default:
		log.LogWarnf("action[setOpType] unknown opCode[%v]", keyArr[1])
	}
}

func (c *Cluster) submit(metadata *RaftCmd) (err error) {
	cmd, err := metadata.Marshal()
	if err != nil {
		return errors.New(err.Error())
	}
	if _, err = c.partition.Submit(cmd); err != nil {
		msg := fmt.Sprintf("action[keystore_submit] err:%v", err.Error())
		return errors.New(msg)
	}
	return
}

func (c *Cluster) syncAddKey(keyInfo *keystore.KeyInfo) (err error) {
	return c.syncPutKeyInfo(opSyncAddKey, keyInfo)
}

func (c *Cluster) syncAddAccessKey(akInfo *keystore.AccessKeyInfo) (err error) {
	return c.syncPutAccessKeyInfo(opSyncAddKey, akInfo)
}

func (c *Cluster) syncAddCaps(keyInfo *keystore.KeyInfo) (err error) {
	return c.syncPutKeyInfo(opSyncAddCaps, keyInfo)
}

func (c *Cluster) syncDeleteKey(keyInfo *keystore.KeyInfo) (err error) {
	return c.syncPutKeyInfo(opSyncDeleteKey, keyInfo)
}

func (c *Cluster) syncDeleteAccessKey(akInfo *keystore.AccessKeyInfo) (err error) {
	return c.syncPutAccessKeyInfo(opSyncDeleteKey, akInfo)
}

func (c *Cluster) syncDeleteCaps(keyInfo *keystore.KeyInfo) (err error) {
	return c.syncPutKeyInfo(opSyncDeleteCaps, keyInfo)
}

func (c *Cluster) syncPutKeyInfo(opType uint32, keyInfo *keystore.KeyInfo) (err error) {
	keydata := new(RaftCmd)
	keydata.Op = opType
	keydata.K = ksPrefix + keyInfo.ID + idSeparator + strconv.FormatUint(c.fsm.id, 10)
	vv := *keyInfo
	if keydata.V, err = json.Marshal(vv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(keydata)
}

func (c *Cluster) syncPutAccessKeyInfo(opType uint32, accessKeyInfo *keystore.AccessKeyInfo) (err error) {
	keydata := new(RaftCmd)
	keydata.Op = opType
	keydata.K = akPrefix + accessKeyInfo.AccessKey + idSeparator + strconv.FormatUint(c.fsm.id, 10)
	vv := *accessKeyInfo
	if keydata.V, err = json.Marshal(vv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(keydata)
}

func (c *Cluster) loadKeystore() (err error) {
	ks := make(map[string]*keystore.KeyInfo, 0)
	log.LogInfof("action[loadKeystore]")
	result, err := c.fsm.store.SeekForPrefix([]byte(ksPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadKeystore],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		k := &keystore.KeyInfo{}
		if err = json.Unmarshal(value, k); err != nil {
			err = fmt.Errorf("action[loadKeystore],value:%v,unmarshal err:%v", string(value), err)
			return err
		}
		if _, ok := ks[k.ID]; !ok {
			ks[k.ID] = k
		}
		log.LogInfof("action[loadKeystore],key[%v]", k)
	}
	c.fsm.ksMutex.Lock()
	defer c.fsm.ksMutex.Unlock()
	c.fsm.keystore = ks

	return
}

func (c *Cluster) clearKeystore() {
	c.fsm.ksMutex.Lock()
	defer c.fsm.ksMutex.Unlock()
	c.fsm.keystore = nil
}

func (c *Cluster) loadAKstore() (err error) {
	aks := make(map[string]*keystore.AccessKeyInfo, 0)
	log.LogInfof("action[loadAccessKeystore]")
	result, err := c.fsm.store.SeekForPrefix([]byte(akPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadAccessKeystore], err: %v", err.Error())
		return err
	}
	for _, value := range result {
		ak := &keystore.AccessKeyInfo{}
		if err = json.Unmarshal(value, ak); err != nil {
			err = fmt.Errorf("action[loadAccessKeystore], value: %v, unmarshal err: %v", string(value), err)
			return err
		}
		if _, ok := aks[ak.AccessKey]; !ok {
			aks[ak.AccessKey] = ak
		}
		log.LogInfof("action[loadAccessKeystore], access key[%v]", ak)
	}
	c.fsm.aksMutex.Lock()
	defer c.fsm.aksMutex.Unlock()
	c.fsm.accessKeystore = aks

	return
}

func (c *Cluster) clearAKstore() {
	c.fsm.aksMutex.Lock()
	defer c.fsm.aksMutex.Unlock()
	c.fsm.accessKeystore = nil
}

func (c *Cluster) addRaftNode(nodeID uint64, addr string) (err error) {
	peer := proto.Peer{ID: nodeID}
	_, err = c.partition.ChangeMember(proto.ConfAddNode, peer, []byte(addr))
	if err != nil {
		return errors.New("action[addRaftNode] error: " + err.Error())
	}
	return nil
}

func (c *Cluster) removeRaftNode(nodeID uint64, addr string) (err error) {
	peer := proto.Peer{ID: nodeID}
	_, err = c.partition.ChangeMember(proto.ConfRemoveNode, peer, []byte(addr))
	if err != nil {
		return errors.New("action[removeRaftNode] error: " + err.Error())
	}
	return nil
}
