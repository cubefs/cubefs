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
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/caps"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/keystore"
	"github.com/cubefs/cubefs/util/log"
)

// PKIKey defines the pki keys
type PKIKey struct {
	EnableHTTPS        bool
	AuthRootPrivateKey []byte
	AuthRootPublicKey  []byte
}

// Cluster stores all the cluster-level information.
type Cluster struct {
	Name                string
	leaderInfo          *LeaderInfo
	cfg                 *clusterConfig
	retainLogs          uint64
	DisableAutoAllocate bool
	fsm                 *KeystoreFsm
	partition           raftstore.Partition
	AuthSecretKey       []byte
	AuthRootKey         []byte
	PKIKey              PKIKey
}

func newCluster(name string, leaderInfo *LeaderInfo, fsm *KeystoreFsm, partition raftstore.Partition, cfg *clusterConfig) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.leaderInfo = leaderInfo
	c.cfg = cfg
	c.fsm = fsm
	c.partition = partition
	c.fsm.keystore = make(map[string]*keystore.KeyInfo, 0)
	c.fsm.accessKeystore = make(map[string]*keystore.AccessKeyInfo, 0)
	return
}

func (c *Cluster) scheduleTask() {
	c.scheduleToCheckHeartbeat()
}

func (c *Cluster) authAddr() (addr string) {
	return c.leaderInfo.addr
}

func (c *Cluster) scheduleToCheckHeartbeat() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsRaftLeader() {
				c.checkLeaderAddr()
			}
			time.Sleep(time.Second * defaultIntervalToCheckHeartbeat)
		}
	}()
}

func (c *Cluster) checkLeaderAddr() {
	leaderID, _ := c.partition.LeaderTerm()
	c.leaderInfo.addr = AddrDatabase[leaderID]
}

// CreateNewKey create a new key to the keystore
func (c *Cluster) CreateNewKey(id string, keyInfo *keystore.KeyInfo) (res *keystore.KeyInfo, err error) {
	c.fsm.opKeyMutex.Lock()
	defer c.fsm.opKeyMutex.Unlock()
	accessKeyInfo := &keystore.AccessKeyInfo{
		ID: keyInfo.ID,
	}
	if _, err = c.fsm.GetKey(id); err == nil {
		err = proto.ErrDuplicateKey
		goto errHandler
	}
	keyInfo.Ts = time.Now().Unix()
	keyInfo.AuthKey = cryptoutil.GenSecretKey([]byte(c.AuthRootKey), keyInfo.Ts, id)
	//TODO check duplicate
	keyInfo.AccessKey = util.RandomString(16, util.Numeric|util.LowerLetter|util.UpperLetter)
	keyInfo.SecretKey = util.RandomString(32, util.Numeric|util.LowerLetter|util.UpperLetter)
	if err = c.syncAddKey(keyInfo); err != nil {
		goto errHandler
	}
	accessKeyInfo.AccessKey = keyInfo.AccessKey
	if err = c.syncAddAccessKey(accessKeyInfo); err != nil {
		goto errHandler
	}
	res = keyInfo
	c.fsm.PutKey(keyInfo)
	c.fsm.PutAKInfo(accessKeyInfo)
	return
errHandler:
	err = fmt.Errorf("action[CreateNewKey], clusterID[%v] ID:%v, err:%v ", c.Name, keyInfo, err.Error())
	log.LogError(errors.Stack(err))
	return
}

// DeleteKey delete a key from the keystore
func (c *Cluster) DeleteKey(id string) (res *keystore.KeyInfo, err error) {
	c.fsm.opKeyMutex.Lock()
	defer c.fsm.opKeyMutex.Unlock()
	akInfo := new(keystore.AccessKeyInfo)
	if res, err = c.fsm.GetKey(id); err != nil {
		err = proto.ErrKeyNotExists
		goto errHandler
	}
	if err = c.syncDeleteKey(res); err != nil {
		goto errHandler
	}
	akInfo.AccessKey = res.AccessKey
	akInfo.ID = res.ID
	if err = c.syncDeleteAccessKey(akInfo); err != nil {
		goto errHandler
	}
	c.fsm.DeleteKey(id)
	c.fsm.DeleteAKInfo(akInfo.AccessKey)
	return
errHandler:
	err = fmt.Errorf("action[DeleteKey], clusterID[%v] ID:%v, err:%v ", c.Name, id, err.Error())
	log.LogError(errors.Stack(err))
	return
}

// GetKey get a key from the keystore
func (c *Cluster) GetKey(id string) (res *keystore.KeyInfo, err error) {
	if res, err = c.fsm.GetKey(id); err != nil {
		err = proto.ErrKeyNotExists
		goto errHandler
	}
	return
errHandler:
	err = fmt.Errorf("action[GetKey], clusterID[%v] ID:%v, err:%v ", c.Name, id, err.Error())
	log.LogError(errors.Stack(err))
	return
}

// GetKey get a key from the AKstore
func (c *Cluster) GetAKInfo(accessKey string) (akInfo *keystore.AccessKeyInfo, err error) {
	if akInfo, err = c.fsm.GetAKInfo(accessKey); err != nil {
		err = proto.ErrAccessKeyNotExists
		goto errHandler
	}
	return
errHandler:
	err = fmt.Errorf("action[GetAKInfo], clusterID[%v] ID:%v, err:%v ", c.Name, accessKey, err.Error())
	log.LogError(errors.Stack(err))
	return
}

// AddCaps add caps to the key
func (c *Cluster) AddCaps(id string, keyInfo *keystore.KeyInfo) (res *keystore.KeyInfo, err error) {
	var (
		addCaps *caps.Caps
		curCaps *caps.Caps
		newCaps []byte
	)
	c.fsm.opKeyMutex.Lock()
	defer c.fsm.opKeyMutex.Unlock()
	if res, err = c.fsm.GetKey(id); err != nil {
		err = proto.ErrKeyNotExists
		goto errHandler
	}

	addCaps = &caps.Caps{}
	if err = addCaps.Init(keyInfo.Caps); err != nil {
		goto errHandler
	}
	curCaps = &caps.Caps{}
	if err = curCaps.Init(res.Caps); err != nil {
		goto errHandler
	}
	curCaps.Union(addCaps)
	if newCaps, err = json.Marshal(curCaps); err != nil {
		goto errHandler
	}
	res.Caps = newCaps
	if err = c.syncAddCaps(res); err != nil {
		goto errHandler
	}
	c.fsm.PutKey(res)
	return
errHandler:
	err = fmt.Errorf("action[AddCaps], clusterID[%v] ID:%v, err:%v ", c.Name, keyInfo, err.Error())
	log.LogError(errors.Stack(err))
	return
}

// DeleteCaps delete caps from the key
func (c *Cluster) DeleteCaps(id string, keyInfo *keystore.KeyInfo) (res *keystore.KeyInfo, err error) {
	var (
		delCaps *caps.Caps
		curCaps *caps.Caps
		newCaps []byte
	)
	c.fsm.opKeyMutex.Lock()
	defer c.fsm.opKeyMutex.Unlock()
	if res, err = c.fsm.GetKey(id); err != nil {
		err = proto.ErrKeyNotExists
		goto errHandler
	}

	delCaps = &caps.Caps{}
	if err = delCaps.Init(keyInfo.Caps); err != nil {
		return
	}
	curCaps = &caps.Caps{}
	if err = curCaps.Init(res.Caps); err != nil {
		return
	}

	curCaps.Delete(delCaps)

	if newCaps, err = json.Marshal(curCaps); err != nil {
		goto errHandler
	}
	res.Caps = newCaps
	if err = c.syncDeleteCaps(res); err != nil {
		goto errHandler
	}
	c.fsm.PutKey(res)
	return
errHandler:
	err = fmt.Errorf("action[DeleteCaps], clusterID[%v] ID:%v, err:%v ", c.Name, keyInfo, err.Error())
	log.LogError(errors.Stack(err))
	return
}
