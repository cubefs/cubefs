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

package master

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (c *Cluster) updateVolsResponseCache() (body []byte, err error) {

	var vol *Vol
	volsInfo := make([]*proto.VolInfo, 0)
	for _, name := range c.allVolNames() {
		if c.leaderHasChanged() {
			return nil, proto.ErrRaftLeaderHasChanged
		}
		if vol, err = c.getVol(name); err != nil {
			continue
		}
		stat := volStat(vol)
		volInfo := proto.NewVolInfo(vol.Name, vol.Owner, vol.createTime, vol.status(), stat.TotalSize, stat.RealUsedSize,
			vol.trashRemainingDays, vol.ChildFileMaxCount, vol.isSmart, vol.smartRules, vol.ForceROW, vol.compact(),
			vol.TrashCleanInterval, vol.enableToken, vol.enableWriteCache, vol.BatchDelInodeCnt, vol.DelInodeInterval,
			vol.CleanTrashDurationEachTime, vol.TrashCleanMaxCountEachTime, vol.EnableBitMapAllocator, vol.enableRemoveDupReq,
			vol.TruncateEKCountEveryTime, vol.DefaultStoreMode)
		volsInfo = append(volsInfo, volInfo)
	}
	reply := newSuccessHTTPReply(volsInfo)
	if body, err = json.Marshal(reply); err != nil {
		log.LogError(fmt.Sprintf("action[updateVolsResponseCache],err:%v", err.Error()))
		return nil, proto.ErrMarshalData
	}
	c.volsInfoCacheMutex.Lock()
	c.volsInfoRespCache = body
	c.volsInfoCacheMutex.Unlock()
	return
}

func (c *Cluster) getVolsResponseCache() []byte {
	c.volsInfoCacheMutex.RLock()
	defer c.volsInfoCacheMutex.RUnlock()
	return c.volsInfoRespCache
}

func (c *Cluster) clearVolsResponseCache() {
	c.volsInfoCacheMutex.Lock()
	defer c.volsInfoCacheMutex.Unlock()
	c.volsInfoRespCache = nil
}
