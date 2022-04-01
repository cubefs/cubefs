package compact

import (
	"fmt"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/log"
	"sync"
)

type ClusterInfo struct {
	sync.RWMutex
	mc        *master.MasterClient
	name      string
	volumeMap map[string]*CompactVolumeInfo
}

func NewClusterInfo(clusterName string, mc *master.MasterClient) *ClusterInfo {
	clusterInfo := new(ClusterInfo)
	clusterInfo.name = clusterName
	clusterInfo.mc = mc
	clusterInfo.volumeMap = make(map[string]*CompactVolumeInfo, 0)

	return clusterInfo
}

func (cluster *ClusterInfo) CheckAndAddCompactVol(volName string, mcc *metaNodeControlConfig) (err error) {
	cluster.Lock()
	defer cluster.Unlock()
	if vol, exist := cluster.volumeMap[volName]; exist {
		vol.UpdateStateToInit()
		return
	}
	var cmpVol *CompactVolumeInfo
	if cmpVol, err = NewCompVolume(volName, cluster.name, cluster.mc.Nodes(), mcc); err != nil {
		err = fmt.Errorf("new compact cluster(%s) volume(%s) info failed:%s", cluster.name, volName, err.Error())
		return
	}
	cluster.volumeMap[volName] = cmpVol
	return
}

func (cluster *ClusterInfo) GetCompactVolByName(name string) *CompactVolumeInfo {
	cluster.RLock()
	defer cluster.RUnlock()
	vol, ok := cluster.volumeMap[name]
	if !ok {
		return nil
	}
	return vol
}

func (cluster *ClusterInfo) GetVolumeCount() int {
	cluster.RLock()
	defer cluster.RUnlock()
	return len(cluster.volumeMap)
}
func (cluster *ClusterInfo) Nodes() (nodes []string) {
	cluster.RLock()
	defer cluster.RUnlock()
	return cluster.mc.Nodes()
}

func (cluster *ClusterInfo) AddNode(node string) {
	cluster.Lock()
	defer cluster.Unlock()
	cluster.mc.AddNode(node)
}

func (cluster *ClusterInfo) UpdateNodes(nodes []string) {
	cluster.Lock()
	defer cluster.Unlock()
	cluster.mc = master.NewMasterClient(nodes, false)
}

func (cluster *ClusterInfo) GetClusterName() string {
	return cluster.name
}

func (cluster *ClusterInfo) releaseUnusedCompVolume() {
	cluster.Lock()
	defer cluster.Unlock()
	for volName, compVolume := range cluster.volumeMap {
		if compVolume.ReleaseResourceMeetCondition() {
			log.LogDebugf("releaseUnusedCompVolume volName(%v)", volName)
			delete(cluster.volumeMap, volName)
		}
	}
}

func (cluster *ClusterInfo) UpdateVolState(volumeName string, state uint32) bool {
	cluster.RLock()
	defer cluster.RUnlock()
	var volInfo *CompactVolumeInfo
	var ok bool
	if volInfo, ok = cluster.volumeMap[volumeName]; !ok {
		return false
	}
	volInfo.UpdateState(state)
	return true
}
