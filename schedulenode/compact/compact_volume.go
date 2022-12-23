package compact

import (
	"context"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/unit"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CmpVolInitST uint32 = iota
	CmpVolRunningST
	CmpVolClosingST
)

type CompactVolumeInfo struct {
	sync.RWMutex
	Name            string
	ClusterName     string
	State           uint32
	LastUpdate      int64
	RunningMPCnt    uint32
	runingMpIds     map[uint64]struct{}
	RunningInoCnt   uint32
	limitSizes      []uint32
	limitCnts       []uint16
	inodeCheckStep  int
	inodeConcurrent int
	minEkLen        int
	minInodeSize    uint64
	maxEkAvgSize    uint64
	metaClient      *meta.MetaWrapper
	dataClient      *data.ExtentClient
}

func NewCompVolume(name string, clusterName string, nodes []string, mcc *metaNodeControlConfig) (volume *CompactVolumeInfo, err error) {
	volume = &CompactVolumeInfo{Name: name, ClusterName: clusterName}
	volume.limitSizes = []uint32{8, 16, 32, 64}
	volume.limitCnts = []uint16{10, 5, 2, 2}
	volume.inodeCheckStep = mcc.inodeCheckStep
	volume.inodeConcurrent = mcc.inodeConcurrent
	volume.minEkLen = mcc.minEkLen
	volume.minInodeSize = mcc.minInodeSize
	volume.maxEkAvgSize = mcc.maxEkAvgSize
	volume.runingMpIds = make(map[uint64]struct{}, 0)
	var metaConfig = &meta.MetaConfig{
		Volume:        name,
		Masters:       nodes,
		Authenticate:  false,
		ValidateOwner: false,
	}

	var metaWrapper *meta.MetaWrapper
	if metaWrapper, err = meta.NewMetaWrapper(metaConfig); err != nil {
		return nil, err
	}

	var extentConfig = &data.ExtentConfig{
		Volume:              name,
		Masters:             nodes,
		FollowerRead:        true,
		TinySize:            unit.MB * 8,
		OnInsertExtentKey:   metaWrapper.InsertExtentKey,
		OnGetExtents:        metaWrapper.GetExtents,
		OnTruncate:          metaWrapper.Truncate,
		OnInodeMergeExtents: metaWrapper.InodeMergeExtents_ll,
	}
	var extentClient *data.ExtentClient
	if extentClient, err = data.NewExtentClient(extentConfig, nil); err != nil {
		metaWrapper.Close()
		return nil, err
	}

	volume.metaClient = metaWrapper
	volume.dataClient = extentClient
	volume.State = CmpVolInitST
	return volume, nil
}

func (vol *CompactVolumeInfo) ReleaseResource() {
	vol.Lock()
	defer vol.Unlock()
	if err := vol.metaClient.Close(); err != nil {
		log.LogErrorf("vol[%s-%s] close meta wrapper failed:%s", vol.ClusterName, vol.Name, err.Error())
	}
	if err := vol.dataClient.Close(context.Background()); err != nil {
		log.LogErrorf("vol[%s-%s] close data wrapper failed:%s", vol.ClusterName, vol.Name, err.Error())
	}
}

func (vol *CompactVolumeInfo) ReleaseResourceMeetCondition() bool {
	vol.Lock()
	defer vol.Unlock()
	curTime := time.Now().Unix()
	if !(vol.RunningMPCnt == 0 && vol.RunningInoCnt == 0 && curTime-vol.LastUpdate > VolLastUpdateIntervalTime) {
		return false
	}
	if err := vol.metaClient.Close(); err != nil {
		log.LogErrorf("vol[%s-%s] close meta wrapper failed:%s", vol.ClusterName, vol.Name, err.Error())
	}
	if err := vol.dataClient.Close(context.Background()); err != nil {
		log.LogErrorf("vol[%s-%s] close data wrapper failed:%s", vol.ClusterName, vol.Name, err.Error())
	}
	return true
}

func (vol *CompactVolumeInfo) isRunning() (flag bool) {
	vol.RLock()
	defer vol.RUnlock()
	if vol.State == CmpVolRunningST || vol.State == CmpVolInitST {
		flag = true
	}
	return
}

func (vol *CompactVolumeInfo) GetVolStatus() uint32 {
	return atomic.LoadUint32(&vol.State)
}

func (vol *CompactVolumeInfo) UpdateVolLastTime() {
	vol.Lock()
	defer vol.Unlock()
	vol.LastUpdate = time.Now().Unix()
}

func (vol *CompactVolumeInfo) UpdateState(state uint32) {
	vol.Lock()
	defer vol.Unlock()
	vol.State = state
}

func (vol *CompactVolumeInfo) UpdateStateToInit() {
	vol.Lock()
	defer vol.Unlock()
	if vol.State == CmpVolClosingST {
		vol.State = CmpVolInitST
	}
}

func (vol *CompactVolumeInfo) GetLimitSizes() []uint32 {
	vol.RLock()
	defer vol.RUnlock()
	return vol.limitSizes
}

func (vol *CompactVolumeInfo) GetLimitCnts() []uint16 {
	vol.RLock()
	defer vol.RUnlock()
	return vol.limitCnts
}

func (vol *CompactVolumeInfo) GetInodeCheckStep() int {
	vol.RLock()
	defer vol.RUnlock()
	return vol.inodeCheckStep
}

func (vol *CompactVolumeInfo) GetInodeConcurrentPerMP() int {
	vol.RLock()
	defer vol.RUnlock()
	return vol.inodeConcurrent
}

func (vol *CompactVolumeInfo) GetInodeFilterParams() (minEkLen int, minInodeSize uint64, maxEkAvgSize uint64) {
	vol.RLock()
	defer vol.RUnlock()
	return vol.minEkLen, vol.minInodeSize, vol.maxEkAvgSize
}

func (vol *CompactVolumeInfo) AddMPRunningCnt(mpId uint64) bool {
	vol.Lock()
	defer vol.Unlock()
	if vol.State == CmpVolRunningST || vol.State == CmpVolInitST {
		vol.RunningMPCnt += 1
		vol.runingMpIds[mpId] = struct{}{}
		return true
	}
	return false
}

func (vol *CompactVolumeInfo) DelMPRunningCnt(mpId uint64) {
	vol.Lock()
	defer vol.Unlock()
	if vol.RunningMPCnt == 0 {
		return
	}
	vol.RunningMPCnt -= 1
	delete(vol.runingMpIds, mpId)
	vol.LastUpdate = time.Now().Unix()
}

func (vol *CompactVolumeInfo) AddInodeRunningCnt() bool {
	vol.Lock()
	defer vol.Unlock()
	if vol.State == CmpVolRunningST || vol.State == CmpVolInitST {
		vol.RunningInoCnt += 1
		return true
	}
	return false
}

func (vol *CompactVolumeInfo) DelInodeRunningCnt() {
	vol.Lock()
	defer vol.Unlock()
	if vol.RunningInoCnt == 0 {
		return
	}
	vol.RunningInoCnt -= 1
}

func (vol *CompactVolumeInfo) CanBeRelease() bool {
	vol.RLock()
	defer vol.RUnlock()
	return vol.RunningMPCnt == 0 && vol.RunningInoCnt == 0
}

func (vol *CompactVolumeInfo) GetVolumeCompactView() *proto.VolumeCompactView {
	vol.RLock()
	defer vol.RUnlock()
	var mpIds = make([]uint64, 0, len(vol.runingMpIds))
	for mpId := range vol.runingMpIds {
		mpIds = append(mpIds, mpId)
	}
	sort.Slice(mpIds, func(i, j int) bool { return mpIds[i] < mpIds[j] })
	return &proto.VolumeCompactView{
		ClusterName:   vol.ClusterName,
		Name:          vol.Name,
		State:         vol.State,
		LastUpdate:    vol.LastUpdate,
		RunningMPCnt:  vol.RunningMPCnt,
		RunningMpIds:  mpIds,
		RunningInoCnt: vol.RunningInoCnt,
	}
}
