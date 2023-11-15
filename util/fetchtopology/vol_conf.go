package fetchtopology

import "sync"

type VolumeConfig struct {
	sync.RWMutex
	trashDay                          int32
	childFileMaxCnt                   uint32
	trashCleanInterval                uint64
	batchDelInodeCnt                  uint32
	delInodeInterval                  uint32
	enableBitMapAllocator             bool
	cleanTrashItemMaxDurationEachTime int32
	cleanTrashItemMaxCountEachTime    int32
	enableRemoveDupReq                bool
	truncateEKCount                   int
}

func (conf *VolumeConfig) GetEnableBitMapFlag() bool {
	conf.RLock()
	defer conf.RUnlock()

	return conf.enableBitMapAllocator
}

func (conf *VolumeConfig) GetBatchDelInodeCount() uint32 {
	conf.RLock()
	defer conf.RUnlock()

	return conf.batchDelInodeCnt
}

func (conf *VolumeConfig) GetTrashDays() int32 {
	conf.RLock()
	defer conf.RUnlock()

	return conf.trashDay
}

func (conf *VolumeConfig) GetChildFileMaxCount() uint32 {
	conf.RLock()
	defer conf.RUnlock()

	return conf.childFileMaxCnt
}

func (conf *VolumeConfig) GetTrashCleanInterval() uint64 {
	conf.RLock()
	defer conf.RUnlock()

	return conf.trashCleanInterval
}

func (conf *VolumeConfig) GetEnableRemoveDupReqFlag() bool {
	conf.RLock()
	defer conf.RUnlock()

	return conf.enableRemoveDupReq
}

func (conf *VolumeConfig) GetDelInodeInterval() uint32 {
	conf.RLock()
	defer conf.RUnlock()

	return conf.delInodeInterval
}

func (conf *VolumeConfig) GetCleanTrashItemMaxCount() int32 {
	conf.RLock()
	defer conf.RUnlock()

	return conf.cleanTrashItemMaxCountEachTime
}

func (conf *VolumeConfig) GetCleanTrashItemMaxDuration() int32 {
	conf.RLock()
	defer conf.RUnlock()

	return conf.cleanTrashItemMaxDurationEachTime
}

func (conf *VolumeConfig) GetTruncateEKCount() int {
	conf.RLock()
	defer conf.RUnlock()

	return conf.truncateEKCount
}

func (conf *VolumeConfig) update(newConf *VolumeConfig) {
	conf.Lock()
	defer conf.Unlock()

	conf.enableBitMapAllocator = newConf.enableBitMapAllocator
	conf.batchDelInodeCnt = newConf.batchDelInodeCnt
	conf.trashDay = newConf.trashDay
	conf.childFileMaxCnt = newConf.childFileMaxCnt
	conf.trashCleanInterval = newConf.trashCleanInterval
	conf.delInodeInterval = newConf.delInodeInterval
	conf.cleanTrashItemMaxCountEachTime = newConf.cleanTrashItemMaxCountEachTime
	conf.cleanTrashItemMaxDurationEachTime = newConf.cleanTrashItemMaxDurationEachTime
	conf.enableRemoveDupReq = newConf.enableRemoveDupReq
	conf.truncateEKCount = newConf.truncateEKCount
}
