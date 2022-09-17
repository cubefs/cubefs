package datanode

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"time"

	"github.com/chubaofs/chubaofs/util/log"
)

type PersistFlag int

// Persist方法会执行以下操作:
// 1. Sync所有打开的文件句柄
// 2. Sync Raft WAL以及HardState信息
// 3. 持久化Applied Index水位信息
// 4. 持久化DP的META信息, 主要用于持久化和Applied Index对应的LastTruncateID。
// 若status参数为nil，则会使用调用该方法时WALApplyStatus状态
func (dp *DataPartition) Persist(status *WALApplyStatus) (err error) {
	dp.persistSync <- struct{}{}
	defer func() {
		<-dp.persistSync
	}()

	if status == nil {
		status = dp.applyStatus.Snap()
	}

	dp.forceFlushAllFD()

	if dp.raftPartition != nil {
		if err = dp.raftPartition.FlushWAL(false); err != nil {
			return
		}
	}

	if err = dp.persistAppliedID(status); err != nil {
		return
	}

	if err = dp.persistMetadata(status); err != nil {
		return
	}

	return
}

// PersistMetaDataOnly仅持久化DP的META信息(不对LastTruncatedID信息进行变更)
func (dp *DataPartition) PersistMetaDataOnly() (err error) {
	dp.persistSync <- struct{}{}
	defer func() {
		<-dp.persistSync
	}()

	if err = dp.persistMetadata(nil); err != nil {
		return
	}
	return
}

func (dp *DataPartition) persistAppliedID(snap *WALApplyStatus) (err error) {

	var (
		originalApplyIndex uint64
		newAppliedIndex    = snap.Applied()
	)

	if newAppliedIndex == 0 || newAppliedIndex <= dp.persistedApplied {
		return
	}

	var originalFilename = path.Join(dp.Path(), ApplyIndexFile)
	if originalFileData, readErr := ioutil.ReadFile(originalFilename); readErr == nil {
		_, _ = fmt.Sscanf(string(originalFileData), "%d", &originalApplyIndex)
	}

	if newAppliedIndex <= originalApplyIndex {
		return
	}

	tmpFilename := path.Join(dp.Path(), TempApplyIndexFile)
	tmpFile, err := os.OpenFile(tmpFilename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFilename)
	}()
	if _, err = tmpFile.WriteString(fmt.Sprintf("%d", newAppliedIndex)); err != nil {
		return
	}
	if err = tmpFile.Sync(); err != nil {
		return
	}
	err = os.Rename(tmpFilename, path.Join(dp.Path(), ApplyIndexFile))
	log.LogInfof("dp(%v) persistAppliedID to (%v)", dp.partitionID, newAppliedIndex)
	dp.persistedApplied = newAppliedIndex
	return
}

// PersistMetadata persists the file metadata on the disk.
// 若snap参数为nil，则不会修改META文件中的LastTruncateID信息。
func (dp *DataPartition) persistMetadata(snap *WALApplyStatus) (err error) {

	originFileName := path.Join(dp.path, DataPartitionMetadataFileName)
	tempFileName := path.Join(dp.path, TempMetadataFileName)

	var metadata = new(DataPartitionMetadata)
	if originData, err := ioutil.ReadFile(originFileName); err == nil {
		_ = json.Unmarshal(originData, metadata)
	}
	sp := sortedPeers(dp.config.Peers)
	sort.Sort(sp)
	metadata.VolumeID = dp.config.VolName
	metadata.PartitionID = dp.config.PartitionID
	metadata.PartitionSize = dp.config.PartitionSize
	metadata.Peers = dp.config.Peers
	metadata.Hosts = dp.config.Hosts
	metadata.Learners = dp.config.Learners
	metadata.DataPartitionCreateType = dp.DataPartitionCreateType
	metadata.VolumeHAType = dp.config.VolHAType
	metadata.LastUpdateTime = dp.lastUpdateTime
	metadata.IsCatchUp = dp.isCatchUp
	if metadata.CreateTime == "" {
		metadata.CreateTime = time.Now().Format(TimeLayout)
	}

	if snap != nil && snap.LastTruncate() > metadata.LastTruncateID {
		metadata.LastTruncateID = snap.LastTruncate()
	} else if dp.persistedMetadata != nil {
		metadata.LastTruncateID = dp.persistedMetadata.LastTruncateID
	}

	if dp.persistedMetadata != nil && dp.persistedMetadata.Equals(metadata) {
		return
	}

	var newData []byte
	if newData, err = json.Marshal(metadata); err != nil {
		return
	}
	var tempFile *os.File
	if tempFile, err = os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	defer func() {
		_ = tempFile.Close()
		if err != nil {
			_ = os.Remove(tempFileName)
		}
	}()
	if _, err = tempFile.Write(newData); err != nil {
		return
	}
	if err = tempFile.Sync(); err != nil {
		return
	}
	if err = os.Rename(tempFileName, originFileName); err != nil {
		return
	}
	dp.persistedMetadata = metadata
	log.LogInfof("PersistMetadata DataPartition(%v) data(%v)", dp.partitionID, string(newData))
	return
}

func (dp *DataPartition) forceFlushAllFD() (cnt int) {
	return dp.extentStore.ForceFlushAllFD()
}
