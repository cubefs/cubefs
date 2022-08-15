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

const (
	PF_WAL PersistFlag = 1 << iota
	PF_EXTENT
	PF_APPLY
	PF_METADATA

	PF_ALL = PF_WAL | PF_EXTENT | PF_APPLY | PF_METADATA
)

func (dp *DataPartition) Persist(f PersistFlag) (err error) {
	dp.persistSync <- struct{}{}
	defer func() {
		<-dp.persistSync
	}()

	if f&PF_EXTENT == PF_EXTENT {
		dp.forceFlushAllFD()
	}

	if f&PF_WAL == PF_WAL {
		if err = dp.raftPartition.FlushWAL(false); err != nil {
			return
		}
	}

	if f&PF_APPLY == PF_APPLY {
		if err = dp.persistAppliedID(); err != nil {
			return
		}
	}

	if f&PF_METADATA == PF_METADATA {
		if err = dp.persistMetadata(); err != nil {
			return
		}
	}
	return
}

func (dp *DataPartition) persistAppliedID() (err error) {

	var (
		originalApplyIndex uint64
		newAppliedIndex    = dp.applyStatus.Applied()
	)

	if newAppliedIndex == 0 {
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
	return
}

// PersistMetadata persists the file metadata on the disk.
func (dp *DataPartition) persistMetadata() (err error) {

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
	if metadata.CreateTime == "" {
		metadata.CreateTime = time.Now().Format(TimeLayout)
	}
	if lastTruncate := dp.applyStatus.LastTruncate(); lastTruncate > metadata.LastTruncateID {
		metadata.LastTruncateID = lastTruncate
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
	log.LogInfof("PersistMetadata DataPartition(%v) data(%v)", dp.partitionID, string(newData))
	return
}

func (dp *DataPartition) forceFlushAllFD() (cnt int) {
	return dp.extentStore.ForceFlushAllFD()
}
