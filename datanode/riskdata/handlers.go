package riskdata

import (
	"context"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/multirate"
	"github.com/cubefs/cubefs/util/unit"
)

func (p *Fixer) getHandlers(remotes []string, hat proto.CrossRegionHAType, fragment *Fragment) []fixHandler {
	var handlers []fixHandler
	if !proto.IsTinyExtent(fragment.ExtentID) {
		handlers = append(handlers, p.getFastHandlers(remotes, hat, fragment)...)
	}
	handlers = append(handlers, p.getStdHandlers(remotes, hat, fragment)...)
	return handlers
}

func (p *Fixer) getFastHandlers(remotes []string, hat proto.CrossRegionHAType, fragment *Fragment) []fixHandler {
	var (
		getLocalFGPOnce = new(sync.Once)
		localFGP        storage.Fingerprint
		localFGPError   error
	)
	var getLocalFGP = func() (storage.Fingerprint, error) {
		getLocalFGPOnce.Do(func() {
			localFGP, localFGPError = p.computeLocalFingerprint(fragment)
			if log.IsDebugEnabled() {
				log.LogDebugf("Fixer[%v] compute local fingerprint %v: fgp=%v, error=%v", p.partitionID, fragment, localFGP.String(), localFGPError)
			}
		})
		return localFGP, localFGPError
	}
	return []fixHandler{
		{
			name: "FastTrust",
			handle: func() FixResult {
				return p.fixByFastTrustPolicy(remotes, hat, getLocalFGP, fragment)
			},
		},
	}
}

func (p *Fixer) getStdHandlers(remotes []string, hat proto.CrossRegionHAType, fragment *Fragment) []fixHandler {
	var (
		getLocalCRCOnce = new(sync.Once)
		localCRC        uint32
		localCRCError   error
	)
	var getLocalCRC = func() (uint32, error) {
		getLocalCRCOnce.Do(func() {
			localCRC, localCRCError = p.computeLocalCRC(fragment)
			if log.IsDebugEnabled() {
				log.LogDebugf("Fixer[%v] compute local CRC %v: crc=%v, error=%v", p.partitionID, fragment, localCRC, localCRCError)
			}
		})
		return localCRC, localCRCError
	}
	return []fixHandler{
		{
			name: "StdTrust",
			handle: func() FixResult {
				return p.fixByStdTrustPolicy(remotes, hat, getLocalCRC, fragment)
			},
		},
		{
			name: "StdQuorum",
			handle: func() FixResult {
				return p.fixByStdQuorumPolicy(remotes, hat, getLocalCRC, fragment)
			},
		},
	}
}

func (p *Fixer) fixByFastTrustPolicy(hosts []string, hat proto.CrossRegionHAType, getLocalFGP func() (storage.Fingerprint, error), fragment *Fragment) FixResult {
	var (
		extentID = fragment.ExtentID
		offset   = fragment.Offset
		size     = fragment.Size

		rejects    int
		failures   int
		unsupports int
	)

	if proto.IsTinyExtent(extentID) {
		return Failed
	}

	var err error
	var localFGP storage.Fingerprint
	if localFGP, err = getLocalFGP(); err != nil {
		return Retry
	}
	if localFGP.Empty() {
		return Success
	}

	var start time.Time
	for _, host := range hosts {
		var remoteFgp storage.Fingerprint
		var rejected bool
		var err error
		start = time.Now()
		remoteFgp, rejected, err = p.fetchRemoveFingerprint(host, extentID, offset, size, false)
		if log.IsDebugEnabled() {
			log.LogDebugf("Fixer[%v] fetched remote FGP %v: host=%v, fgp=%v, rejected=%v, error=%v, elapsed=%v", p.partitionID, fragment, host, remoteFgp.String(), rejected, err, time.Now().Sub(start))
		}
		if err != nil {
			if strings.Contains(err.Error(), repl.ErrorUnknownOp.Error()) {
				unsupports++
			}
			failures++
			continue
		}
		if rejected {
			rejects++
			continue
		}
		if remoteFgp.Empty() {
			if hat == proto.CrossRegionHATypeQuorum {
				continue
			}
			return Success
		}
		if localFGP.Equals(remoteFgp) {
			return Success
		}

		var firstConflict = localFGP.FirstConflict(remoteFgp)
		var issueStartBlkNo = int(offset / unit.BlockSize)
		var conflictStartBlkNo = issueStartBlkNo + firstConflict
		var conflictOffset = uint64(math.Max(float64(uint64(conflictStartBlkNo)*unit.BlockSize), float64(offset)))
		var conflictSize = (offset + size) - conflictOffset
		var extentStartOffset = int64(conflictOffset)

		if log.IsDebugEnabled() {
			log.LogDebugf("Fixer[%v] computed conflict %v: offset=%v, size=%v", p.partitionID, fragment, conflictOffset, conflictSize)
		}

		var writeAtExtent WriterAtFunc = func(b []byte, off int64) (n int, err error) {
			var extentWriteOffset = extentStartOffset + off
			var extentWriteSize = int64(len(b))
			var crc = crc32.ChecksumIEEE(b)
			err = p.limiter(context.Background(), proto.OpExtentRepairWrite_, uint32(extentWriteSize), multirate.FlowDisk)
			if err != nil {
				return
			}
			err = p.storage.Write(context.Background(), extentID, extentWriteOffset, extentWriteSize, b, crc, storage.RandomWriteType, false)
			n = int(extentWriteSize)
			return
		}
		start = time.Now()
		var n int64
		n, rejected, err = p.fetchRemoteDataTo(host, extentID, conflictOffset, conflictSize, false, writeAtExtent)
		if log.IsDebugEnabled() {
			log.LogDebugf("Fixer[%v] fetched remote data %v: host=%v, bytes=%v, rejected=%v, error=%v, elapsed=%v", p.partitionID, fragment, host, n, rejected, err, time.Now().Sub(start))
		}
		if err != nil {
			failures++
			continue
		}
		if rejected {
			rejects++
			continue
		}
		if n < int64(size) && hat == proto.CrossRegionHATypeQuorum {
			continue
		}
		return Success
	}
	if failures > 0 {
		// 存在失败远端，稍后重试
		if failures == unsupports {
			// 所有远端副本均为老版本不支持该操作
			return Failed
		}
		return Retry
	}
	if rejects == 0 {
		// 所有远端均汇报无数据, 无需修复
		return Success
	}
	// 当前修复策略无法认定数据是否安全及进行修复, 由下一个策略继续处理
	log.LogErrorf("Fixer: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) can not determine correct data by FastTrustPolicy",
		p.partitionID, extentID, offset, size)
	return Failed
}

// 使用可信副本策略对指定数据片段进行修复
func (p *Fixer) fixByStdTrustPolicy(hosts []string, hat proto.CrossRegionHAType, _ func() (uint32, error), fragment *Fragment) FixResult {
	var (
		err       error
		extentID  = fragment.ExtentID
		offset    = fragment.Offset
		size      = fragment.Size
		rejects   int
		failures  []error
		tempFiles = make([]*os.File, 0, len(hosts))
	)

	defer func() {
		for _, tempFile := range tempFiles {
			_ = tempFile.Close()
			_ = os.Remove(tempFile.Name())
		}
	}()

	var exists bool
	exists, err = p.checkLocalExists(fragment)
	if err != nil {
		return Retry
	}
	if !exists {
		return Success
	}

	var start time.Time
	for _, host := range hosts {
		var extentStartOffset = int64(offset)
		var writeAtExtent WriterAtFunc = func(b []byte, off int64) (n int, err error) {
			var extentWriteOffset = extentStartOffset + off
			var extentWriteSize = int64(len(b))
			var crc = crc32.ChecksumIEEE(b)
			err = p.limiter(context.Background(), proto.OpExtentRepairWrite_, uint32(extentWriteSize), multirate.FlowDisk)
			if err != nil {
				return
			}
			err = p.storage.Write(context.Background(), extentID, extentWriteOffset, extentWriteSize, b, crc, storage.RandomWriteType, false)
			if log.IsDebugEnabled() {
				log.LogDebugf("Fixer[%v] write data to local storage: extent=%v, offset=%v, size=%v", p.partitionID, extentID, extentWriteOffset, extentWriteSize)
			}
			n = int(extentWriteSize)
			return
		}
		start = time.Now()
		var n, rejected, fetchedErr = p.fetchRemoteDataTo(host, extentID, offset, size, false, writeAtExtent)
		if log.IsDebugEnabled() {
			log.LogDebugf("Fixer[%v] fetched remote data %v: host=%v, bytes=%v, rejected=%v, error=%v, elapsed=%v", p.partitionID, fragment, host, n, rejected, err, time.Now().Sub(start))
		}
		if fetchedErr != nil {
			failures = append(failures, fmt.Errorf("fetch extent data (partition=%v, extent=%v, offset=%v, size=%v) from %v failed: %v",
				p.partitionID, extentID, offset, size, host, fetchedErr))
			continue
		}
		if rejected {
			rejects++
			continue
		}
		if n < int64(size) && hat == proto.CrossRegionHATypeQuorum {
			continue
		}
		return Success
	}
	if len(failures) > 0 {
		// 存在失败远端，稍后重试
		return Retry
	}
	if rejects == 0 {
		// 所有远端均汇报无数据, 无需修复
		return Success
	}
	// 当前修复策略无法认定数据是否安全及进行修复, 由下一个策略继续处理
	log.LogErrorf("Fixer: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) can not determine correct data by TrustPolicy",
		p.partitionID, extentID, offset, size)
	return Failed
}

// 使用超半数版本策略对制定数据片段进行修复
func (p *Fixer) fixByStdQuorumPolicy(hosts []string, _ proto.CrossRegionHAType, getLocalCRC func() (uint32, error), fragment *Fragment) FixResult {
	var (
		err              error
		extentID         = fragment.ExtentID
		offset           = fragment.Offset
		size             = fragment.Size
		failures         int
		versions         = make(map[uint32][]*os.File) // crc -> temp files
		tempFiles        = make([]*os.File, 0, len(hosts))
		registerTempFile = func(f *os.File) {
			tempFiles = append(tempFiles, f)
		}
	)

	defer func() {
		for _, tempFile := range tempFiles {
			_ = tempFile.Close()
			_ = os.Remove(tempFile.Name())
		}
	}()

	var localCRC uint32
	if localCRC, err = getLocalCRC(); err != nil {
		return Retry
	}
	if localCRC == 0 {
		return Success
	}

	// 从远端收集数据
	for _, host := range hosts {
		var tempFile *os.File
		if tempFile, err = p.createRepairTmpFile(host, extentID, offset, size); err != nil {
			return Retry
		}
		registerTempFile(tempFile)
		var hash = crc32.NewIEEE()
		var writeAtTempFile WriterAtFunc = func(b []byte, off int64) (n int, err error) {
			_, _ = hash.Write(b)
			n, err = tempFile.WriteAt(b, off)
			return
		}
		var _, _, fetchedErr = p.fetchRemoteDataTo(host, extentID, offset, size, true, writeAtTempFile)
		if fetchedErr != nil {
			failures++
			continue
		}
		var crc = hash.Sum32()
		versions[crc] = append(versions[crc], tempFile)
	}
	var quorum = (len(hosts)+1)/2 + 1
	for crc, files := range versions {
		if len(files) >= quorum {
			// 找到了超半数版本, 使用该版本数据
			if crc != 0 && crc != localCRC {
				// 仅在目标数据非空洞且有效长度超过本地数据的情况下进行修复
				if err = p.applyTempFileToExtent(files[0], extentID, offset, size); err != nil {
					// 覆盖本地数据时出错，延迟修复
					log.LogErrorf("Fixer: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) fix CRC(%v -> %v) failed: %v", p.partitionID, extentID, offset, size, localCRC, crc, err)
					return Retry
				}
			}
			log.LogWarnf("Fixer: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) skip fix cause quorum version same as local or empty.", p.partitionID, extentID, offset, size)
			return Success
		}
	}
	log.LogErrorf("Fixer: Partition(%v)_Extent(%v)_Offset(%v)_Size(%v) can not determine correct data by quorum",
		p.partitionID, extentID, offset, size)
	return Failed
}
