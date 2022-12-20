package compact

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"io"
	"math"
	"sync"
	"time"
)

type CmpInodeTask struct {
	sync.RWMutex
	Inode     *proto.InodeInfo
	State     proto.InodeCompactState
	Name      string
	cmpMpTask *CmpMpTask

	extents        []proto.ExtentKey
	lastCmpEkIndex int
	startIndex     int
	endIndex       int
	buff           []byte
	newEks         []*proto.ExtentKey
	firstCmp       bool
	limitSize      uint32
	limitCnt       uint16

	vol            *CompactVolumeInfo
	statisticsInfo StatisticsInfo
	lastUpdateTime int64
}

type StatisticsInfo struct {
	CmpCnt      uint64
	CmpInodeCnt uint64
	CmpEkCnt    uint64
	NewEkCnt    uint64
	CmpSize     uint64
	CmpErrCode  int
	CmpErrCnt   uint64
	CmpErrMsg   string
}

func NewCmpInodeTask(cmpMpTask *CmpMpTask, inode *proto.CmpInodeInfo, vol *CompactVolumeInfo) *CmpInodeTask {
	task := &CmpInodeTask{cmpMpTask: cmpMpTask, vol: vol, Inode: inode.Inode, extents: inode.Extents}
	task.Name = fmt.Sprintf("%s_%d_%d", vol.Name, cmpMpTask.id, inode.Inode.Inode)
	task.firstCmp = true
	task.statisticsInfo = StatisticsInfo{CmpInodeCnt: 1}
	return task
}

func (inodeTask *CmpInodeTask) InitTask() {
	inodeTask.Lock()
	defer inodeTask.Unlock()
	inodeTask.lastCmpEkIndex = 0
	inodeTask.State = proto.InodeCmpOpenFile
}

func (inodeTask *CmpInodeTask) OpenFile() (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("inode[%v] %v:%v", inodeTask.Name, InodeOpenFailed, err.Error())
			log.LogErrorf("%v", err)
			inodeTask.DealActionErr(InodeOpenFailedCode, err)
			return
		} else {
			log.LogDebugf("[inode compact] OpenFile success ino(%v)", inodeTask.Name)
		}
		inodeTask.State = proto.InodeCmpCalcCmpEKS
	}()
	if err = inodeTask.vol.dataClient.OpenStream(inodeTask.Inode.Inode, false, false); err != nil {
		return
	}
	if err = inodeTask.vol.dataClient.RefreshExtentsCache(context.Background(), inodeTask.Inode.Inode); err != nil {
		inodeTask.CmpTaskCloseStream()
		return
	}
	return
}

func (inodeTask *CmpInodeTask) CalcCmpExtents() (err error) {
	if inodeTask.firstCmp {
		inodeTask.stepCalCompactEksArea()
		log.LogDebugf("CalcCmpExtents ino(%v) firstCmp(%v) startIndex(%v) endIndex(%v) limitSize(%v) limitCnt(%v) lastCmpEkIndex(%v)",
			inodeTask.Name, inodeTask.firstCmp, inodeTask.startIndex, inodeTask.endIndex, inodeTask.limitSize, inodeTask.limitCnt, inodeTask.lastCmpEkIndex)
		inodeTask.firstCmp = false
	} else {
		inodeTask.calCompactEksArea(inodeTask.limitSize*1024*1024, inodeTask.limitCnt)
		log.LogDebugf("CalcCmpExtents ino(%v) firstCmp(%v) startIndex(%v) endIndex(%v) limitSize(%v) limitCnt(%v) lastCmpEkIndex(%v)",
			inodeTask.Name, inodeTask.firstCmp, inodeTask.startIndex, inodeTask.endIndex, inodeTask.limitSize, inodeTask.limitCnt, inodeTask.lastCmpEkIndex)
	}
	if inodeTask.startIndex >= inodeTask.endIndex {
		//already cmp
		inodeTask.State = proto.InodeCmpStopped
	} else {
		inodeTask.State = proto.InodeCmpWaitRead
	}
	return
}

func (inodeTask *CmpInodeTask) ReadAndWriteEkData() (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("inode[%v] %v:%v", inodeTask.Name, InodeReadAndWriteFailed, err.Error())
			log.LogErrorf("%v", err)
			inodeTask.CmpTaskCloseStream()
			inodeTask.DealActionErr(InodeReadFailedCode, err)
			return
		}
		inodeTask.State = proto.InodeCmpMetaMerge
	}()
	offset := inodeTask.extents[inodeTask.startIndex].FileOffset
	totalSize := inodeTask.extents[inodeTask.endIndex].FileOffset + uint64(inodeTask.extents[inodeTask.endIndex].Size) - offset
	cmpEksCnt := inodeTask.endIndex - inodeTask.startIndex + 1
	var (
		firstWrite = true
		dp         *data.DataPartition
		newEk      *proto.ExtentKey
		readN      int
		writeN     int
		readOffset = offset
		readSize   uint64
		buff       = make([]byte, unit.BlockSize)
		write      int
		writeTotal int
	)
	ctx := context.Background()
	for {
		readSize = uint64(len(buff))
		if totalSize-(readOffset-offset) <= 0 {
			break
		}
		if totalSize-(readOffset-offset) < readSize {
			readSize = totalSize - (readOffset - offset)
		}
		readN, _, err = inodeTask.vol.dataClient.Read(ctx, inodeTask.Inode.Inode, buff, readOffset, int(readSize))
		if err != nil && err != io.EOF {
			return
		}
		if readN > 0 {
			if firstWrite {
				dp, writeN, newEk, err = inodeTask.vol.dataClient.SyncWrite(ctx, inodeTask.Inode.Inode, readOffset, buff[:readN])
				if err != nil {
					return
				}
				inodeTask.newEks = append(inodeTask.newEks, newEk)
				firstWrite = false
			} else {
				writeN, err = inodeTask.vol.dataClient.SyncWriteToSpecificExtent(ctx, dp, inodeTask.Inode.Inode, readOffset, write, buff[:readN], int(newEk.ExtentId))
				if err != nil {
					log.LogWarnf("ReadAndWriteEkData syncWriteToSpecificExtent ino(%v), err(%v)", inodeTask.Name, err)
					dp, writeN, newEk, err = inodeTask.vol.dataClient.SyncWrite(ctx, inodeTask.Inode.Inode, readOffset, buff[:readN])
					write = 0
					if err != nil {
						return
					}
					inodeTask.newEks = append(inodeTask.newEks, newEk)
					if len(inodeTask.newEks) >= cmpEksCnt {
						err = fmt.Errorf("ReadAndWriteEkData new create extent ino(%v) newEks length(%v) is greater than or equal to oldEks length(%v)", inodeTask.Name, len(inodeTask.newEks), cmpEksCnt)
						// delete new create extent todo
						return
					}
				} else {
					newEk.Size += uint32(writeN)
				}
			}
			readOffset += uint64(readN)
			write += writeN
			writeTotal += writeN
			log.LogDebugf("ReadAndWriteEkData write data ino(%v), totalSize(%v), readN(%v), readOffset(%v), write(%v), dpId(%v), extId(%v)", inodeTask.Name, totalSize, readN, readOffset, write, dp.PartitionID, newEk.ExtentId)
		}
		if err == io.EOF {
			err = nil
			break
		}
	}
	if totalSize != uint64(writeTotal) {
		err = fmt.Errorf("ReadAndWriteEkData compare equal ino(%v) totalSize(%v) but write size(%v)", inodeTask.Name, totalSize, writeTotal)
		return
	}
	return
}

func (inodeTask *CmpInodeTask) MetaMergeExtents() (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("inode[%v] %v:%v", inodeTask.Name, InodeMergeFailed, err.Error())
			log.LogErrorf("%v", err)
			inodeTask.CmpTaskCloseStream()
			inodeTask.DealActionErr(InodeMergeFailedCode, err)
			return
		}
		inodeTask.State = proto.InodeCmpCalcCmpEKS
	}()
	copyNewEks := make([]proto.ExtentKey, len(inodeTask.newEks))
	for i, ek := range inodeTask.newEks {
		copyNewEks[i] = *ek
	}
	inodeTask.newEks = nil
	err = inodeTask.vol.metaClient.InodeMergeExtents_ll(context.Background(), inodeTask.Inode.Inode, inodeTask.extents[inodeTask.startIndex:inodeTask.endIndex+1], copyNewEks)
	if err != nil {
		return
	}

	inodeTask.statisticsInfo.CmpCnt += 1
	inodeTask.statisticsInfo.CmpEkCnt += uint64(inodeTask.endIndex + 1 - inodeTask.startIndex)
	inodeTask.statisticsInfo.NewEkCnt += uint64(len(copyNewEks))
	var cmpSize uint32
	for _, newEk := range copyNewEks {
		cmpSize += newEk.Size
	}
	inodeTask.statisticsInfo.CmpSize += uint64(cmpSize)
	return
}

func (inodeTask *CmpInodeTask) CmpTaskCloseStream() {
	if err := inodeTask.vol.dataClient.CloseStream(context.Background(), inodeTask.Inode.Inode); err != nil {
		log.LogErrorf("CmpTaskCloseStream ino(%v) err(%v)", inodeTask.Name, err)
	}
}

func (inodeTask *CmpInodeTask) stepCalCompactEksArea() {
	limitSizes := inodeTask.vol.GetLimitSizes()
	limitCnts := inodeTask.vol.GetLimitCnts()
	for i, limitSize := range limitSizes {
		limitSizeByte := limitSize * 1024 * 1024
		limitCnt := limitCnts[i]
		if inodeTask.Inode.Size <= InodeLimitSize {
			limitSizeByte = math.MaxUint32
			limitCnt = math.MaxUint16
		}
		inodeTask.calCompactEksArea(limitSizeByte, limitCnt)
		log.LogDebugf("inodeTask.calCompactEksArea inode(%v) startIndex(%v) endIndex(%v) limitSize(%v) limitCnt(%v) lastCmpEkIndex(%v)",
			inodeTask.Name, inodeTask.startIndex, inodeTask.endIndex, limitSizeByte/1024/1024, limitCnt, inodeTask.lastCmpEkIndex)
		if inodeTask.startIndex < inodeTask.endIndex {
			inodeTask.limitSize = limitSize
			inodeTask.limitCnt = limitCnt
			break
		}
		inodeTask.lastCmpEkIndex = 0
	}
}

func (inodeTask *CmpInodeTask) calCompactEksArea(limitSize uint32, limitCnt uint16) {
	start := 0
	end := 0
	cmpSize := uint32(0)
	cmpCnt := uint16(0)
	var lastEk *proto.ExtentKey
	eks := inodeTask.extents
	for inodeTask.lastCmpEkIndex < len(inodeTask.extents)-1 {
		//init cmp counter
		cmpCnt = 0
		cmpSize = uint32(0)

		//find first ek
		for i := inodeTask.lastCmpEkIndex; i < len(eks)-1; i++ {
			inodeTask.lastCmpEkIndex = i
			if eks[i].Size < limitSize {
				start = i
				lastEk = &eks[start]
				cmpSize += lastEk.Size
				cmpCnt += 1
				break
			}
		}
		inodeTask.lastCmpEkIndex += 1

		for i := inodeTask.lastCmpEkIndex; i < len(eks)-1 && cmpCnt < limitCnt && cmpSize < limitSize; i++ {
			inodeTask.lastCmpEkIndex = i
			if eks[i].Size >= limitSize {
				break
			}

			if lastEk != nil && eks[i].FileOffset != lastEk.FileOffset+uint64(lastEk.Size) {
				break
			}
			end = i
			lastEk = &eks[i]
			cmpSize += lastEk.Size
			cmpCnt += 1
			inodeTask.lastCmpEkIndex += 1 // maybe counters meet conditions, lastEnd need jump to next
		}

		if inodeTask.lastCmpEkIndex >= len(eks) {
			inodeTask.lastCmpEkIndex = len(eks) - 1
		}

		//lastEnd ek is not continuous or overlap, end need back stack
		for i := end; i > start; i-- {
			if eks[i].FileOffset+uint64(eks[i].Size) == eks[inodeTask.lastCmpEkIndex].FileOffset {
				break
			}

			if eks[i].FileOffset+uint64(eks[i].Size) > eks[inodeTask.lastCmpEkIndex].FileOffset {
				//overlap back end
				end -= 1
				continue
			}

			if (i+1 == inodeTask.lastCmpEkIndex) && eks[i].FileOffset+uint64(eks[i].Size) < eks[inodeTask.lastCmpEkIndex].FileOffset {
				// not continuous, back end
				end -= 1
				break
			}
		}

		//eks not continuous at this point, get next point
		if end <= start {
			continue
		}

		//found  return the result
		inodeTask.startIndex = start
		inodeTask.endIndex = end
		return
	}
	inodeTask.startIndex = 0
	inodeTask.endIndex = 0
	return
}

func (inodeTask *CmpInodeTask) RunOnce(isIgnoreCompactSwitch bool) (finished bool, err error) {
	defer func() {
		inodeTask.vol.DelInodeRunningCnt()
	}()
	if !inodeTask.vol.AddInodeRunningCnt() {
		return
	}
	defer func() {
		inodeTask.vol.UpdateVolLastTime()
		inodeTask.cmpMpTask.UpdateStatisticsInfo(inodeTask.statisticsInfo)
	}()
	for err == nil {
		if !isIgnoreCompactSwitch && !inodeTask.vol.isRunning() {
			log.LogDebugf("inode compact stop because vol(%v) be stopped, ino(%v) inode.State(%v)", inodeTask.vol.Name, inodeTask.Name, inodeTask.State)
			inodeTask.State = proto.InodeCmpStopped
		}
		log.LogDebugf("inode runonce ino(%v) inode.State(%v)", inodeTask.Name, inodeTask.State)
		switch inodeTask.State {
		case proto.InodeCmpInit:
			inodeTask.InitTask()
		case proto.InodeCmpOpenFile:
			err = inodeTask.OpenFile()
		case proto.InodeCmpCalcCmpEKS:
			err = inodeTask.CalcCmpExtents()
		case proto.InodeCmpWaitRead:
			err = inodeTask.ReadAndWriteEkData()
		case proto.InodeCmpMetaMerge:
			err = inodeTask.MetaMergeExtents()
		case proto.InodeCmpStopped:
			inodeTask.CmpTaskCloseStream()
			finished = true
			return
		default:
			err = nil
			return
		}
		inodeTask.UpdateTime()
	}
	return
}

func (inodeTask *CmpInodeTask) DealActionErr(errCode int, err error) {
	if err == nil {
		return
	}
	inodeTask.statisticsInfo.CmpErrCode = errCode
	inodeTask.statisticsInfo.CmpErrCnt += 1
	inodeTask.statisticsInfo.CmpErrMsg = err.Error()
	return
}

func (inodeTask *CmpInodeTask) UpdateTime() {
	if time.Now().Unix()-inodeTask.lastUpdateTime > 10*60 {
		if updateErr := mysql.UpdateTaskUpdateTime(inodeTask.cmpMpTask.task.TaskId); updateErr != nil {
			log.LogErrorf("UpdateTaskUpdateTime to mysql failed, tasks(%v), err(%v)", inodeTask.cmpMpTask.task, updateErr)
		}
		inodeTask.lastUpdateTime = time.Now().Unix()
	}
}
