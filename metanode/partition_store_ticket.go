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

package metanode

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

type storeMsg struct {
	command    uint32
	applyIndex uint64
	snap       Snapshot
}

func (mp *metaPartition) startSchedule(curIndex uint64) {
	timer := time.NewTimer(time.Hour * 24 * 365)
	timer.Stop()
	timerCursor := time.NewTimer(intervalToSyncCursor)
	scheduleState := common.StateStopped
	dumpFunc := func(msg *storeMsg) {
		log.LogWarnf("[beforMetaPartitionStore] partitionId=%d: nowAppID"+
			"=%d, applyID=%d", mp.config.PartitionId, curIndex,
			msg.applyIndex)
		if err := mp.store(msg); err == nil {
			// truncate raft log
			if mp.raftPartition != nil {
				mp.raftPartition.Truncate(curIndex)
				log.LogWarnf("[afterMetaPartitionStore] partitionId=%d: nowAppID"+
					"=%d, applyID=%d", mp.config.PartitionId, curIndex,
					msg.applyIndex)
				curIndex = msg.applyIndex
			} else {
				// maybe happen when start load dentry
				log.LogWarnf("[startSchedule] raftPartition is nil so skip" +
					" truncate raft log")
			}
			if msg.snap != nil {
				msg.snap.Close()
			}

		} else {
			// retry again
			mp.storeChan <- msg
			err = errors.NewErrorf("[startSchedule]: dump partition id=%d: %v", mp.config.PartitionId, err.Error())
			log.LogErrorf(err.Error())
			exporter.Warning(err.Error())
		}

		if _, ok := mp.IsLeader(); ok {
			timer.Reset(intervalToPersistData)
		}
		scheduleState = common.StateStopped
	}
	go func(stopC chan bool) {
		var msgs []*storeMsg
		readyChan := make(chan struct{}, 1)
		for {
			if len(msgs) > 0 {
				if scheduleState == common.StateStopped {
					scheduleState = common.StateRunning
					readyChan <- struct{}{}
				}
			}
			select {
			case <-stopC:
				timer.Stop()
				timerCursor.Stop()
				return

			case <-readyChan:
				var (
					maxIdx uint64
					maxMsg *storeMsg
				)
				for _, msg := range msgs {
					if curIndex >= msg.applyIndex {
						if msg.snap != nil {
							msg.snap.Close()
						}
						continue
					}
					if maxIdx < msg.applyIndex {
						if maxMsg != nil && maxMsg.snap != nil {
							maxMsg.snap.Close()
						}
						maxIdx = msg.applyIndex
						maxMsg = msg
					} else {
						if msg.snap != nil {
							msg.snap.Close()
						}
					}
				}
				if maxMsg != nil {
					go dumpFunc(maxMsg)
				}
				msgs = msgs[:0]
			case msg := <-mp.storeChan:
				switch msg.command {
				case startStoreTick:
					timer.Reset(intervalToPersistData)
				case stopStoreTick:
					timer.Stop()
				case opFSMStoreTick:
					msgs = append(msgs, msg)
				case resetStoreTick:
					if _, ok := mp.IsLeader(); ok {
						timer.Reset(intervalToPersistData)
					}
				}
			case <-timer.C:
				if mp.applyID <= curIndex {
					timer.Reset(intervalToPersistData)
					continue
				}
				if _, err := mp.submit(context.Background(), opFSMStoreTick, "", nil); err != nil {
					log.LogErrorf("[startSchedule] raft submit: %s", err.Error())
					if _, ok := mp.IsLeader(); ok {
						timer.Reset(intervalToPersistData)
					}
				}
			case <-timerCursor.C:
				if _, ok := mp.IsLeader(); !ok {
					timerCursor.Reset(intervalToSyncCursor)
					continue
				}
				cursorBuf := make([]byte, 8)
				binary.BigEndian.PutUint64(cursorBuf, mp.config.Cursor)
				if _, err := mp.submit(context.Background(), opFSMSyncCursor, "", cursorBuf); err != nil {
					log.LogErrorf("[startSchedule] raft submit: %s", err.Error())
				}
				timerCursor.Reset(intervalToSyncCursor)
			}
		}
	}(mp.stopC)
}

func (mp *metaPartition) startCleanTrashScheduler() {
	cleanTrashTicker := time.NewTicker(1 * time.Hour)
	go func(stopC chan bool) {
		for {
			select {
			case <-stopC:
				cleanTrashTicker.Stop()
				return
			case <-cleanTrashTicker.C:
				if _, ok := mp.IsLeader(); !ok {
					continue
				}

				if mp.trashExpiresFirstUpdateTime.IsZero() {
					log.LogDebugf("mp[%v] trashExpiresFirstUpdateTime no update", mp.config.PartitionId)
					continue
				}

				if time.Since(mp.trashExpiresFirstUpdateTime) < (intervalToUpdateAllVolsTrashDays + intervalToUpdateVolTrashExpires) {
					log.LogDebugf("mp[%v] since trashExpiresFirstUpdateTime less than %v",
						mp.config.PartitionId, intervalToUpdateAllVolsTrashDays + intervalToUpdateVolTrashExpires)
					continue
				}
				err := mp.CleanExpiredDeletedDentry()
				if err != nil {
					log.LogErrorf("[CleanExpiredDeletedDentry], vol: %v, error: %s", mp.config.VolName, err.Error())
				}
				err = mp.CleanExpiredDeletedINode()
				if err != nil {
					log.LogErrorf("[CleanExpiredDeletedINode], vol: %v, error: %s", mp.config.VolName, err.Error())
				}

			}
		}
	}(mp.stopC)
}

func (mp *metaPartition) startUpdateTrashDaysScheduler() {
	for {
		if mp.config.TrashRemainingDays > -1 {
			break
		}

		mp.config.TrashRemainingDays = mp.manager.getTrashDaysByVol(mp.config.VolName)
		if mp.config.TrashRemainingDays == -1 {
			log.LogWarnf("[startUpdateTrashDaysScheduler], Vol: %v, PartitionID: %v", mp.config.VolName, mp.config.PartitionId)
			time.Sleep(time.Second)
			continue
		}
		break
	}
	ticker := time.NewTicker(intervalToUpdateVolTrashExpires)
	go func(stopC chan bool) {
		for {
			select {
			case <-stopC:
				ticker.Stop()
				return
			case <-ticker.C:
				if mp.trashExpiresFirstUpdateTime.IsZero() {
					mp.trashExpiresFirstUpdateTime = time.Now()
				}
				mp.config.TrashRemainingDays = mp.manager.getTrashDaysByVol(mp.config.VolName)
				log.LogDebugf("Vol: %v, PartitionID: %v, trash-days: %v", mp.config.VolName, mp.config.PartitionId, mp.config.TrashRemainingDays)
			}
		}
	}(mp.stopC)
}

func (mp *metaPartition) stop() {
	if mp.stopC != nil {
		close(mp.stopC)
	}
}
