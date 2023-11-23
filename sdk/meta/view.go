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

package meta

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
)

const (
	MaxSendToMaster = 3
)

type VolumeView struct {
	Name              string
	Owner             string
	MetaPartitions    []*MetaPartition
	OSSSecure         *OSSSecure
	OSSBucketPolicy   proto.BucketAccessPolicy
	CreateTime        int64
	CrossRegionHAType proto.CrossRegionHAType
	ConnConfig        *proto.ConnConfig
}

type OSSSecure struct {
	AccessKey string
	SecretKey string
}

type VolStatInfo = proto.VolStatInfo

func (mw *MetaWrapper) fetchVolumeView() (vv *proto.VolView, err error) {
	if mw.ownerValidation {
		var authKey string
		if authKey, err = calculateAuthKey(mw.owner); err != nil {
			return
		}
		if mw.authenticate {
			var (
				tokenMessage string
				ts           int64
			)
			mw.accessToken.Type = proto.MsgMasterFetchVolViewReq
			if tokenMessage, ts, err = genMasterToken(mw.accessToken, mw.sessionKey); err != nil {
				log.LogWarnf("fetchVolumeView generate token failed: err(%v)", err)
				return nil, err
			}
			var decoder master.Decoder = func(raw []byte) ([]byte, error) {
				return mw.parseAndVerifyResp(raw, ts)
			}
			if vv, err = mw.mc.ClientAPI().GetVolumeWithAuthnode(mw.volname, authKey, tokenMessage, decoder); err != nil {
				return
			}
		} else {
			if vv, err = mw.mc.ClientAPI().GetVolume(mw.volname, authKey); err != nil {
				return
			}
		}
	} else {
		if vv, err = mw.mc.ClientAPI().GetVolumeWithoutAuthKey(mw.volname); err != nil {
			return
		}
	}
	return
}

func (mw *MetaWrapper) saveVolView() *proto.VolView {
	vv := &proto.VolView{
		MetaPartitions: make([]*proto.MetaPartitionView, 0, len(mw.partitions)),
	}
	mw.RLock()
	for _, mp := range mw.partitions {
		view := &proto.MetaPartitionView{
			PartitionID: mp.PartitionID,
			Start:       mp.Start,
			End:         mp.End,
			Members:     mp.Members,
			Learners:    mp.Learners,
			LeaderAddr:  mp.GetLeaderAddr(),
			Status:      mp.Status,
		}
		vv.MetaPartitions = append(vv.MetaPartitions, view)
	}
	mw.RUnlock()
	if mw.ossSecure != nil {
		vv.OSSSecure = &proto.OSSSecure{}
		vv.OSSSecure.AccessKey = mw.ossSecure.AccessKey
		vv.OSSSecure.SecretKey = mw.ossSecure.SecretKey
	}
	vv.OSSBucketPolicy = mw.ossBucketPolicy
	vv.CreateTime = mw.volCreateTime
	vv.CrossRegionHAType = mw.crossRegionHAType
	vv.ConnConfig = &proto.ConnConfig{
		IdleTimeoutSec:   mw.connConfig.IdleTimeoutSec,
		ConnectTimeoutNs: mw.connConfig.ConnectTimeoutNs,
		WriteTimeoutNs:   mw.connConfig.WriteTimeoutNs,
		ReadTimeoutNs:    mw.connConfig.ReadTimeoutNs,
	}
	return vv
}

// fetch and update cluster info if successful
func (mw *MetaWrapper) updateClusterInfo() (err error) {
	var (
		info    *proto.ClusterInfo
		localIp string
	)
	if info, err = mw.mc.AdminAPI().GetClusterInfo(); err != nil {
		log.LogWarnf("updateClusterInfo: get cluster info fail: err(%v)", err)
		return
	}
	log.LogInfof("updateClusterInfo: get cluster info: cluster(%v) localIP(%v)",
		info.Cluster, info.Ip)
	mw.cluster = info.Cluster
	if localIp, err = iputil.GetLocalIPByDial(mw.mc.Nodes(), iputil.GetLocalIPTimeout); err != nil {
		log.LogWarnf("UpdateClusterInfo: get local ip fail: err(%v)", err)
		return
	}
	mw.localIP = localIp
	return
}

func (mw *MetaWrapper) updateVolStatInfo() (err error) {
	var info *proto.VolStatInfo
	if info, err = mw.mc.ClientAPI().GetVolumeStat(mw.volname); err != nil {
		log.LogWarnf("updateVolStatInfo: get volume status fail: volume(%v) err(%v)", mw.volname, err)
		return
	}
	atomic.StoreUint64(&mw.totalSize, info.TotalSize)
	atomic.StoreUint64(&mw.usedSize, info.UsedSize)
	log.LogInfof("VolStatInfo: info(%v)", info)
	return
}

func (mw *MetaWrapper) isCredibleMetaPartitionView(mps []*proto.MetaPartitionView) (isCredible bool) {
	if len(mps) < len(mw.partitions) {
		log.LogWarnf("isCredibleMetaPartitionView: mps length:%v less than last update:%v", len(mps), len(mw.partitions))
		return false
	}

	newMaxID := mw.maxPartitionID
	defer func() {
		if isCredible {
			mw.maxPartitionID = newMaxID
		}
		if log.IsDebugEnabled() {
			log.LogDebugf("isCredibleMetaPartitionView: isCredible(%v) maxID(%v)", isCredible, mw.maxPartitionID)
		}
	}()

	if mw.maxPartitionID <= 0 {
		if !sortAndCheckOverlap(mps) {
			return false
		}
		newMaxID = mps[len(mps)-1].PartitionID
		return true
	}

	newAddPartition := make([]*proto.MetaPartitionView, 0)
	for _, mp := range mps {
		if mp.PartitionID >= newMaxID {
			newMaxID = mp.PartitionID
			newAddPartition = append(newAddPartition, mp)
		}
	}

	if len(newAddPartition) < 1 {
		return false
	}
	if len(newAddPartition) == 1 {
		return mw.validMaxPartition(newAddPartition[0])
	}
	return sortAndCheckOverlap(newAddPartition)
}

func (mw *MetaWrapper) validMaxPartition(mp *proto.MetaPartitionView) bool {
	mw.RLock()
	defer mw.RUnlock()

	maxMp, ok := mw.partitions[mw.maxPartitionID]
	if log.IsDebugEnabled() {
		log.LogDebugf("validMaxPartition: maxMp:%v mp:%v", maxMp, mp)
	}
	if ok {
		return maxMp.PartitionID == mp.PartitionID && maxMp.Start == mp.Start && maxMp.End == mp.End
	}
	return false
}

func sortAndCheckOverlap(mps []*proto.MetaPartitionView) bool {
	if log.IsDebugEnabled() {
		log.LogDebugf("sortAndCheckOverlap: mp.len=%v", len(mps))
	}
	if len(mps) < 1 {
		return false
	}
	sort.SliceStable(mps, func(i, j int) bool {
		return mps[i].Start < mps[j].Start
	})

	for i := 1; i < len(mps); i++ {
		if mps[i].Start != mps[i-1].End+1 {
			log.LogWarnf("sortAndCheckOverlap: mp ranges overlap, %v-%v", mps[i-1], mps[i])
			return false
		}
	}
	return true
}

func (mw *MetaWrapper) updateConfigByVolView(vv *proto.VolView) {
	if vv.OSSSecure != nil {
		ossSecure := &OSSSecure{
			AccessKey: vv.OSSSecure.AccessKey,
			SecretKey: vv.OSSSecure.SecretKey,
		}
		mw.ossSecure = ossSecure
	}
	mw.ossBucketPolicy = vv.OSSBucketPolicy
	mw.volCreateTime = vv.CreateTime
	mw.crossRegionHAType = vv.CrossRegionHAType

	mw.updateConnConfig(vv.ConnConfig)
	return
}

func (mw *MetaWrapper) updateRanges(mps []*proto.MetaPartitionView, needNewRange bool) {
	var convert = func(mp *proto.MetaPartitionView) *MetaPartition {
		return &MetaPartition{
			PartitionID: mp.PartitionID,
			Start:       mp.Start,
			End:         mp.End,
			Members:     mp.Members,
			Learners:    mp.Learners,
			Status:      mp.Status,
			LeaderAddr:  proto.NewAtomicString(mp.LeaderAddr),
		}
	}

	var (
		newPartitions     map[uint64]*MetaPartition
		newRanges         *btree.BTree
		rwPartitions      = make([]*MetaPartition, 0)
		unavailPartitions = make([]*MetaPartition, 0)
	)

	if needNewRange {
		log.LogInfof("updateRanges: clear meta partitions, volNotExistCount(%v)", mw.volNotExistCount)
		mw.volNotExistCount = 0
		newRanges = btree.New(32)
		newPartitions = make(map[uint64]*MetaPartition, 0)
	}

	for _, view := range mps {
		mp := convert(view)
		if mp.Status == proto.ReadWrite {
			rwPartitions = append(rwPartitions, mp)
		} else if mp.Status == proto.Unavailable {
			unavailPartitions = append(unavailPartitions, mp)
		}
		if needNewRange {
			newRanges.ReplaceOrInsert(mp)
			newPartitions[mp.PartitionID] = mp
		} else {
			mw.replaceOrInsertPartition(mp)
		}
	}

	mw.Lock()
	defer mw.Unlock()

	if len(newPartitions) > 0 {
		mw.ranges = newRanges
		mw.partitions = newPartitions
	}
	if len(rwPartitions) == 0 {
		log.LogInfof("updateRanges: no valid rwPartitions")
	}
	mw.rwPartitions = rwPartitions
	mw.unavailPartitions = unavailPartitions
}
func (mw *MetaWrapper) updateMetaPartitions() error {
	vv, err := mw.fetchVolumeView()
	if err != nil {
		if err == proto.ErrVolNotExists {
			mw.volNotExistCount++
		}
		log.LogWarnf("updateMetaPartitions: fetchVolumeView failed, err(%v) volNotExistCount(%v)", err, mw.volNotExistCount)
		return err
	}

	mw.updateConfigByVolView(vv)

	if !mw.isCredibleMetaPartitionView(vv.MetaPartitions) {
		err = fmt.Errorf("incredible mp view from master, %v", vv.MetaPartitions)
		return err
	}

	needNewRange := mw.volNotExistCount > VolNotExistClearViewThresholdMin
	mw.updateRanges(vv.MetaPartitions, needNewRange)

	log.LogInfof("updateMetaPartitions: len(rwPartitions)=%v", len(mw.rwPartitions))
	return nil
}

func (mw *MetaWrapper) updateMetaPartitionsWithNoCache() error {
	var views []*proto.MetaPartitionView
	start := time.Now()
	for {
		var err error
		if views, err = mw.mc.ClientAPI().GetMetaPartitions(mw.volname); err == nil {
			log.LogInfof("updateMetaPartitionsWithNoCache: vol(%v)", mw.volname)
			break
		}
		if err != nil && time.Since(start) > MasterNoCacheAPIRetryTimeout {
			log.LogWarnf("updateMetaPartitionsWithNoCache: err(%v) vol(%v) retry timeout(%v)", err, mw.volname, time.Since(start))
			return err
		}
		log.LogWarnf("updateMetaPartitionsWithNoCache: err(%v) vol(%v) retry next round", err, mw.volname)
		time.Sleep(1 * time.Second)
	}

	if !mw.isCredibleMetaPartitionView(views) {
		err := fmt.Errorf("incredible mp view from master, %v", views)
		return err
	}

	mw.updateRanges(views, false)

	log.LogInfof("updateMetaPartitionsWithNoCache: len(rwPartition)=%v", len(mw.rwPartitions))
	return nil
}

func (mw *MetaWrapper) forceUpdateMetaPartitions() error {
	// Only one forceUpdateMetaPartition is allowed in a specific period of time.
	if ok := mw.forceUpdateLimit.AllowN(time.Now(), MinForceUpdateMetaPartitionsInterval); !ok {
		return errors.New("Force update meta partitions throttled!")
	}

	return mw.updateMetaPartitionsWithNoCache()
}

// Should be protected by partMutex, otherwise the caller might not be signaled.
func (mw *MetaWrapper) triggerAndWaitForceUpdate() {
	mw.partMutex.Lock()
	mw.triggerForceUpdate()
	mw.partCond.Wait()
	mw.partMutex.Unlock()
}

func (mw *MetaWrapper) triggerForceUpdate() {
	select {
	case mw.forceUpdate <- struct{}{}:
	default:
	}
}

func (mw *MetaWrapper) refresh() {
	defer mw.wg.Done()
	for {
		err := mw.refreshWithRecover()
		if err == nil {
			break
		}
		log.LogErrorf("refreshMetaInfo: err(%v) try next update", err)
	}
}

func (mw *MetaWrapper) refreshWithRecover() (panicErr error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("refreshMetaInfo panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("refreshMetaInfo panic: err(%v)", r)
			handleUmpAlarm(mw.cluster, mw.volname, "refreshMetaInfo", msg)
			panicErr = errors.New(msg)
		}
	}()

	var err error

	t := time.NewTimer(RefreshMetaPartitionsInterval)
	defer t.Stop()

	for {
		select {
		case <-mw.closeCh:
			return
		case <-t.C:
			if err = mw.updateMetaPartitions(); err != nil {
				mw.onAsyncTaskError.OnError(err)
				log.LogErrorf("updateMetaPartition fail cause: %v", err)
			}
			if err = mw.updateVolStatInfo(); err != nil {
				mw.onAsyncTaskError.OnError(err)
				log.LogErrorf("updateVolStatInfo fail cause: %v", err)
			}
			t.Reset(RefreshMetaPartitionsInterval)
		case <-mw.forceUpdate:
			log.LogInfof("Start forceUpdateMetaPartitions")
			mw.partMutex.Lock()
			if err = mw.forceUpdateMetaPartitions(); err == nil {
				if err = mw.updateVolStatInfo(); err == nil {
					t.Reset(RefreshMetaPartitionsInterval)
				}
			}
			mw.partMutex.Unlock()
			mw.partCond.Broadcast()
			log.LogInfof("End forceUpdateMetaPartitions: err(%v)", err)
		}
	}
}

func calculateAuthKey(key string) (authKey string, err error) {
	h := md5.New()
	_, err = h.Write([]byte(key))
	if err != nil {
		log.LogErrorf("action[calculateAuthKey] calculate auth key[%v] failed,err[%v]", key, err)
		return
	}
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr)), nil
}

func genMasterToken(req proto.APIAccessReq, key string) (message string, ts int64, err error) {
	var (
		sessionKey []byte
		data       []byte
	)

	if sessionKey, err = cryptoutil.Base64Decode(key); err != nil {
		return
	}

	if req.Verifier, ts, err = cryptoutil.GenVerifier(sessionKey); err != nil {
		return
	}

	if data, err = json.Marshal(req); err != nil {
		return
	}
	message = base64.StdEncoding.EncodeToString(data)

	return
}

func (mw *MetaWrapper) updateTicket() error {
	ticket, err := mw.ac.API().GetTicket(mw.owner, mw.ticketMess.ClientKey, proto.MasterServiceID)
	if err != nil {
		return errors.Trace(err, "Update ticket from authnode failed!")
	}
	mw.accessToken.Ticket = ticket.Ticket
	mw.sessionKey = ticket.SessionKey
	return nil
}

func (mw *MetaWrapper) parseAndVerifyResp(body []byte, ts int64) (dataBody []byte, err error) {
	var resp proto.MasterAPIAccessResp
	if resp, err = mw.parseRespWithAuth(body); err != nil {
		log.LogWarnf("fetchVolumeView parse response failed: err(%v) body(%v)", err, string(body))
		return nil, err
	}
	if err = proto.VerifyAPIRespComm(&(resp.APIResp), mw.accessToken.Type, mw.owner, proto.MasterServiceID, ts); err != nil {
		log.LogWarnf("fetchVolumeView verify response: err(%v)", err)
		return nil, err
	}
	var viewBody = &struct {
		Code int32  `json:"code"`
		Msg  string `json:"msg"`
		Data json.RawMessage
	}{}
	if err = json.Unmarshal(resp.Data, viewBody); err != nil {
		log.LogWarnf("VolViewCache unmarshal: err(%v) body(%v)", err, viewBody)
		return nil, err
	}
	if viewBody.Code != 0 {
		return nil, fmt.Errorf("request error, code[%d], msg[%s]", viewBody.Code, viewBody.Msg)
	}
	return viewBody.Data, err
}

func (mw *MetaWrapper) parseRespWithAuth(body []byte) (resp proto.MasterAPIAccessResp, err error) {
	var (
		message    string
		sessionKey []byte
		plaintext  []byte
	)

	if err = json.Unmarshal(body, &message); err != nil {
		return
	}

	if sessionKey, err = cryptoutil.Base64Decode(mw.sessionKey); err != nil {
		return
	}

	if plaintext, err = cryptoutil.DecodeMessage(message, sessionKey); err != nil {
		return
	}

	if err = json.Unmarshal(plaintext, &resp); err != nil {
		return
	}

	return
}
