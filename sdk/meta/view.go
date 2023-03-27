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

func (mw *MetaWrapper) convertVolumeView(vv *proto.VolView) (view *VolumeView) {
	var convert = func(volView *proto.VolView) *VolumeView {
		result := &VolumeView{
			Name:              volView.Name,
			Owner:             volView.Owner,
			MetaPartitions:    make([]*MetaPartition, len(volView.MetaPartitions)),
			OSSSecure:         &OSSSecure{},
			OSSBucketPolicy:   volView.OSSBucketPolicy,
			CreateTime:        volView.CreateTime,
			CrossRegionHAType: volView.CrossRegionHAType,
			ConnConfig:        volView.ConnConfig,
		}
		if volView.OSSSecure != nil {
			result.OSSSecure.AccessKey = volView.OSSSecure.AccessKey
			result.OSSSecure.SecretKey = volView.OSSSecure.SecretKey
		}
		for i, mp := range volView.MetaPartitions {
			result.MetaPartitions[i] = &MetaPartition{
				PartitionID: mp.PartitionID,
				Start:       mp.Start,
				End:         mp.End,
				Members:     mp.Members,
				Learners:    mp.Learners,
				Status:      mp.Status,
			}
			result.MetaPartitions[i].SetLeaderAddr(mp.LeaderAddr)
		}
		return result
	}
	view = convert(vv)
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
	if localIp, err = iputil.GetLocalIPByDialWithMaster(mw.mc.Nodes(), iputil.GetLocalIPTimeout); err != nil {
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

func (mw *MetaWrapper) updateMetaPartitions() error {
	var view *VolumeView
	vv, err := mw.fetchVolumeView()
	if err != nil {
		if err == proto.ErrVolNotExists {
			mw.volNotExistCount++
		}
		log.LogWarnf("updateMetaPartitions error: %v, volNotExistCount(%v)", err.Error(), mw.volNotExistCount)
		return err
	}
	view = mw.convertVolumeView(vv)

	rwPartitions := make([]*MetaPartition, 0)
	unavailPartitions := make([]*MetaPartition, 0)
	for _, mp := range view.MetaPartitions {
		if mw.volNotExistCount <= VolNotExistClearViewThresholdMin {
			mw.replaceOrInsertPartition(mp)
			log.LogInfof("updateMetaPartition: mp(%v)", mp)
		}
		if mp.Status == proto.ReadWrite {
			rwPartitions = append(rwPartitions, mp)
		} else if mp.Status == proto.Unavailable {
			unavailPartitions = append(unavailPartitions, mp)
		}
	}

	if mw.volNotExistCount > VolNotExistClearViewThresholdMin && len(view.MetaPartitions) > 0 {
		log.LogInfof("clear and updateMetaPartition: volNotExistCount(%v)", mw.volNotExistCount)
		newPartitions := make(map[uint64]*MetaPartition)
		newRanges := btree.New(32)
		for _, mp := range view.MetaPartitions {
			newPartitions[mp.PartitionID] = mp
			newRanges.ReplaceOrInsert(mp)
			log.LogInfof("updateMetaPartition: mp(%v)", mp)
		}
		mw.Lock()
		mw.partitions = newPartitions
		mw.ranges = newRanges
		mw.Unlock()
		mw.volNotExistCount = 0
		log.LogInfof("clear and updateMetaPartition done: volNotExistCount(%v)", mw.volNotExistCount)
	}

	mw.ossSecure = view.OSSSecure
	mw.ossBucketPolicy = view.OSSBucketPolicy
	mw.volCreateTime = view.CreateTime
	mw.crossRegionHAType = view.CrossRegionHAType
	mw.updateConnConfig(view.ConnConfig)

	if len(rwPartitions) == 0 {
		log.LogInfof("updateMetaPartition: no valid partitions")
		return nil
	}

	mw.Lock()
	mw.rwPartitions = rwPartitions
	mw.unavailPartitions = unavailPartitions
	mw.Unlock()
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
	rwPartitions := make([]*MetaPartition, 0)
	for _, view := range views {
		mp := &MetaPartition{
			PartitionID: view.PartitionID,
			Start:       view.Start,
			End:         view.End,
			Members:     view.Members,
			Learners:    view.Learners,
			Status:      view.Status,
		}
		mp.SetLeaderAddr(view.LeaderAddr)
		mw.replaceOrInsertPartition(mp)
		log.LogInfof("updateMetaPartitionsWithNoCache: mp(%v)", mp)
		if mp.Status == proto.ReadWrite {
			rwPartitions = append(rwPartitions, mp)
		}
	}
	if len(rwPartitions) == 0 {
		log.LogInfof("updateMetaPartitionsWithNoCache: no valid partitions")
		return nil
	}

	mw.Lock()
	mw.rwPartitions = rwPartitions
	mw.Unlock()

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
