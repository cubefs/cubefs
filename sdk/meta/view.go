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
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/jacobsa/daemonize"
)

const (
	MaxSendToMaster = 3
)

type VolumeView struct {
	Name           string
	Owner          string
	MetaPartitions []*MetaPartition
	OSSSecure      *OSSSecure
	CreateTime     int64
}

type OSSSecure struct {
	AccessKey string
	SecretKey string
}

type VolStatInfo = proto.VolStatInfo

func (mw *MetaWrapper) fetchVolumeView() (view *VolumeView, err error) {
	var vv *proto.VolView
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
	if vv.Status == 1 {
		log.LogErrorf("fetchVolumeView: volume has been marked for deletion: volume(%v) status(%v - 0:normal/1:markDelete)",
			vv.Name, vv.Status)
		return nil, proto.ErrVolNotExists
	}
	var convert = func(volView *proto.VolView) *VolumeView {
		result := &VolumeView{
			Name:           volView.Name,
			Owner:          volView.Owner,
			MetaPartitions: make([]*MetaPartition, len(volView.MetaPartitions)),
			OSSSecure:      &OSSSecure{},
			CreateTime:     volView.CreateTime,
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
				LeaderAddr:  mp.LeaderAddr,
				Status:      mp.Status,
			}
		}
		return result
	}
	view = convert(vv)
	return
}

// fetch and update cluster info if successful
func (mw *MetaWrapper) updateClusterInfo() (err error) {
	var info *proto.ClusterInfo
	if info, err = mw.mc.AdminAPI().GetClusterInfo(); err != nil {
		log.LogWarnf("updateClusterInfo: get cluster info fail: err(%v) volume(%v)", err, mw.volname)
		return
	}
	log.LogInfof("updateClusterInfo: get cluster info: cluster(%v) localIP(%v) volume(%v)",
		info.Cluster, info.Ip, mw.volname)
	mw.cluster = info.Cluster
	mw.localIP = info.Ip
	return
}

func (mw *MetaWrapper) updateDirChildrenNumLimit() (err error) {
	var clusterInfo *proto.ClusterInfo
	clusterInfo, err = mw.mc.AdminAPI().GetClusterInfo()
	if err != nil {
		return
	}

	if clusterInfo.DirChildrenNumLimit < proto.MinDirChildrenNumLimit {
		log.LogWarnf("updateDirChildrenNumLimit: DirChildrenNumLimit probably not enabled on master, set to default value(%v)",
			proto.DefaultDirChildrenNumLimit)
		atomic.StoreUint32(&mw.DirChildrenNumLimit, proto.DefaultDirChildrenNumLimit)
	} else {
		atomic.StoreUint32(&mw.DirChildrenNumLimit, clusterInfo.DirChildrenNumLimit)
		log.LogInfof("updateDirChildrenNumLimit: DirChildrenNumLimit(%v)", mw.DirChildrenNumLimit)
	}

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
	atomic.StoreUint64(&mw.inodeCount, info.InodeCount)
	log.LogInfof("VolStatInfo: volume(%v) info(%v)", mw.volname, info)
	return
}

func (mw *MetaWrapper) updateMetaPartitions() error {
	view, err := mw.fetchVolumeView()
	if err != nil {
		log.LogInfof("updateMetaPartition volume(%v) error: %v", mw.volname, err.Error())
		switch err {
		case proto.ErrExpiredTicket:
			// TODO: bad logic, remove later (Mofei Zhang)
			if e := mw.updateTicket(); e != nil {
				log.LogFlush()
				daemonize.SignalOutcome(err)
				os.Exit(1)
			}
			log.LogInfof("updateTicket: ok!")
			return err
		case proto.ErrInvalidTicket:
			// TODO: bad logic, remove later (Mofei Zhang)
			log.LogFlush()
			daemonize.SignalOutcome(err)
			os.Exit(1)
		default:
			return err
		}
	}

	rwPartitions := make([]*MetaPartition, 0)
	for _, mp := range view.MetaPartitions {
		mw.replaceOrInsertPartition(mp)
		log.LogInfof("updateMetaPartition: mp(%v)", mp)
		if mp.Status == proto.ReadWrite {
			rwPartitions = append(rwPartitions, mp)
		}
	}
	mw.ossSecure = view.OSSSecure
	mw.volCreateTime = view.CreateTime

	if len(rwPartitions) == 0 {
		log.LogInfof("updateMetaPartition: no valid partitions")
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

	return mw.updateMetaPartitions()
}

// Should be protected by partMutex, otherwise the caller might not be signaled.
func (mw *MetaWrapper) triggerAndWaitForceUpdate() {
	mw.partMutex.Lock()
	select {
	case mw.forceUpdate <- struct{}{}:
	default:
	}
	mw.partCond.Wait()
	mw.partMutex.Unlock()
}

func (mw *MetaWrapper) refresh() {
	var err error

	t := time.NewTimer(RefreshMetaPartitionsInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if err = mw.updateMetaPartitions(); err != nil {
				mw.onAsyncTaskError.OnError(err)
				log.LogErrorf("updateMetaPartition fail cause: %v", err)
			}
			if err = mw.updateVolStatInfo(); err != nil {
				mw.onAsyncTaskError.OnError(err)
				log.LogErrorf("updateVolStatInfo fail cause: %v", err)
			}
			if err = mw.updateDirChildrenNumLimit(); err != nil {
				mw.onAsyncTaskError.OnError(err)
				log.LogErrorf("updateDirChildrenNumLimit fail cause: %v", err)
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
		case <-mw.closeCh:
			return
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

func (mw *MetaWrapper) updateQuotaInfoTick() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mw.UpdateQuotaInfo()
		case <-mw.closeCh:
			return
		}

	}
}

func (mw *MetaWrapper) UpdateQuotaInfo() {
	quotaInfos, err := mw.mc.AdminAPI().ListQuota(mw.volname)
	if err != nil {
		log.LogWarnf("UpdateQuotaInfo get quota info fail: vol [%v] err [%v]", mw.volname, err)
		return
	}
	mw.QuotaLock.Lock()
	defer mw.QuotaLock.Unlock()
	mw.QuotaInfoMap = make(map[uint32]*proto.QuotaInfo)
	for _, info := range quotaInfos {
		mw.QuotaInfoMap[info.QuotaId] = info
		log.LogDebugf("UpdateQuotaInfo quotaInfo [%v]", info)
	}
}

func (mw *MetaWrapper) IsQuotaLimited(quotaIds []uint32) bool {
	mw.QuotaLock.RLock()
	defer mw.QuotaLock.RUnlock()
	for _, quotaId := range quotaIds {
		if info, isFind := mw.QuotaInfoMap[quotaId]; isFind {
			if info.LimitedInfo.LimitedBytes {
				log.LogDebugf("IsQuotaLimited quotaId [%v]", quotaId)
				return true
			}
		}
		log.LogDebugf("IsQuotaLimited false quota [%v]", quotaId)
	}
	return false
}

func (mw *MetaWrapper) GetQuotaFullPaths() (fullPaths []string) {
	fullPaths = make([]string, 0, 0)
	mw.QuotaLock.RLock()
	defer mw.QuotaLock.RUnlock()
	for _, info := range mw.QuotaInfoMap {
		fullPaths = append(fullPaths, info.FullPath)
	}
	return fullPaths
}

func findNestedPath(path1, path2 string) string {
	if path1 == path2 {
		return ""
	}

	if len(path1) > len(path2) {
		return ""
	}

	if strings.HasPrefix(path2, path1) {
		if len(path1) == 1 || strings.HasSuffix(path1, "/") {
			return path2[len(path1):]
		} else if path2[len(path1)] == '/' {
			return path2[len(path1)+1:]
		}
	}

	return ""
}
