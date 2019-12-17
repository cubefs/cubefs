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

package meta

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/cryptoutil"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/jacobsa/daemonize"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

const (
	MaxSendToMaster = 3
)

type VolumeView struct {
	VolName        string
	MetaPartitions []*MetaPartition
}

type VolStatInfo struct {
	Name      string
	TotalSize uint64
	UsedSize  uint64
}

// VolName view managements
//
func (mw *MetaWrapper) fetchVolumeView() (*VolumeView, error) {
	params := make(map[string]string)
	params["name"] = mw.volname
	authKey, err := calculateAuthKey(mw.owner)
	if err != nil {
		return nil, err
	}
	params["authKey"] = authKey
	var dataBody []byte
	if mw.authenticate {
		mw.accessToken.Type = proto.MsgMasterFetchVolViewReq
		tokenMessage, ts, err := genMasterToken(mw.accessToken, mw.sessionKey)
		if err != nil {
			log.LogWarnf("fetchVolumeView generate token failed: err(%v)", err)
			return nil, err
		}
		params[proto.ClientMessage] = tokenMessage
		body, err := mw.master.Request(http.MethodPost, proto.ClientVol, params, nil)
		if err != nil {
			log.LogWarnf("fetchVolumeView request: err(%v)", err)
			return nil, err
		}
		dataBody, err = mw.parseRespWithAuth(body, ts)
		if err != nil {
			log.LogWarnf("fetchVolumeView request: err(%v)", err)
			return nil, err
		}
	} else {
		body, err := mw.master.Request(http.MethodPost, proto.ClientVol, params, nil)
		if err != nil {
			log.LogWarnf("fetchVolumeView request: err(%v)", err)
			return nil, err
		}
		dataBody = body
	}

	view := new(VolumeView)
	if err = json.Unmarshal(dataBody, view); err != nil {
		log.LogWarnf("fetchVolumeView unmarshal: err(%v) body(%v)", err, string(dataBody))
		return nil, err
	}
	return view, nil
}

// fetch and update cluster info if successful
func (mw *MetaWrapper) updateClusterInfo() error {
	body, err := mw.master.Request(http.MethodPost, proto.AdminGetIP, nil, nil)
	if err != nil {
		log.LogWarnf("updateClusterInfo request: err(%v)", err)
		return err
	}

	info := new(proto.ClusterInfo)
	if err = json.Unmarshal(body, info); err != nil {
		log.LogWarnf("updateClusterInfo unmarshal: err(%v)", err)
		return err
	}
	log.LogInfof("ClusterInfo: %v", *info)
	mw.cluster = info.Cluster
	mw.localIP = info.Ip
	return nil
}

func (mw *MetaWrapper) updateVolStatInfo() error {
	params := make(map[string]string)
	params["name"] = mw.volname
	body, err := mw.master.Request(http.MethodPost, proto.ClientVolStat, params, nil)
	if err != nil {
		log.LogWarnf("updateVolStatInfo request: err(%v)", err)
		return err
	}

	info := new(VolStatInfo)
	if err = json.Unmarshal(body, info); err != nil {
		log.LogWarnf("updateVolStatInfo unmarshal: err(%v)", err)
		return err
	}
	atomic.StoreUint64(&mw.totalSize, info.TotalSize)
	atomic.StoreUint64(&mw.usedSize, info.UsedSize)
	log.LogInfof("VolStatInfo: info(%v)", *info)
	return nil
}

func (mw *MetaWrapper) updateMetaPartitions() error {
	view, err := mw.fetchVolumeView()
	if err != nil {
		log.LogInfof("error: %v", err.Error())
		switch err {
		case util.ErrExpiredTicket:
			if e := mw.updateTicket(); e != nil {
				log.LogFlush()
				daemonize.SignalOutcome(err)
				os.Exit(1)
			}
			log.LogInfof("updateTicket: ok!")
			return err
		case util.ErrInvalidTicket:
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

	if len(rwPartitions) == 0 {
		log.LogInfof("updateMetaPartition: no rw partitions")
		return nil
	}

	mw.Lock()
	mw.rwPartitions = rwPartitions
	mw.Unlock()
	return nil
}

func (mw *MetaWrapper) refresh() {
	t := time.NewTicker(RefreshMetaPartitionsInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			mw.updateMetaPartitions()
			mw.updateVolStatInfo()
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
	ticket, err := getTicketFromAuthnode(mw.owner, mw.ticketMess)
	if err != nil {
		return errors.Trace(err, "Update ticket from authnode failed!")
	}
	mw.accessToken.Ticket = ticket.Ticket
	mw.sessionKey = ticket.SessionKey
	return nil
}

func (mw *MetaWrapper) verifyResponse(checkMsg string, serviceID string, ts int64) (err error) {
	var (
		sessionKey []byte
		plaintext  []byte
		resp       proto.APIAccessResp
	)

	if sessionKey, err = cryptoutil.Base64Decode(mw.sessionKey); err != nil {
		return
	}

	if plaintext, err = cryptoutil.DecodeMessage(checkMsg, sessionKey); err != nil {
		return
	}

	if err = json.Unmarshal(plaintext, &resp); err != nil {
		return
	}
	return proto.VerifyAPIRespComm(&resp, mw.accessToken.Type, mw.owner, serviceID, ts)

}

func (mw *MetaWrapper) parseRespWithAuth(body []byte, ts int64) (dataBody []byte, err error) {
	getVolResp := new(proto.GetVolResponse)
	if err = json.Unmarshal(body, getVolResp); err != nil {
		log.LogWarnf("fetchVolumeView unmarshal: err(%v) body(%v)", err, string(body))
		return nil, err
	}
	if err = mw.verifyResponse(getVolResp.CheckMsg, proto.MasterServiceID, ts); err != nil {
		log.LogWarnf("fetchVolumeView verify response: err(%v) body(%v)", err, getVolResp.CheckMsg)
		return nil, err
	}
	var viewBody = &struct {
		Code int32  `json:"code"`
		Msg  string `json:"msg"`
		Data json.RawMessage
	}{}
	if err = json.Unmarshal(getVolResp.VolViewCache, viewBody); err != nil {
		log.LogWarnf("VolViewCache unmarshal: err(%v) body(%v)", err, viewBody)
		return nil, err
	}
	if viewBody.Code != 0 {
		return nil, fmt.Errorf("request error, code[%d], msg[%s]", viewBody.Code, viewBody.Msg)
	}
	return viewBody.Data, err
}
