// Copyright 2018 The Cubefs Authors.
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

package authnode

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/keystore"
	"github.com/cubefs/cubefs/util/log"

	"fmt"
)

const (
	nodeType = "auth"
)

func (m *Server) getTicket(w http.ResponseWriter, r *http.Request) {
	var (
		plaintext []byte
		err       error
		jobj      proto.AuthGetTicketReq
		ts        int64
		key       []byte
		message   string
	)

	if m.metaReady == false {
		log.LogWarnf("action[handlerWithInterceptor] leader meta has not ready")
		http.Error(w, m.leaderInfo.addr, http.StatusBadRequest)
	}

	if plaintext, err = m.extractClientReqInfo(r); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = json.Unmarshal([]byte(plaintext), &jobj); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if key, err = m.getSecretKey(jobj.ClientID); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if ts, err = proto.ParseVerifier(jobj.Verifier, key); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = validateGetTicketReqFormat(&jobj); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if message, err = m.genGetTicketAuthResp(&jobj, ts, r); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPAuthReply(message))
	return
}

func (m *Server) raftNodeOp(w http.ResponseWriter, r *http.Request) {
	var (
		plaintext []byte
		err       error
		jobj      proto.AuthRaftNodeReq
		ticket    cryptoutil.Ticket
		ts        int64
		message   string
	)

	if plaintext, err = m.extractClientReqInfo(r); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = json.Unmarshal([]byte(plaintext), &jobj); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "Unmarshal AuthRaftNodeReq failed: " + err.Error()})
		return
	}

	apiReq := jobj.APIReq
	raftNodeInfo := jobj.RaftNodeInfo

	if err = proto.VerifyAPIAccessReqIDs(&apiReq); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "VerifyAPIAccessReqIDs failed: " + err.Error()})
		return
	}

	if ticket, ts, err = proto.ExtractAPIAccessTicket(&apiReq, m.cluster.AuthSecretKey); err != nil {
		if err == proto.ErrExpiredTicket {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeExpiredTicket, Msg: "ExtractAPIAccessTicket failed: " + err.Error()})
		} else {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "ExtractAPIAccessTicket failed: " + err.Error()})
		}
		return
	}

	if err = proto.CheckAPIAccessCaps(&ticket, proto.APIRsc, apiReq.Type, proto.APIAccess); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "CheckAPIAccessCaps failed: " + err.Error()})
		return
	}

	switch apiReq.Type {
	case proto.MsgAuthAddRaftNodeReq:
		err = m.handleAddRaftNode(&raftNodeInfo)
	case proto.MsgAuthRemoveRaftNodeReq:
		err = m.handleRemoveRaftNode(&raftNodeInfo)
	default:
	}

	if err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeAuthKeyStoreError, Msg: err.Error()})
		return
	}

	msg := fmt.Sprintf("add raft node id :%v, addr:%v successfully \n", raftNodeInfo.ID, raftNodeInfo.Addr)

	if message, err = genAuthRaftNodeOpResp(&apiReq, ts, ticket.SessionKey.Key, msg); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeAuthRaftNodeGenRespError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPAuthReply(message))
	return
}

func (m *Server) handleAddRaftNode(raftNodeInfo *proto.AuthRaftNodeInfo) (err error) {
	if err = m.cluster.addRaftNode(raftNodeInfo.ID, raftNodeInfo.Addr); err != nil {
		return
	}
	return
}

func (m *Server) handleRemoveRaftNode(raftNodeInfo *proto.AuthRaftNodeInfo) (err error) {
	if err = m.cluster.removeRaftNode(raftNodeInfo.ID, raftNodeInfo.Addr); err != nil {
		return
	}
	return
}

func genAuthRaftNodeOpResp(req *proto.APIAccessReq, ts int64, key []byte, msg string) (message string, err error) {
	var (
		jresp []byte
		resp  proto.AuthRaftNodeResp
	)

	resp.APIResp.Type = req.Type + 1
	resp.APIResp.ClientID = req.ClientID
	resp.APIResp.ServiceID = req.ServiceID
	resp.APIResp.Verifier = ts + 1 // increase ts by one for client verify server identity

	resp.Msg = msg

	if jresp, err = json.Marshal(resp); err != nil {
		err = fmt.Errorf("json marshal for response failed %s", err.Error())
		return
	}

	if message, err = cryptoutil.EncodeMessage(jresp, key); err != nil {
		err = fmt.Errorf("encode message for response failed %s", err.Error())
		return
	}

	return
}

func (m *Server) apiAccessEntry(w http.ResponseWriter, r *http.Request) {
	var (
		plaintext  []byte
		err        error
		jobj       proto.AuthAPIAccessReq
		ticket     cryptoutil.Ticket
		ts         int64
		newKeyInfo *keystore.KeyInfo
		message    string
	)

	if plaintext, err = m.extractClientReqInfo(r); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = json.Unmarshal([]byte(plaintext), &jobj); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "Unmarshal AuthAPIAccessReq failed: " + err.Error()})
		return
	}

	apiReq := jobj.APIReq
	keyInfo := jobj.KeyInfo

	if err = keyInfo.IsValidID(); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	switch apiReq.Type {
	case proto.MsgAuthCreateKeyReq:
		if keyInfo.ID == proto.AuthServiceID {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "AuthServiceID is reserved"})
			return
		}
		if err = keyInfo.IsValidKeyInfo(); err != nil {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	case proto.MsgAuthDeleteKeyReq:
	case proto.MsgAuthGetKeyReq:
	case proto.MsgAuthAddCapsReq:
		fallthrough
	case proto.MsgAuthDeleteCapsReq:
		if err = keyInfo.IsValidCaps(); err != nil {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	case proto.MsgAuthGetCapsReq:
	default:
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("invalid request messge type %x", int32(apiReq.Type)).Error()})
		return
	}

	if err = proto.VerifyAPIAccessReqIDs(&apiReq); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "VerifyAPIAccessReqIDs failed: " + err.Error()})
		return
	}

	if ticket, ts, err = proto.ExtractAPIAccessTicket(&apiReq, m.cluster.AuthSecretKey); err != nil {
		if err == proto.ErrExpiredTicket {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeExpiredTicket, Msg: "ExtractAPIAccessTicket failed: " + err.Error()})
		} else {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "ExtractAPIAccessTicket failed: " + err.Error()})
		}
		return
	}

	if err = proto.CheckAPIAccessCaps(&ticket, proto.APIRsc, apiReq.Type, proto.APIAccess); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "CheckAPIAccessCaps failed: " + err.Error()})
		return
	}

	switch apiReq.Type {
	case proto.MsgAuthCreateKeyReq:
		newKeyInfo, err = m.handleCreateKey(&keyInfo)
	case proto.MsgAuthDeleteKeyReq:
		newKeyInfo, err = m.handleDeleteKey(&keyInfo)
	case proto.MsgAuthGetKeyReq:
		newKeyInfo, err = m.handleGetKey(&keyInfo)
	case proto.MsgAuthAddCapsReq:
		newKeyInfo, err = m.handleAddCaps(&keyInfo)
	case proto.MsgAuthDeleteCapsReq:
		newKeyInfo, err = m.handleDeleteCaps(&keyInfo)
	case proto.MsgAuthGetCapsReq:
		newKeyInfo, err = m.handleGetCaps(&keyInfo)
	}

	if err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeAuthKeyStoreError, Msg: err.Error()})
		return
	}

	if message, err = genAuthAPIAccessResp(&apiReq, newKeyInfo, ts, ticket.SessionKey.Key); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeAuthAPIAccessGenRespError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPAuthReply(message))
	return
}

func (m *Server) handleCreateKey(keyInfo *keystore.KeyInfo) (res *keystore.KeyInfo, err error) {
	if res, err = m.cluster.CreateNewKey(keyInfo.ID, keyInfo); err != nil {
		return
	}
	return
}

func (m *Server) handleDeleteKey(keyInfo *keystore.KeyInfo) (res *keystore.KeyInfo, err error) {
	if res, err = m.cluster.DeleteKey(keyInfo.ID); err != nil {
		return
	}
	return
}

func (m *Server) handleGetKey(keyInfo *keystore.KeyInfo) (res *keystore.KeyInfo, err error) {
	if res, err = m.getSecretKeyInfo(keyInfo.ID); err != nil {
		return
	}
	return
}

func (m *Server) handleAddCaps(keyInfo *keystore.KeyInfo) (res *keystore.KeyInfo, err error) {
	return m.cluster.AddCaps(keyInfo.ID, keyInfo)
}

func (m *Server) handleDeleteCaps(keyInfo *keystore.KeyInfo) (res *keystore.KeyInfo, err error) {
	return m.cluster.DeleteCaps(keyInfo.ID, keyInfo)
}

func (m *Server) handleGetCaps(keyInfo *keystore.KeyInfo) (res *keystore.KeyInfo, err error) {
	var info *keystore.KeyInfo
	if info, err = m.getSecretKeyInfo(keyInfo.ID); err != nil {
		return
	}
	res = &keystore.KeyInfo{
		ID:        info.ID,
		AccessKey: info.AccessKey,
		SecretKey: info.SecretKey,
		Caps:      info.Caps,
	}
	return
}

func (m *Server) extractClientReqInfo(r *http.Request) (plaintext []byte, err error) {
	var (
		message string
	)
	if err = r.ParseForm(); err != nil {
		return
	}

	if message = r.FormValue(proto.ClientMessage); message == "" {
		err = keyNotFound(proto.ClientMessage)
		return
	}

	if plaintext, err = cryptoutil.Base64Decode(message); err != nil {
		return
	}

	return
}

func (m *Server) osCapsOp(w http.ResponseWriter, r *http.Request) {
	var (
		plaintext []byte
		err       error
		jobj      proto.AuthOSAccessKeyReq
		ticket    cryptoutil.Ticket
		ts        int64
		newAkCaps *keystore.AccessKeyCaps
		message   string
	)

	if plaintext, err = m.extractClientReqInfo(r); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if err = json.Unmarshal([]byte(plaintext), &jobj); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "Unmarshal AuthOSAccessKeyReq failed: " + err.Error()})
		return
	}

	apiReq := jobj.APIReq
	akCaps := jobj.AKCaps

	if err = akCaps.IsValidAK(); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	switch apiReq.Type {
	case proto.MsgAuthOSAddCapsReq:
		fallthrough
	case proto.MsgAuthOSDeleteCapsReq:
		if err = akCaps.IsValidCaps(); err != nil {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
	case proto.MsgAuthOSGetCapsReq:
	default:
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: fmt.Errorf("invalid request messge type %x", int32(apiReq.Type)).Error()})
		return
	}

	if err = proto.VerifyAPIAccessReqIDs(&apiReq); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "VerifyAPIAccessReqIDs failed: " + err.Error()})
		return
	}

	if ticket, ts, err = proto.ExtractAPIAccessTicket(&apiReq, m.cluster.AuthSecretKey); err != nil {
		if err == proto.ErrExpiredTicket {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeExpiredTicket, Msg: "ExtractAPIAccessTicket failed: " + err.Error()})
		} else {
			sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "ExtractAPIAccessTicket failed: " + err.Error()})
		}
		return
	}

	if err = proto.CheckAPIAccessCaps(&ticket, proto.APIRsc, apiReq.Type, proto.APIAccess); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeParamError, Msg: "CheckAPIAccessCaps failed: " + err.Error()})
		return
	}

	switch apiReq.Type {
	case proto.MsgAuthOSAddCapsReq:
		newAkCaps, err = m.handleOSAddCaps(&akCaps)
	case proto.MsgAuthOSDeleteCapsReq:
		newAkCaps, err = m.handleOSDeleteCaps(&akCaps)
	case proto.MsgAuthOSGetCapsReq:
		newAkCaps, err = m.handleOSGetCaps(&akCaps)
	}

	if err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeAuthKeyStoreError, Msg: err.Error()})
		return
	}

	if message, err = genAuthOSCapsOpResp(&apiReq, newAkCaps, ts, ticket.SessionKey.Key); err != nil {
		sendErrReply(w, r, &proto.HTTPAuthReply{Code: proto.ErrCodeAuthOSCapsOpGenRespError, Msg: err.Error()})
		return
	}

	sendOkReply(w, r, newSuccessHTTPAuthReply(message))
	return
}

func (m *Server) genTicket(key []byte, serviceID string, IP string, caps []byte) (ticket cryptoutil.Ticket) {
	currentTime := time.Now().Unix()
	ticket.Version = cryptoutil.TicketVersion
	ticket.ServiceID = serviceID
	ticket.SessionKey.Ctime = currentTime
	ticket.SessionKey.Key = cryptoutil.AuthGenSessionKeyTS(key)
	ticket.Exp = currentTime + cryptoutil.TicketAge
	ticket.IP = IP
	ticket.Caps = caps
	return
}

func (m *Server) getSecretKey(id string) (key []byte, err error) {
	var (
		keyInfo *keystore.KeyInfo
	)
	if keyInfo, err = m.getSecretKeyInfo(id); err != nil {
		return
	}
	return keyInfo.AuthKey, err
}

func (m *Server) getSecretKeyInfo(id string) (keyInfo *keystore.KeyInfo, err error) {
	if id == proto.AuthServiceID {
		keyInfo = &keystore.KeyInfo{
			AuthKey: m.cluster.AuthSecretKey,
			Caps:    []byte(`{"API": ["*:*:*"]}`),
		}
	} else {
		if keyInfo, err = m.cluster.GetKey(id); err != nil {
			return
		}
	}
	return
}

func (m *Server) genGetTicketAuthResp(req *proto.AuthGetTicketReq, ts int64, r *http.Request) (message string, err error) {
	var (
		jticket    []byte
		jresp      []byte
		resp       proto.AuthGetTicketResp
		serviceKey []byte
		clientKey  []byte
		caps       []byte
		keyInfo    *keystore.KeyInfo
	)

	resp.Type = req.Type + 1
	resp.ClientID = req.ClientID
	resp.ServiceID = req.ServiceID
	// increase ts by one for client verify server
	resp.Verifier = ts + 1

	if keyInfo, err = m.getSecretKeyInfo(resp.ClientID); err != nil {
		return
	}
	caps = keyInfo.Caps

	// Use service key to encrypt ticket
	if serviceKey, err = m.getSecretKey(req.ServiceID); err != nil {
		return
	}

	ticket := m.genTicket(serviceKey, resp.ServiceID, iputil.RealIP(r), caps)
	resp.SessionKey = ticket.SessionKey

	if jticket, err = json.Marshal(ticket); err != nil {
		return
	}

	if resp.Ticket, err = cryptoutil.EncodeMessage(jticket, serviceKey); err != nil {
		return
	}

	if jresp, err = json.Marshal(resp); err != nil {
		return
	}

	// Use client secret key to encrypt response message
	if keyInfo, err = m.getSecretKeyInfo(resp.ClientID); err != nil {
		return
	}
	clientKey = keyInfo.AuthKey
	if message, err = cryptoutil.EncodeMessage(jresp, clientKey); err != nil {
		return
	}

	return
}

func validateGetTicketReqFormat(req *proto.AuthGetTicketReq) (err error) {
	if err = proto.IsValidClientID(req.ClientID); err != nil {
		return
	}

	if err = proto.IsValidServiceID(req.ServiceID); err != nil {
		return
	}

	if err = proto.IsValidMsgReqType(req.ServiceID, req.Type); err != nil {
		return
	}
	return
}

func genAuthAPIAccessResp(req *proto.APIAccessReq, keyInfo *keystore.KeyInfo, ts int64, key []byte) (message string, err error) {
	var (
		jresp []byte
		resp  proto.AuthAPIAccessResp
	)

	resp.APIResp.Type = req.Type + 1
	resp.APIResp.ClientID = req.ClientID
	resp.APIResp.ServiceID = req.ServiceID
	resp.APIResp.Verifier = ts + 1 // increase ts by one for client verify server

	resp.KeyInfo = *keyInfo

	if jresp, err = json.Marshal(resp); err != nil {
		err = fmt.Errorf("json marshal for response failed %s", err.Error())
		return
	}

	if message, err = cryptoutil.EncodeMessage(jresp, key); err != nil {
		err = fmt.Errorf("encode message for response failed %s", err.Error())
		return
	}

	return
}

func (m *Server) handleOSAddCaps(akCaps *keystore.AccessKeyCaps) (newAKCaps *keystore.AccessKeyCaps, err error) {
	var akInfo *keystore.AccessKeyInfo
	if akInfo, err = m.cluster.GetAKInfo(akCaps.AccessKey); err != nil {
		return
	}
	keyInfo := &keystore.KeyInfo{
		ID:   akInfo.ID,
		Caps: akCaps.Caps,
	}
	if keyInfo, err = m.cluster.AddCaps(akInfo.ID, keyInfo); err != nil {
		return
	}
	newAKCaps = &keystore.AccessKeyCaps{
		AccessKey: keyInfo.AccessKey,
		Caps:      keyInfo.Caps,
	}
	return newAKCaps, err
}

func (m *Server) handleOSDeleteCaps(akCaps *keystore.AccessKeyCaps) (newAKCaps *keystore.AccessKeyCaps, err error) {
	var akInfo *keystore.AccessKeyInfo
	if akInfo, err = m.cluster.GetAKInfo(akCaps.AccessKey); err != nil {
		return
	}
	keyInfo := &keystore.KeyInfo{
		ID:   akInfo.ID,
		Caps: akCaps.Caps,
	}
	if keyInfo, err = m.cluster.DeleteCaps(akInfo.ID, keyInfo); err != nil {
		return
	}
	newAKCaps = &keystore.AccessKeyCaps{
		AccessKey: keyInfo.AccessKey,
		Caps:      keyInfo.Caps,
	}
	return newAKCaps, err
}

func (m *Server) handleOSGetCaps(akCaps *keystore.AccessKeyCaps) (newAKCaps *keystore.AccessKeyCaps, err error) {
	var akInfo *keystore.AccessKeyInfo
	var keyInfo *keystore.KeyInfo
	if akInfo, err = m.cluster.GetAKInfo(akCaps.AccessKey); err != nil {
		return
	}
	if keyInfo, err = m.getSecretKeyInfo(akInfo.ID); err != nil {
		return
	}
	newAKCaps = &keystore.AccessKeyCaps{
		AccessKey: keyInfo.AccessKey,
		SecretKey: keyInfo.SecretKey,
		Caps:      keyInfo.Caps,
		ID:        keyInfo.ID,
	}
	return newAKCaps, err
}

func genAuthOSCapsOpResp(req *proto.APIAccessReq, akCaps *keystore.AccessKeyCaps, ts int64, key []byte) (message string, err error) {
	var (
		jresp []byte
		resp  proto.AuthOSAccessKeyResp
	)

	resp.APIResp.Type = req.Type + 1
	resp.APIResp.ClientID = req.ClientID
	resp.APIResp.ServiceID = req.ServiceID
	resp.APIResp.Verifier = ts + 1 // increase ts by one for client verify server

	resp.AKCaps = *akCaps

	if jresp, err = json.Marshal(resp); err != nil {
		err = fmt.Errorf("json marshal for response failed %s", err.Error())
		return
	}

	if message, err = cryptoutil.EncodeMessage(jresp, key); err != nil {
		err = fmt.Errorf("encode message for response failed %s", err.Error())
		return
	}

	return
}

func newSuccessHTTPAuthReply(data interface{}) *proto.HTTPAuthReply {
	return &proto.HTTPAuthReply{Code: proto.ErrCodeSuccess, Msg: proto.ErrSuc.Error(), Data: data}
}

func sendOkReply(w http.ResponseWriter, r *http.Request, HTTPAuthReply *proto.HTTPAuthReply) (err error) {
	reply, err := json.Marshal(HTTPAuthReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", HTTPAuthReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	send(w, r, reply)
	return
}

func send(w http.ResponseWriter, r *http.Request, reply []byte) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
		return
	}
	log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	return
}

func keyNotFound(name string) (err error) {
	return errors.NewErrorf("parameter %v not found", name)
}

func sendErrReply(w http.ResponseWriter, r *http.Request, HTTPAuthReply *proto.HTTPAuthReply) {
	log.LogInfof("URL[%v],remoteAddr[%v],response err[%v]", r.URL, r.RemoteAddr, HTTPAuthReply)
	reply, err := json.Marshal(HTTPAuthReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", HTTPAuthReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}
	return
}
