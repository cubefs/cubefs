package master

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/oss"
	"net/http"

	"github.com/chubaofs/chubaofs/proto"
)

func (m *Server) createOSSUser(w http.ResponseWriter, r *http.Request) {
	var (
		akPolicy *oss.AKPolicy
		owner    string
		err      error
	)
	if owner, err = parseOwner(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.cluster.createKey(owner); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func (m *Server) deleteOSSUser(w http.ResponseWriter, r *http.Request) {
	var (
		owner string
		err   error
	)
	if owner, err = parseOwner(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.deleteKey(owner); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg := fmt.Sprintf("delete OSS user[%v] successfully", owner)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) getOSSAKInfo(w http.ResponseWriter, r *http.Request) {
	var (
		ak       string
		akPolicy *oss.AKPolicy
		err      error
	)
	if ak, err = parseAccessKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.cluster.getKeyInfo(ak); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func (m *Server) getOSSUserInfo(w http.ResponseWriter, r *http.Request) {
	var (
		owner    string
		akPolicy *oss.AKPolicy
		err      error
	)
	if owner, err = parseOwner(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.cluster.getUserInfo(owner); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func (m *Server) addOSSPolicy(w http.ResponseWriter, r *http.Request) {
	var (
		ak         string
		akPolicy   *oss.AKPolicy
		userPolicy *oss.UserPolicy
		err        error
	)
	if ak, userPolicy, err = parseAKAndPolicy(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.cluster.addPolicy(ak, userPolicy); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func (m *Server) deleteOSSPolicy(w http.ResponseWriter, r *http.Request) {
	var (
		ak         string
		akPolicy   *oss.AKPolicy
		userPolicy *oss.UserPolicy
		err        error
	)
	if ak, userPolicy, err = parseAKAndPolicy(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.cluster.deletePolicy(ak, userPolicy); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func parseAKAndPolicy(r *http.Request) (ak string, userPolicy *oss.UserPolicy, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ak, err = extractAccessKey(r); err != nil {
		return
	}
	if userPolicy, err = extractPolicy(r); err != nil {
		return
	}
	return
}

func parseOwner(r *http.Request) (owner string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if owner, err = extractOwner(r); err != nil {
		return
	}
	return
}

func parseAccessKey(r *http.Request) (ak string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ak, err = extractAccessKey(r); err != nil {
		return
	}
	return
}

func extractAccessKey(r *http.Request) (ak string, err error) {
	if ak = r.FormValue(akKey); ak == "" {
		err = keyNotFound(akKey)
		return
	}
	return
}

func extractPolicy(r *http.Request) (userPolicy *oss.UserPolicy, err error) {
	var policy string
	if policy = r.FormValue(policyKey); policy == "" {
		err = keyNotFound(policyKey)
		return
	}
	if err = json.Unmarshal([]byte(policy), userPolicy); err != nil {
		return
	}
	return
}
