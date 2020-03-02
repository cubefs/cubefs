package master

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/oss"
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

func (m *Server) createOSSUserWithKey(w http.ResponseWriter, r *http.Request) {
	var (
		akPolicy *oss.AKPolicy
		owner    string
		ak       string
		sk       string
		err      error
	)
	if owner, ak, sk, err = parseOwnerAndKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.cluster.createUserWithKey(owner, ak, sk); err != nil {
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
		body       []byte
		err        error
	)
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeReadBodyError, Msg: err.Error()})
		return
	}
	userPolicy = &oss.UserPolicy{}
	if err = json.Unmarshal(body, userPolicy); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeUnmarshalData, Msg: err.Error()})
		return
	}
	if ak, err = parseAccessKey(r); err != nil {
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
		body       []byte
		err        error
	)
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeReadBodyError, Msg: err.Error()})
		return
	}
	userPolicy = &oss.UserPolicy{}
	if err = json.Unmarshal(body, userPolicy); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeUnmarshalData, Msg: err.Error()})
		return
	}
	if ak, err = parseAccessKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.cluster.deletePolicy(ak, userPolicy); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func (m *Server) deleteOSSVolPolicy(w http.ResponseWriter, r *http.Request) {
	var (
		vol string
		err error
	)
	if vol, err = parseVolName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.deleteVolPolicy(vol); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg := fmt.Sprintf("delete OSS vol[%v] policy successfully", vol)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) transferOSSVol(w http.ResponseWriter, r *http.Request) {
	var (
		vol       string
		ak        string
		targetKey string
		akPolicy  *oss.AKPolicy
		err       error
	)
	if vol, ak, targetKey, err = parseVolAndKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if _, err = m.cluster.getVol(vol); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if akPolicy, err = m.cluster.transferVol(vol, ak, targetKey); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
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

func parseOwnerAndKey(r *http.Request) (owner, ak, sk string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if owner, err = extractOwner(r); err != nil {
		return
	}
	if ak, err = extractAccessKey(r); err != nil {
		return
	}
	if sk, err = extractSecretKey(r); err != nil {
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

func parseVolAndKey(r *http.Request) (name, accessKey, targetKey string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
	if accessKey, err = extractAccessKey(r); err != nil {
		return
	}
	if targetKey, err = extractTargetKey(r); err != nil {
		return
	}
	return

}

func extractAccessKey(r *http.Request) (ak string, err error) {
	if ak = r.FormValue(akKey); ak == "" {
		err = keyNotFound(akKey)
		return
	}
	if !akRegexp.MatchString(ak) {
		return "", errors.New("accesskey can only be number and letters")
	}
	return
}

func extractTargetKey(r *http.Request) (targetAK string, err error) {
	if targetAK = r.FormValue(targetKey); targetAK == "" {
		err = keyNotFound(targetKey)
		return
	}
	if !akRegexp.MatchString(targetAK) {
		return "", errors.New("accesskey can only be number and letters")
	}
	return
}

func extractSecretKey(r *http.Request) (sk string, err error) {
	if sk = r.FormValue(skKey); sk == "" {
		err = keyNotFound(skKey)
		return
	}
	if !skRegexp.MatchString(sk) {
		return "", errors.New("secretkey can only be number and letters")
	}
	return
}
