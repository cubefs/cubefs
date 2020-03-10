package master

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

func (m *Server) createUser(w http.ResponseWriter, r *http.Request) {
	var (
		akPolicy *proto.AKPolicy
		owner    string
		password string
		err      error
	)
	if owner, password, err = parseOwnerAndPassword(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.user.createKey(owner, password); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func (m *Server) createUserWithKey(w http.ResponseWriter, r *http.Request) {
	var (
		akPolicy *proto.AKPolicy
		owner    string
		password string
		ak       string
		sk       string
		err      error
	)
	if owner, password, ak, sk, err = parseOwnerAndKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.user.createUserWithKey(owner, password, ak, sk); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func (m *Server) deleteUser(w http.ResponseWriter, r *http.Request) {
	var (
		owner string
		err   error
	)
	if owner, err = parseOwner(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.user.deleteKey(owner); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg := fmt.Sprintf("delete user[%v] successfully", owner)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) getUserAKInfo(w http.ResponseWriter, r *http.Request) {
	var (
		ak       string
		akPolicy *proto.AKPolicy
		err      error
	)
	if ak, err = parseAccessKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.user.getKeyInfo(ak); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func (m *Server) getUserInfo(w http.ResponseWriter, r *http.Request) {
	var (
		owner    string
		akPolicy *proto.AKPolicy
		err      error
	)
	if owner, err = parseOwner(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.user.getUserInfo(owner); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func (m *Server) addUserPolicy(w http.ResponseWriter, r *http.Request) {
	var (
		ak         string
		akPolicy   *proto.AKPolicy
		userPolicy *proto.UserPolicy
		body       []byte
		err        error
	)
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeReadBodyError, Msg: err.Error()})
		return
	}
	userPolicy = &proto.UserPolicy{}
	if err = json.Unmarshal(body, userPolicy); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeUnmarshalData, Msg: err.Error()})
		return
	}
	if ak, err = parseAccessKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.user.addPolicy(ak, userPolicy); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func (m *Server) deleteUserPolicy(w http.ResponseWriter, r *http.Request) {
	var (
		ak         string
		akPolicy   *proto.AKPolicy
		userPolicy *proto.UserPolicy
		body       []byte
		err        error
	)
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeReadBodyError, Msg: err.Error()})
		return
	}
	userPolicy = &proto.UserPolicy{}
	if err = json.Unmarshal(body, userPolicy); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeUnmarshalData, Msg: err.Error()})
		return
	}
	if ak, err = parseAccessKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if akPolicy, err = m.user.deletePolicy(ak, userPolicy); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(akPolicy))
}

func (m *Server) deleteUserVolPolicy(w http.ResponseWriter, r *http.Request) {
	var (
		vol string
		err error
	)
	if vol, err = parseVolName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.user.deleteVolPolicy(vol); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	msg := fmt.Sprintf("delete vol[%v] policy successfully", vol)
	log.LogWarn(msg)
	sendOkReply(w, r, newSuccessHTTPReply(msg))
}

func (m *Server) transferUserVol(w http.ResponseWriter, r *http.Request) {
	var (
		volName   string
		ak        string
		targetKey string
		akPolicy  *proto.AKPolicy
		vol       *Vol
		err       error
	)
	if volName, ak, targetKey, err = parseVolAndKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if vol, err = m.cluster.getVol(volName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}
	if akPolicy, err = m.user.transferVol(volName, ak, targetKey); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	owner := vol.Owner
	vol.Owner = akPolicy.UserID
	if err = m.cluster.syncUpdateVol(vol); err != nil {
		vol.Owner = owner
		err = proto.ErrPersistenceByRaft
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

func parseOwnerAndKey(r *http.Request) (owner, password, ak, sk string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if owner, err = extractOwner(r); err != nil {
		return
	}
	if password, err = extractPassword(r); err != nil {
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

func parseOwnerAndPassword(r *http.Request) (owner, password string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if owner, err = extractOwner(r); err != nil {
		return
	}
	if password, err = extractPassword(r); err != nil {
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

func extractPassword(r *http.Request) (password string, err error) {
	if password = r.FormValue(passwordKey); password == "" {
		err = keyNotFound(passwordKey)
		return
	}
	// todo password regex
	return
}
