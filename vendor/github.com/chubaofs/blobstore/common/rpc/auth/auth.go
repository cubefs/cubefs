package auth

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net/http"
)

const (
	// md5 need 16 byte
	TokenKeyLenth = 16

	TokenHeaderKey = "OPPO-OCS-AUTH-TOKEN"

	DefaultAuthSecret = "OPPO-AUTHER-20210310"
)

var errMismatchToken = errors.New("mismatch token")

type Config struct {
	EnableAuth bool   `json:"enable_auth"`
	Secret     string `json:"secret"`
}

// simply: use timestamp as a token calculate param
type authInfo struct {
	timestamp int64
	token     []byte
	// other auth content
	others []byte
}

func encodeAuthInfo(info *authInfo) (ret string, err error) {
	w := bytes.NewBuffer([]byte{})
	if err = binary.Write(w, binary.LittleEndian, &info.timestamp); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, &info.token); err != nil {
		return
	}
	return base64.URLEncoding.EncodeToString(w.Bytes()), nil
}

func decodeAuthInfo(encodeStr string) (info *authInfo, err error) {
	info = new(authInfo)
	b, err := base64.URLEncoding.DecodeString(encodeStr)
	if err != nil {
		return
	}

	info.token = make([]byte, TokenKeyLenth, TokenKeyLenth)
	r := bytes.NewBuffer(b)
	if err = binary.Read(r, binary.LittleEndian, &info.timestamp); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &info.token); err != nil {
		return
	}
	return
}

// calculate auth token with params and secret
func calculate(info *authInfo, secret []byte) (err error) {
	hash := md5.New()
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(info.timestamp))
	hash.Write(info.others)
	hash.Write(b)
	hash.Write(secret)
	info.token = hash.Sum(nil)
	return
}

// verify auth token with params and secret
func verify(info *authInfo, secret []byte) (err error) {
	checkAuthInfo := &authInfo{timestamp: info.timestamp, others: info.others}
	calculate(checkAuthInfo, secret)
	if !bytes.Equal(checkAuthInfo.token, info.token) {
		return errMismatchToken
	}
	return
}

func genEncodeStr(req *http.Request) []byte {
	calStr := req.URL.Path + req.URL.RawQuery
	return []byte(calStr)
}
