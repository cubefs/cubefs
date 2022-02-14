package keystore

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/cubefs/cubefs/util/caps"
)

var roleSet = map[string]bool{
	"client":  true,
	"service": true,
}

// KeyInfo defines the key info structure in key store
type KeyInfo struct {
	ID        string `json:"id"`
	AuthKey   []byte `json:"auth_key"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Ts        int64  `json:"create_ts"`
	Role      string `json:"role"`
	Caps      []byte `json:"caps"`
}

// DumpJSONFile dump KeyInfo to file in json format
func (u *KeyInfo) DumpJSONFile(filename string) (err error) {
	var (
		data string
	)
	if data, err = u.DumpJSONStr(); err != nil {
		return
	}

	file, err := os.Create(filename)
	if err != nil {
		return
	}
	defer file.Close()

	_, err = io.WriteString(file, data)
	if err != nil {
		return
	}
	return
}

// DumpJSONStr dump KeyInfo to string in json format
func (u *KeyInfo) DumpJSONStr() (r string, err error) {
	dumpInfo := struct {
		ID        string `json:"id"`
		AuthKey   []byte `json:"auth_key"`
		AccessKey string `json:"access_key"`
		SecretKey string `json:"secret_key"`
		Ts        int64  `json:"create_ts"`
		Role      string `json:"role"`
		Caps      string `json:"caps"`
	}{
		u.ID,
		u.AuthKey,
		u.AccessKey,
		u.SecretKey,
		u.Ts,
		u.Role,
		string(u.Caps),
	}
	data, err := json.MarshalIndent(dumpInfo, "", "  ")
	if err != nil {
		return
	}
	r = string(data)
	return
}

// IsValidID check the validity of ID
func (u *KeyInfo) IsValidID() (err error) {
	re := regexp.MustCompile("^[A-Za-z]{1,1}[A-Za-z0-9_]{0,20}$")
	if !re.MatchString(u.ID) {
		err = fmt.Errorf("invalid ID [%s]", u.ID)
		return
	}
	return
}

// IsValidRole check the validity of role
func (u *KeyInfo) IsValidRole() (err error) {
	if _, ok := roleSet[u.Role]; !ok {
		err = fmt.Errorf("invalid Role [%s]", u.Role)
		return
	}
	return
}

// IsValidCaps check the validity of caps
func (u *KeyInfo) IsValidCaps() (err error) {
	cap := new(caps.Caps)
	if err = cap.Init(u.Caps); err != nil {
		err = fmt.Errorf("Invalid caps [%s] %s", u.Caps, err.Error())
	}
	return
}

// IsValidKeyInfo is a valid of KeyInfo
func (u *KeyInfo) IsValidKeyInfo() (err error) {
	if err = u.IsValidID(); err != nil {
		return
	}
	if err = u.IsValidRole(); err != nil {
		return
	}
	if err = u.IsValidCaps(); err != nil {
		return
	}
	return
}
