package keystore

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/cubefs/cubefs/util/caps"
)

type AccessKeyInfo struct {
	AccessKey string `json:"access_key"`
	ID        string `json:"id"`
}

type AccessKeyCaps struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Caps      []byte `json:"caps"`
	ID        string `json:"user_id"`
}

func (u *AccessKeyCaps) IsValidCaps() (err error) {
	cap := new(caps.Caps)
	if err = cap.Init(u.Caps); err != nil {
		err = fmt.Errorf("Invalid caps [%s] %s", u.Caps, err.Error())
	}
	return
}

func (u *AccessKeyCaps) IsValidAK() (err error) {
	re := regexp.MustCompile("^[A-Za-z0-9]{16}$")
	if !re.MatchString(u.AccessKey) {
		err = fmt.Errorf("invalid AccessKey [%s]", u.AccessKey)
		return
	}
	return
}

func (u *AccessKeyCaps) DumpJSONStr() (r string, err error) {
	dumpInfo := struct {
		AccessKey string `json:"access_key"`
		SecretKey string `json:"secret_key"`
		Caps      string `json:"caps"`
		ID        string `json:"id"`
	}{
		u.AccessKey,
		u.SecretKey,
		string(u.Caps),
		u.ID,
	}
	data, err := json.MarshalIndent(dumpInfo, "", "  ")
	if err != nil {
		return
	}
	r = string(data)
	return
}
