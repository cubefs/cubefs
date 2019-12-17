package keystore

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/caps"
	"regexp"
)

type AccessKeyInfo struct {
	AccessKey string `json:"access_key"`
	ID        string `json:"id"`
}

type AccessKeyCaps struct {
	AccessKey string `json:"access_key"`
	Caps      []byte `json:"caps"`
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
