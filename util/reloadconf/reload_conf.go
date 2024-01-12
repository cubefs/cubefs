// Copyright 2023 The CubeFS Authors.
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

package reloadconf

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

// ReloadConf loads remote configuration change dynamically
type ReloadConf struct {
	ConfName      string
	ReloadSec     int
	RequestRemote func() ([]byte, error)

	md5sum []byte
	mutex  sync.Mutex
}

func (self *ReloadConf) reload(reload func(data []byte) error) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// remote load
	if self.RequestRemote != nil {
		errRemote := self.remoteReload(reload)
		if errRemote != nil {
			log.LogWarn("remoteReload failed", self.ConfName, errors.Detail(errRemote))
		} else {
			return nil
		}
	}

	// local load
	errLocal := self.localReload(reload)
	if errLocal != nil {
		err := errors.Info(errLocal, "localReload").Detail(errLocal)
		return err
	}
	return nil
}

func (self *ReloadConf) remoteReload(reload func(data []byte) error) (err error) {
	data, md5sum, err := fetchRemote(self.RequestRemote)
	if err != nil {
		err = errors.Info(err, "fetchRemote").Detail(err)
		return
	}

	// compare whether md5sum is changed
	if bytes.Equal(md5sum, self.md5sum) {
		log.LogDebug("remoteReload:", self.ConfName, "do nothing cause of md5sum is equal")
		return nil
	}

	confName := fmt.Sprintf("%v_%v", self.ConfName, base64.URLEncoding.EncodeToString(md5sum))
	err = os.WriteFile(confName, data, 0o666)
	if err != nil {
		err = errors.Info(err, "os.WriteFile")
		return
	}
	log.LogInfof("remoteReload %v remote file is changed, oldmd5: %v, newmd5: %v", self.ConfName, self.md5sum, md5sum)

	err = reload(data)
	if err != nil {
		os.Remove(confName)
		err = errors.Info(err, "reload", confName).Detail(err)
		return
	}

	err = os.Rename(confName, self.ConfName)
	if err != nil {
		os.Remove(confName)
		err = errors.Info(err, "os.Rename")
		return
	}
	self.md5sum = md5sum

	return
}

func (self *ReloadConf) localReload(reload func(data []byte) error) (err error) {
	data, err := os.ReadFile(self.ConfName)
	if err != nil {
		err = errors.Info(err, "os.ReadFile").Detail(err)
		return
	}
	md5sum := calcMD5Sum(data)

	if bytes.Equal(md5sum, self.md5sum) {
		log.LogDebug("localReload:", self.ConfName, "do nothing cause of md5sum is equal")
		return nil
	}

	log.LogInfof("localReload: %v local file is changed, oldmd5: %v, newmd5: %v", self.ConfName, self.md5sum, md5sum)
	err = reload(data)
	if err != nil {
		err = errors.Info(err, "reload").Detail(err)
		return
	}
	self.md5sum = md5sum
	return
}

func fetchRemote(requestRemote func() ([]byte, error)) (data, md5sum []byte, err error) {
	data, err = requestRemote()
	if err != nil {
		err = errors.Info(err, "io.ReadAll")
		return
	}
	md5sum = calcMD5Sum(data)

	return
}

func StartReload(cfg *ReloadConf, reload func(data []byte) error) (err error) {
	err = cfg.reload(reload)
	if err != nil {
		log.LogError("cfg.reload:", cfg.ConfName, errors.Detail(err))
		return
	}

	if cfg.ReloadSec == 0 {
		return
	}
	go func() {
		dur := time.Duration(cfg.ReloadSec) * time.Second
		for range time.Tick(dur) {
			err := cfg.reload(reload)
			if err != nil {
				log.LogError("cfg.reload:", cfg.ConfName, errors.Detail(err))
			}
		}
	}()
	return
}

func calcMD5Sum(b []byte) []byte {
	h := md5.New()
	h.Write(b)
	return h.Sum(nil)
}
