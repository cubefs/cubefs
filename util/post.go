// Copyright 2018 The Containerfs Authors.
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

package util

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/tiglabs/containerfs/util/log"
)

const (
	TaskWaitResponseTimeOut = 2 * time.Second
)

func PostToNode(data []byte, url string) (msg []byte, err error) {
	log.LogDebug(fmt.Sprintf("action[PostToNode],url:%v,send data:%v", url, string(data)))
	client := &http.Client{Timeout: TaskWaitResponseTimeOut}
	buff := bytes.NewBuffer(data)
	req, err := http.NewRequest("POST", url, buff)
	if err != nil {
		log.LogError(fmt.Sprintf("action[PostToNode],url:%v,err:%v", url, err.Error()))
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	resp, err := client.Do(req)
	if err != nil {
		log.LogError(fmt.Sprintf("action[PostToNode],url:%v, err:%v", url, err.Error()))
		return nil, err
	}
	defer resp.Body.Close()
	msg, _ = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf(" action[PostToNode] Data send failed,url:%v, status code:%v ", url, strconv.Itoa(resp.StatusCode))
		return msg, err
	}

	return msg, nil
}
