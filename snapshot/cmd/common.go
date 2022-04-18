// Copyright 2020 The Chubao Authors.
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

package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"io/ioutil"
	"net/http"
)

var (
	MasterAddr string
	VolName    string
	MetaPort   string
	VerSeq     uint64
)

type Inode struct {
	Inode      uint64
	Type       uint32
	Size       uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	NLink      uint32

	Dens  []*Dentry
	Valid bool
}

func (i *Inode) String() string {
	data, err := json.Marshal(i)
	if err != nil {
		return ""
	}
	return string(data)
}

type Dentry struct {
	ParentId uint64
	Name     string
	Inode    uint64
	Type     uint32

	Valid bool
}

func (d *Dentry) String() string {
	data, err := json.Marshal(d)
	if err != nil {
		return ""
	}
	return string(data)
}

func getMetaPartitions(addr, name string) ([]*proto.MetaPartitionView, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s%s?name=%s", addr, proto.ClientMetaPartitions, name))
	if err != nil {
		return nil, fmt.Errorf("Get meta partitions failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Get meta partitions read all body failed: %v", err)
	}

	body := &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(data, body); err != nil {
		return nil, fmt.Errorf("Unmarshal meta partitions body failed: %v", err)
	}

	var mps []*proto.MetaPartitionView
	if err = json.Unmarshal(body.Data, &mps); err != nil {
		return nil, fmt.Errorf("Unmarshal meta partitions view failed: %v", err)
	}
	return mps, nil
}
