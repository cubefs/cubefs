// Copyright 2022 The CubeFS Authors.
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

package normaldb

import "C"

import (
	"errors"

	"github.com/cubefs/cubefs/blobstore/common/kvstore"
)

type ConfigTable struct {
	tpl kvstore.KVTable
}

type ConfigRet struct {
	Value []byte `json:"value"`
}

func OpenConfigTable(db kvstore.KVStore) (*ConfigTable, error) {
	if db == nil {
		return nil, errors.New("OpenConfigTable failed: db is nil")
	}
	return &ConfigTable{db.Table("config")}, nil
}

func (c *ConfigTable) Get(key string) (value string, err error) {
	val, err := c.tpl.Get([]byte(key))
	return string(val), err
}

func (c *ConfigTable) Delete(key string) (err error) {
	err = c.tpl.Delete([]byte(key))
	return
}

func (c *ConfigTable) List() (configAll map[string]string, err error) {
	s := c.tpl.NewSnapshot()
	i := c.tpl.NewIterator(s)
	tbConfigMap := make(map[string]string)

	for i.SeekToFirst(); i.Valid(); i.Next() {
		if i.Err() != nil {
			return nil, i.Err()
		}
		ik := i.Key().Data()
		iv := i.Value().Data()
		tbConfigMap[string(ik)] = string(iv)
		i.Key().Free()
		i.Value().Free()
	}
	i.Close()
	c.tpl.ReleaseSnapshot(s)

	return tbConfigMap, err
}

func (c *ConfigTable) Update(key, value []byte) (err error) {
	err = c.tpl.Put(kvstore.KV{Key: key, Value: value})
	return
}
