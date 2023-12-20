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

package httpclient

import (
	"net/http"

	"github.com/cubefs/cubefs/proto"
)

type FlashNode interface {
	EvictVol(volume string) error
	EvictAll() error
	Stat() (proto.FlashNodeStat, error)
}

type flashNode struct {
	client *Client
}

func (c *Client) FlashNode() FlashNode {
	return &flashNode{client: c}
}

func (f *flashNode) EvictVol(volume string) error {
	r := newRequest(http.MethodPost, "/evictVol")
	r.params.Add("volume", volume)
	return f.client.serveWith(r, nil)
}

func (f *flashNode) EvictAll() error {
	return f.client.serveWith(newRequest(http.MethodPost, "/evictAll"), nil)
}

func (f *flashNode) Stat() (st proto.FlashNodeStat, err error) {
	err = f.client.serveWith(newRequest(http.MethodGet, "/stat"), &st)
	return
}
