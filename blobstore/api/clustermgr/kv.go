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

package clustermgr

import (
	"context"
	"fmt"
)

type GetKvArgs struct {
	Key string `json:"key"`
}

type GetKvRet struct {
	Value []byte `json:"value"`
}

type SetKvArgs struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type DeleteKvArgs struct {
	Key string `json:"key"`
}

type ListKvOpts struct {
	Prefix string `json:"prefix,omitempty"`
	Marker string `json:"marker,omitempty"`
	Count  int    `json:"count,omitempty"`
}

type KeyValue struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type ListKvRet struct {
	Kvs    []*KeyValue `json:"kvs"`
	Marker string      `json:"marker"`
}

func (c *Client) GetKV(ctx context.Context, key string) (ret GetKvRet, err error) {
	err = c.GetWith(ctx, "/kv/get/"+key, &ret)
	return
}

func (c *Client) DeleteKV(ctx context.Context, key string) (err error) {
	err = c.PostWith(ctx, "/kv/delete/"+key, nil, nil)
	return
}

func (c *Client) SetKV(ctx context.Context, key string, value []byte) (err error) {
	err = c.PostWith(ctx, "/kv/set/"+key, nil, &SetKvArgs{Key: key, Value: value})
	return
}

func (c *Client) ListKV(ctx context.Context, args *ListKvOpts) (ret ListKvRet, err error) {
	err = c.GetWith(ctx, fmt.Sprintf(
		"/kv/list?prefix=%s&marker=%s&count=%d",
		args.Prefix,
		args.Marker,
		args.Count,
	), &ret)
	return
}
