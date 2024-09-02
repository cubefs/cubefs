// Copyright 2024 The CubeFS Authors.
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

package shardnode

import (
	"context"
)

func (c *Client) AddItem(ctx context.Context, host string, args InsertItemArgs) error {
	return c.doRequest(ctx, host, "/item/insert", &args, nil)
}

func (c *Client) UpdateItem(ctx context.Context, host string, args UpdateItemArgs) error {
	return c.doRequest(ctx, host, "/item/update", &args, nil)
}

func (c *Client) DeleteItem(ctx context.Context, host string, args DeleteItemArgs) error {
	return c.doRequest(ctx, host, "/item/delete", &args, nil)
}

func (c *Client) GetItem(ctx context.Context, host string, args GetItemArgs) (ret Item, err error) {
	err = c.doRequest(ctx, host, "/item/get", &args, &ret)
	return
}

func (c *Client) ListItem(ctx context.Context, host string, args ListItemArgs) (ret ListItemRet, err error) {
	err = c.doRequest(ctx, host, "/item/list", &args, &ret)
	return
}
