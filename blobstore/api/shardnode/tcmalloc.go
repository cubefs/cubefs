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

import "context"

func (c *Client) TCMallocStats(ctx context.Context, host string, args TCMallocArgs) (ret TCMallocRet, err error) {
	err = c.doRequest(ctx, host, "/tcmalloc/stats", &args, &ret)
	return
}

func (c *Client) TCMallocFree(ctx context.Context, host string, args TCMallocArgs) (ret TCMallocRet, err error) {
	err = c.doRequest(ctx, host, "/tcmalloc/free", &args, &ret)
	return
}

func (c *Client) TCMallocRate(ctx context.Context, host string, args TCMallocArgs) (ret TCMallocRet, err error) {
	err = c.doRequest(ctx, host, "/tcmalloc/rate", &args, &ret)
	return
}
