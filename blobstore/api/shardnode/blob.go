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

func (c *Client) CreateBlob(ctx context.Context, host string, args CreateBlobArgs) (CreateBlobRet, error) {
	return CreateBlobRet{}, nil
}

func (c *Client) DeleteBlob(ctx context.Context, host string, args DeleteBlobArgs) error {
	return nil
}

func (c *Client) GetBlob(ctx context.Context, host string, args GetBlobArgs) (GetBlobRet, error) {
	return GetBlobRet{}, nil
}

func (c *Client) SealBlob(ctx context.Context, host string, args SealBlobArgs) error {
	return nil
}

func (c *Client) ListBlob(ctx context.Context, host string, args ListBlobArgs) (ListBlobRet, error) {
	return ListBlobRet{}, nil
}
