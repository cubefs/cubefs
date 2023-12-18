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

package lcnode

import (
	"context"
	"crypto/md5"
	"io"
	"io/ioutil"

	"github.com/cubefs/cubefs/proto"
)

type MockExtentClient struct {
	data []byte
}

func NewMockExtentClient() *MockExtentClient {
	return &MockExtentClient{}
}

func (m *MockExtentClient) OpenStream(inode uint64, openForWrite bool, isCache bool) error {
	return nil
}

func (m *MockExtentClient) CloseStream(inode uint64) error {
	return nil
}

func (m *MockExtentClient) Read(inode uint64, data []byte, offset int, size int, storageClass uint32, isMigration bool) (read int, err error) {
	if isMigration {
		for i := 0; i < size; i++ {
			data[i] = m.data[i]
		}
		return len(data), io.EOF
	}
	for i := 0; i < size; i++ {
		data[i] = 'a'
	}
	return len(data), io.EOF
}

func (m *MockExtentClient) Write(inode uint64, offset int, data []byte, flags int, checkFunc func() error, storageClass uint32, isMigration bool) (write int, err error) {
	m.data = data
	return
}

func (m *MockExtentClient) Flush(inode uint64) error {
	return nil
}

func (m *MockExtentClient) Close() error {
	return nil
}

type MockEbsClient struct {
	data []byte
}

func NewMockEbsClient() *MockEbsClient {
	return &MockEbsClient{}
}

func (m *MockEbsClient) Put(ctx context.Context, volName string, f io.Reader, size uint64) (oek proto.ObjExtentKey, _md5 []byte, err error) {
	m.data, _ = ioutil.ReadAll(f)
	md5Hash := md5.New()
	md5Hash.Write(m.data)
	_md5 = md5Hash.Sum(nil)
	return
}

func (m *MockEbsClient) Get(ctx context.Context, volName string, offset uint64, size uint64, oek proto.ObjExtentKey) (body io.ReadCloser, err error) {
	return
}
