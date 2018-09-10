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

package blob

import (
	"testing"
	"time"

	"github.com/tiglabs/containerfs/util/log"
)

const (
	TestVolName    = "intest"
	TestMasterAddr = "10.196.31.173:80,10.196.31.141:80,10.196.30.200:80"
	TestLogPath    = "testlog"
)

var gBlobClient *BlobClient

func init() {
	_, err := log.InitLog(TestLogPath, "Blob_UT", log.DebugLevel)
	if err != nil {
		panic(err)
	}

	bc, err := NewBlobClient(TestVolName, TestMasterAddr)
	if err != nil {
		panic(err)
	}
	gBlobClient = bc
}

func TestGetVol(t *testing.T) {
	dplist := gBlobClient.partitions.List()
	for _, dp := range dplist {
		t.Logf("%v", dp)
	}
	time.Sleep(2 * time.Second)
}

func TestWrite(t *testing.T) {
	data := []byte("1234")
	key, err := gBlobClient.Write(data)
	if err != nil {
		t.Errorf("Write: data(%v) err(%v)", string(data), err)
	}
	t.Logf("Write: key(%v)", key)

	time.Sleep(2 * time.Second)
}
