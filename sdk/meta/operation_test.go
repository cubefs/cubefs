// Copyright 2018 The ChuBao Authors.
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

package meta

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
)

const (
	TestVolName    = "metatest"
	TestMasterAddr = "localhost"
	TestHttpPort   = "9900"
)

const (
	PartitionNotFound = 0
)

type testcase struct {
	inode       uint64
	partitionID uint64
}

var globalNV *VolumeView

var globalMP = []MetaPartition{
	{1, 1, 100, nil, ""},
	{2, 101, 200, nil, ""},
	{3, 210, 300, nil, ""},
	{4, 301, 400, nil, ""},
}

var globalTests = []testcase{
	{1, 1},
	{100, 1},
	{101, 2},
	{200, 2},
	{201, PartitionNotFound},
	{209, PartitionNotFound},
	{210, 3},
	{220, 3},
	{300, 3},
	{301, 4},
	{400, 4},
	{401, PartitionNotFound},
	{500, PartitionNotFound},
}

var extraMP = []MetaPartition{
	{4, 320, 390, nil, ""},
	{6, 600, 700, nil, ""},
}

var extraTests = []testcase{
	{301, PartitionNotFound},
	{319, PartitionNotFound},
	{320, 4},
	{390, 4},
	{391, PartitionNotFound},
	{400, PartitionNotFound},
	{599, PartitionNotFound},
	{600, 6},
	{700, 6},
	{701, PartitionNotFound},
}

var getNextTests = []testcase{
	{0, 1},
	{1, 2},
	{101, 3},
	{301, 4},
	{320, 6},
	{600, PartitionNotFound},
	{700, PartitionNotFound},
}

func init() {
	globalNV = &VolumeView{
		VolName:        TestVolName,
		MetaPartitions: make([]*MetaPartition, 0),
	}

	globalNV.update(globalMP)

	go func() {
		http.HandleFunc("/client/vol", handleClientNS)
		err := http.ListenAndServe(":"+TestHttpPort, nil)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Done!")
		}
	}()
}

func (nv *VolumeView) update(partitions []MetaPartition) {
	for _, p := range partitions {
		mp := newMetaPartition(p.PartitionID, p.Start, p.End)
		nv.MetaPartitions = append(nv.MetaPartitions, mp)
	}
}

func newMetaPartition(id, start, end uint64) *MetaPartition {
	return &MetaPartition{
		PartitionID: id,
		Start:       start,
		End:         end,
	}
}

func handleClientNS(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("name")
	if strings.Compare(name, globalNV.VolName) != 0 {
		http.Error(w, "No such volname!", http.StatusBadRequest)
		return
	}

	data, err := json.Marshal(globalNV)
	if err != nil {
		http.Error(w, "JSON marshal failed!", http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

func TestGetVolumeView(t *testing.T) {
	resp, err := http.Get("http://" + TestMasterAddr + ":" + TestHttpPort + MetaPartitionViewURL + TestVolName)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	nv := &VolumeView{}
	err = json.Unmarshal(data, nv)
	if err != nil {
		t.Fatal(err)
	}

	for _, mp := range nv.MetaPartitions {
		t.Logf("%v", *mp)
	}
}

func TestMetaPartitionCreate(t *testing.T) {
	mw, err := NewMetaWrapper(TestVolName, TestMasterAddr+":"+TestHttpPort)
	if err != nil {
		t.Fatal(err)
	}

	mw.RLock()
	for _, mp := range mw.partitions {
		t.Logf("%v", *mp)
	}
	mw.RUnlock()
}

func TestMetaPartitionFind(t *testing.T) {
	mw, err := NewMetaWrapper(TestVolName, TestMasterAddr+":"+TestHttpPort)
	if err != nil {
		t.Fatal(err)
	}
	doTest(t, mw, 0, globalTests)
}

func TestMetaPartitionUpdate(t *testing.T) {
	mw, err := NewMetaWrapper(TestVolName, TestMasterAddr+":"+TestHttpPort)
	if err != nil {
		t.Fatal(err)
	}

	globalNV.update(extraMP)
	err = mw.UpdateMetaPartitions()
	if err != nil {
		t.Fatal(err)
	}

	mw.RLock()
	for _, mp := range mw.partitions {
		t.Logf("%v", *mp)
	}
	mw.RUnlock()

	doTest(t, mw, 0, extraTests)
}

func TestGetNextMetaPartition(t *testing.T) {
	mw, err := NewMetaWrapper(TestVolName, TestMasterAddr+":"+TestHttpPort)
	if err != nil {
		t.Fatal(err)
	}

	doTest(t, mw, 1, getNextTests)
}

func doTest(t *testing.T, mw *MetaWrapper, op int, tests []testcase) {
	var mp *MetaPartition
	for _, tc := range tests {
		switch op {
		case 1:
			mp = mw.getNextPartition(tc.inode)
		default:
			mp = mw.getPartitionByInode(tc.inode)
		}
		if !checkResult(mp, tc.partitionID) {
			t.Fatalf("inode = %v, %v", tc.inode, mp)
		}
		t.Logf("PASS: Finding inode = %v , %v", tc.inode, mp)
	}
}

func checkResult(mp *MetaPartition, partitionID uint64) bool {
	var toCompare uint64
	if mp == nil {
		toCompare = PartitionNotFound
	} else {
		toCompare = mp.PartitionID
	}
	return toCompare == partitionID
}
