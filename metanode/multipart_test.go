// Copyright 2018 The CubeFS Authors.
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

package metanode

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	stringutil "github.com/chubaofs/chubaofs/util/string"
)

func TestMUPart_Bytes(t *testing.T) {
	var (
		id         uint16 = 1
		uploadTime        = time.Now().Local()
		md5               = stringutil.RandomString(16, stringutil.UpperLetter|stringutil.Numeric)
		size       uint64 = 65536
		inode      uint64 = 12345
		err        error
	)
	part1 := &Part{
		ID:         id,
		UploadTime: uploadTime,
		MD5:        md5,
		Size:       size,
		Inode:      inode,
	}
	var partBytes []byte
	if partBytes, err = part1.Bytes(); err != nil {
		t.Fatalf("get bytes of part fail cause: %v", err)
	}
	part2 := PartFromBytes(partBytes)
	if !reflect.DeepEqual(part1, part2) {
		t.Fatalf("result mismatch:\n\tpart1:%v\n\tpart2:%v", part1, part2)
	}
	t.Logf("encoded length: %v", len(partBytes))
}

func TestMUParts_Bytes(t *testing.T) {
	var err error
	var random = rand.New(rand.NewSource(time.Now().UnixNano()))
	var parts1 = PartsFromBytes(nil)
	for i := 0; i < 100; i++ {
		part := &Part{
			ID:         uint16(i),
			UploadTime: time.Now().Local(),
			MD5:        stringutil.RandomString(16, stringutil.UpperLetter|stringutil.Numeric),
			Size:       random.Uint64(),
			Inode:      random.Uint64(),
		}
		parts1.Insert(part, false)
	}
	var partsBytes []byte
	partsBytes, err = parts1.Bytes()
	if partsBytes, err = parts1.Bytes(); err != nil {
		t.Fatalf("get bytes of part fail cause: %v", err)
	}
	parts2 := PartsFromBytes(partsBytes)
	if !reflect.DeepEqual(parts1, parts2) {
		t.Fatalf("result mismatch:\n\tpart1:%v\n\tpart2:%v", parts1, parts2)
	}
	t.Logf("encoded length: %v", len(partsBytes))
}

func TestMUParts_Modify(t *testing.T) {
	var random = rand.New(rand.NewSource(time.Now().UnixNano()))
	var parts = PartsFromBytes(nil)
	for i := 0; i < 100; i++ {
		part := &Part{
			ID:         uint16(i),
			UploadTime: time.Now().Local(),
			MD5:        stringutil.RandomString(16, stringutil.UpperLetter|stringutil.Numeric),
			Size:       random.Uint64(),
			Inode:      random.Uint64(),
		}
		parts.Insert(part, false)
	}
	if parts.Len() != 100 {
		t.Fatalf("parts length mismatch: except 100 actual %v", parts.Len())
	}
	// validate before modify
	if _, found := parts.Search(0); !found {
		t.Fatalf("part id[0] not found before modify")
	}
	if _, found := parts.Search(50); !found {
		t.Fatalf("part id[50] not found before modify")
	}
	if _, found := parts.Search(99); !found {
		t.Fatalf("part id[99] not found before modify")
	}
	// modify
	parts.Remove(0)
	parts.Remove(50)
	parts.Remove(99)
	if parts.Len() != 97 {
		t.Fatalf("parts length mismatch: expect 97 actual %v", parts.Len())
	}
	// validate after modify
	if _, found := parts.Search(0); found {
		t.Fatalf("part id[0] not found before modify")
	}
	if _, found := parts.Search(50); found {
		t.Fatalf("part id[50] not found before modify")
	}
	if _, found := parts.Search(99); found {
		t.Fatalf("part id[99] not found before modify")
	}
}

func TestMUSession_Bytes(t *testing.T) {
	var err error
	var random = rand.New(rand.NewSource(time.Now().UnixNano()))
	var session1 = MultipartFromBytes(nil)

	me := NewMultipartExtend()
	me["oss::tag"] = "name=123&age456"
	me["oss::disposition"] = "attachment=file.txt"
	session1.extend = me
	for i := 0; i < 100; i++ {
		id := uint16(i)
		md5 := stringutil.RandomString(16, stringutil.UpperLetter|stringutil.Numeric)
		size := random.Uint64()
		inode := random.Uint64()
		session1.InsertPart(&Part{
			ID:         id,
			MD5:        md5,
			Size:       size,
			Inode:      inode,
			UploadTime: time.Now().Local(),
		}, false)
	}
	var sessionBytes []byte
	sessionBytes, err = session1.Bytes()
	if err != nil {
		t.Fatalf("encode session to bytes fail caue: %v", err)
	}
	session2 := MultipartFromBytes(sessionBytes)
	if !reflect.DeepEqual(session1, session2) {
		t.Fatalf("result mismatch:\n\tsession1:%v\n\tsession2:%v", session1, session2)
	}
	t.Logf("encoded session length: %v", len(sessionBytes))
}

func TestMultipartExtend_Bytes(t *testing.T) {
	me := NewMultipartExtend()
	me["oss::tag"] = "name=123&age456"
	me["oss::disposition"] = "attachment=file.txt"
	bytes, err := me.Bytes()
	if err != nil {
		t.Errorf("Encode multipart extend fail cause : %v", err)
	}
	me2 := MultipartExtendFromBytes(bytes)
	if !reflect.DeepEqual(me, me2) {
		t.Fatalf("result mismatch:\n\tme1:%v\n\tme2:%v", me, me2)
	}
}
