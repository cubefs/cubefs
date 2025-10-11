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

	"github.com/cubefs/cubefs/util"
)

func TestMUPart_Bytes(t *testing.T) {
	var (
		id         uint16 = 1
		uploadTime        = time.Now().Local()
		md5               = util.RandomString(16, util.UpperLetter|util.Numeric)
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
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	parts1 := PartsFromBytes(nil)
	for i := 0; i < 100; i++ {
		part := &Part{
			ID:         uint16(i),
			UploadTime: time.Now().Local(),
			MD5:        util.RandomString(16, util.UpperLetter|util.Numeric),
			Size:       random.Uint64(),
			Inode:      random.Uint64(),
		}
		parts1.Insert(part, false)
	}
	var partsBytes []byte
	parts1.Bytes()
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
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	parts := PartsFromBytes(nil)
	for i := 0; i < 100; i++ {
		part := &Part{
			ID:         uint16(i),
			UploadTime: time.Now().Local(),
			MD5:        util.RandomString(16, util.UpperLetter|util.Numeric),
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
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	session1 := MultipartFromBytes(nil)

	me := NewMultipartExtend()
	me["oss::tag"] = "name=123&age456"
	me["oss::disposition"] = "attachment=file.txt"
	session1.extend = me
	for i := 0; i < 100; i++ {
		id := uint16(i)
		md5 := util.RandomString(16, util.UpperLetter|util.Numeric)
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

func TestMUMultipartExtend_Bytes(t *testing.T) {
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

// TestMUPart_Equal tests the Equal method of Part struct
func TestMUPart_Equal(t *testing.T) {
	part1 := &Part{
		ID:         1,
		UploadTime: time.Now(),
		MD5:        "testmd5",
		Size:       1024,
		Inode:      12345,
	}

	part2 := &Part{
		ID:         1,
		UploadTime: time.Now().Add(time.Hour), // Different time should not affect equality
		MD5:        "testmd5",
		Size:       1024,
		Inode:      12345,
	}

	part3 := &Part{
		ID:         2, // Different ID
		UploadTime: time.Now(),
		MD5:        "testmd5",
		Size:       1024,
		Inode:      12345,
	}

	if !part1.Equal(part2) {
		t.Fatalf("parts with same ID, MD5, Size, Inode should be equal")
	}

	if part1.Equal(part3) {
		t.Fatalf("parts with different ID should not be equal")
	}
}

// TestMUParts_Empty tests operations on empty Parts
func TestMUParts_Empty(t *testing.T) {
	parts := PartsFromBytes(nil)

	if parts.Len() != 0 {
		t.Fatalf("empty parts should have length 0, got %d", parts.Len())
	}

	// Test search on empty parts
	if _, found := parts.Search(1); found {
		t.Fatalf("search on empty parts should not find anything")
	}

	// Test remove on empty parts
	parts.Remove(1) // Should not panic

	// Test bytes on empty parts
	bytes, err := parts.Bytes()
	if err != nil {
		t.Fatalf("empty parts bytes should not fail: %v", err)
	}

	// Test deserialization of empty parts
	parts2 := PartsFromBytes(bytes)
	if parts2.Len() != 0 {
		t.Fatalf("deserialized empty parts should have length 0, got %d", parts2.Len())
	}
}

// TestMUParts_InsertReplace tests Insert with replace flag
func TestMUParts_InsertReplace(t *testing.T) {
	parts := PartsFromBytes(nil)

	part1 := &Part{
		ID:    1,
		MD5:   "md5_1",
		Size:  1024,
		Inode: 100,
	}

	part2 := &Part{
		ID:    1,
		MD5:   "md5_2", // Different MD5
		Size:  2048,    // Different size
		Inode: 200,     // Different inode
	}

	// Insert without replace
	success := parts.Insert(part1, false)
	if !success {
		t.Fatalf("first insert should succeed")
	}

	// Try to insert same ID without replace
	success = parts.Insert(part2, false)
	if success {
		t.Fatalf("insert same ID without replace should fail")
	}

	// Insert with replace
	success = parts.Insert(part2, true)
	if !success {
		t.Fatalf("insert with replace should succeed")
	}

	// Verify the part was replaced
	if parts.Len() != 1 {
		t.Fatalf("parts should have length 1, got %d", parts.Len())
	}

	foundPart, found := parts.Search(1)
	if !found {
		t.Fatalf("part should be found after replace")
	}

	if foundPart.MD5 != "md5_2" {
		t.Fatalf("part should be replaced with new MD5")
	}
}

// TestMUParts_UpdateOrStore tests UpdateOrStore method
func TestMUParts_UpdateOrStore(t *testing.T) {
	parts := PartsFromBytes(nil)

	part1 := &Part{
		ID:         1,
		MD5:        "md5_1",
		Size:       1024,
		Inode:      100,
		UploadTime: time.Now(),
	}

	part2 := &Part{
		ID:         1,
		MD5:        "md5_2",
		Size:       2048,
		Inode:      200,
		UploadTime: time.Now().Add(time.Hour), // Later time
	}

	part3 := &Part{
		ID:         1,
		MD5:        "md5_3",
		Size:       4096,
		Inode:      200, // Same inode as part1
		UploadTime: time.Now().Add(2 * time.Hour),
	}

	// First insert
	_, updated, conflict := parts.UpdateOrStore(part1)
	if updated || conflict {
		t.Fatalf("first insert should not be update or conflict")
	}

	// Update with different inode and later time
	oldInode, updated, conflict := parts.UpdateOrStore(part2)
	if !updated || conflict {
		t.Fatalf("update with later time should succeed")
	}
	if oldInode != 100 {
		t.Fatalf("old inode should be 100, got %d", oldInode)
	}

	// Try to update with same inode - this should not update and not conflict
	oldInode, updated, conflict = parts.UpdateOrStore(part3)
	if updated || conflict {
		t.Fatalf("update with same inode should not update or conflict, got updated=%v conflict=%v", updated, conflict)
	}
	if oldInode != 200 {
		t.Fatalf("old inode should be 200 (from part2), got %d", oldInode)
	}
}

// TestMUParts_Hash tests Hash method
func TestMUParts_Hash(t *testing.T) {
	parts := PartsFromBytes(nil)

	part1 := &Part{ID: 1, MD5: "md5_1", Size: 1024, Inode: 100}
	part2 := &Part{ID: 2, MD5: "md5_2", Size: 2048, Inode: 200}

	parts.Insert(part1, false)
	parts.Insert(part2, false)

	// Test hash for existing part
	if !parts.Hash(part1) {
		t.Fatalf("hash should find existing part")
	}

	// Test hash for non-existing part
	part3 := &Part{ID: 3, MD5: "md5_3", Size: 4096, Inode: 300}
	if parts.Hash(part3) {
		t.Fatalf("hash should not find non-existing part")
	}
}

// TestMUMultipartExtend_Empty tests empty MultipartExtend
func TestMUMultipartExtend_Empty(t *testing.T) {
	me := NewMultipartExtend()

	bytes, err := me.Bytes()
	if err != nil {
		t.Fatalf("empty extend bytes should not fail: %v", err)
	}

	me2 := MultipartExtendFromBytes(bytes)
	// According to the implementation, empty extend returns nil
	if me2 != nil {
		t.Fatalf("deserialized empty extend should be nil, got %v", me2)
	}

	// Test with non-empty extend to ensure the method works
	me["test"] = "value"
	bytes, err = me.Bytes()
	if err != nil {
		t.Fatalf("non-empty extend bytes should not fail: %v", err)
	}

	me3 := MultipartExtendFromBytes(bytes)
	if me3 == nil {
		t.Fatalf("deserialized non-empty extend should not be nil")
	}

	if len(me3) != 1 || me3["test"] != "value" {
		t.Fatalf("deserialized extend should have correct content")
	}
}

// TestMUMultipartExtend_Large tests MultipartExtend with large data
func TestMUMultipartExtend_Large(t *testing.T) {
	me := NewMultipartExtend()

	// Add many key-value pairs
	for i := 0; i < 1000; i++ {
		key := util.RandomString(20, util.UpperLetter|util.LowerLetter|util.Numeric)
		value := util.RandomString(100, util.UpperLetter|util.LowerLetter|util.Numeric)
		me[key] = value
	}

	bytes, err := me.Bytes()
	if err != nil {
		t.Fatalf("large extend bytes should not fail: %v", err)
	}

	me2 := MultipartExtendFromBytes(bytes)
	if !reflect.DeepEqual(me, me2) {
		t.Fatalf("large extend deserialization mismatch")
	}

	t.Logf("large extend encoded length: %v", len(bytes))
}

// TestMUMultipart_Concurrent tests concurrent access to Multipart
func TestMUMultipart_Concurrent(t *testing.T) {
	multipart := &Multipart{
		id:       "test-session",
		key:      "test-key",
		initTime: time.Now(),
		parts:    PartsFromBytes(nil),
	}

	// Concurrent goroutines
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			part := &Part{
				ID:         uint16(id),
				MD5:        util.RandomString(16, util.UpperLetter|util.Numeric),
				Size:       uint64(1024 + id),
				Inode:      uint64(1000 + id),
				UploadTime: time.Now(),
			}

			multipart.InsertPart(part, false)
			multipart.Parts() // Test read access
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	parts := multipart.Parts()
	if len(parts) != 10 {
		t.Fatalf("expected 10 parts, got %d", len(parts))
	}
}

// TestMUMultipart_EdgeCases tests edge cases for Multipart
func TestMUMultipart_EdgeCases(t *testing.T) {
	// Test with nil parts
	multipart := &Multipart{
		id:       "test-session",
		key:      "test-key",
		initTime: time.Now(),
		parts:    nil,
	}

	// This should initialize parts
	multipart.InsertPart(&Part{ID: 1, MD5: "test", Size: 1024, Inode: 100}, false)

	parts := multipart.Parts()
	if len(parts) != 1 {
		t.Fatalf("nil parts should be initialized, expected 1 part, got %d", len(parts))
	}

	// Test UpdateOrStorePart with nil parts
	multipart2 := &Multipart{
		id:       "test-session-2",
		key:      "test-key-2",
		initTime: time.Now(),
		parts:    nil,
	}

	oldInode, updated, conflict := multipart2.UpdateOrStorePart(&Part{ID: 1, MD5: "test", Size: 1024, Inode: 100})
	if updated || conflict {
		t.Fatalf("first UpdateOrStorePart should not be update or conflict")
	}
	if oldInode != 0 {
		t.Fatalf("old inode should be 0 for new part, got %d", oldInode)
	}
}

// TestMUMultipart_BytesEmpty tests Multipart serialization with empty data
func TestMUMultipart_BytesEmpty(t *testing.T) {
	multipart := &Multipart{
		id:       "",
		key:      "",
		initTime: time.Time{},
		parts:    PartsFromBytes(nil),
		extend:   NewMultipartExtend(),
	}

	bytes, err := multipart.Bytes()
	if err != nil {
		t.Fatalf("empty multipart bytes should not fail: %v", err)
	}

	multipart2 := MultipartFromBytes(bytes)
	if multipart2.id != "" || multipart2.key != "" {
		t.Fatalf("deserialized empty multipart should have empty id and key")
	}

	if multipart2.parts.Len() != 0 {
		t.Fatalf("deserialized empty multipart should have empty parts")
	}

	if len(multipart2.extend) != 0 {
		t.Fatalf("deserialized empty multipart should have empty extend")
	}
}

// TestMUParts_Sort tests sorting functionality
func TestMUParts_Sort(t *testing.T) {
	parts := PartsFromBytes(nil)

	// Insert parts in reverse order
	for i := 10; i >= 1; i-- {
		part := &Part{
			ID:    uint16(i),
			MD5:   util.RandomString(16, util.UpperLetter|util.Numeric),
			Size:  uint64(1024 * i),
			Inode: uint64(1000 + i),
		}
		parts.Insert(part, false)
	}

	// Verify parts are sorted by ID
	for i := 0; i < parts.Len()-1; i++ {
		if parts[i].ID >= parts[i+1].ID {
			t.Fatalf("parts should be sorted by ID, found %d >= %d", parts[i].ID, parts[i+1].ID)
		}
	}
}

// TestMUParts_RemoveNonExistent tests removing non-existent parts
func TestMUParts_RemoveNonExistent(t *testing.T) {
	parts := PartsFromBytes(nil)

	// Add some parts
	for i := 1; i <= 5; i++ {
		part := &Part{
			ID:    uint16(i),
			MD5:   util.RandomString(16, util.UpperLetter|util.Numeric),
			Size:  uint64(1024 * i),
			Inode: uint64(1000 + i),
		}
		parts.Insert(part, false)
	}

	originalLen := parts.Len()

	// Try to remove non-existent part
	parts.Remove(999)

	if parts.Len() != originalLen {
		t.Fatalf("removing non-existent part should not change length")
	}
}

// TestMUMultipartExtend_SpecialCharacters tests MultipartExtend with special characters
func TestMUMultipartExtend_SpecialCharacters(t *testing.T) {
	me := NewMultipartExtend()

	// Add keys and values with special characters
	me["key with spaces"] = "value with spaces"
	me["key:with:colons"] = "value:with:colons"
	me["key=with=equals"] = "value=with=equals"
	me["key&with&ampersands"] = "value&with&ampersands"
	me["key\nwith\nnewlines"] = "value\nwith\nnewlines"
	me["key\twith\ttabs"] = "value\twith\ttabs"

	bytes, err := me.Bytes()
	if err != nil {
		t.Fatalf("special characters extend bytes should not fail: %v", err)
	}

	me2 := MultipartExtendFromBytes(bytes)
	if !reflect.DeepEqual(me, me2) {
		t.Fatalf("special characters extend deserialization mismatch")
	}
}
