// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"reflect"
	"testing"

	"github.com/tiglabs/raft/proto"
)

func TestMaybeLastIndex(t *testing.T) {
	tests := []struct {
		entries []*proto.Entry
		offset  uint64
		wok     bool
		windex  uint64
	}{
		// last in entries
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}}, 5, true, 5,
		},
		// empty unstable
		{
			[]*proto.Entry{}, 0, false, 0,
		},
	}

	for i, tt := range tests {
		u := unstable{
			entries: tt.entries,
			offset:  tt.offset,
		}
		index, ok := u.maybeLastIndex()
		if ok != tt.wok {
			t.Errorf("#%d: ok = %t, want %t", i, ok, tt.wok)
		}
		if index != tt.windex {
			t.Errorf("#%d: index = %d, want %d", i, index, tt.windex)
		}
	}
}

func TestUnstableMaybeTerm(t *testing.T) {
	tests := []struct {
		entries []*proto.Entry
		offset  uint64
		index   uint64
		wok     bool
		wterm   uint64
	}{
		// term from entries
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}}, 5, 5, true, 1,
		},
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}}, 5, 6, false, 0,
		},
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}}, 5, 4, false, 0,
		},
		{
			[]*proto.Entry{}, 0, 5, false, 0,
		},
	}

	for i, tt := range tests {
		u := unstable{
			entries: tt.entries,
			offset:  tt.offset,
		}
		term, ok := u.maybeTerm(tt.index)
		if ok != tt.wok {
			t.Errorf("#%d: ok = %t, want %t", i, ok, tt.wok)
		}
		if term != tt.wterm {
			t.Errorf("#%d: term = %d, want %d", i, term, tt.wterm)
		}
	}
}

func TestUnstableRestore(t *testing.T) {
	u := unstable{
		entries: []*proto.Entry{&proto.Entry{Index: 5, Term: 1}},
		offset:  5,
	}
	u.restore(6)

	if u.offset != 7 {
		t.Errorf("offset = %d, want %d", u.offset, 7)
	}
	if len(u.entries) != 0 {
		t.Errorf("len = %d, want 0", len(u.entries))
	}
}

func TestUnstableStableTo(t *testing.T) {
	tests := []struct {
		entries     []*proto.Entry
		offset      uint64
		index, term uint64
		woffset     uint64
		wlen        int
	}{
		{
			[]*proto.Entry{}, 0, 5, 1, 0, 0,
		},
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}}, 5, 5, 1, 6, 0,
		},
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}, &proto.Entry{Index: 6, Term: 1}}, 5,
			5, 1, 6, 1,
		},
		{
			[]*proto.Entry{&proto.Entry{Index: 6, Term: 2}}, 6, 6, 1, 6, 1,
		},
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}}, 5, 4, 1, 5, 1,
		},
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}}, 5, 4, 2, 5, 1,
		},
	}

	for i, tt := range tests {
		u := unstable{
			entries: tt.entries,
			offset:  tt.offset,
		}
		u.stableTo(tt.index, tt.term)
		if u.offset != tt.woffset {
			t.Errorf("#%d: offset = %d, want %d", i, u.offset, tt.woffset)
		}
		if len(u.entries) != tt.wlen {
			t.Errorf("#%d: len = %d, want %d", i, len(u.entries), tt.wlen)
		}
	}
}

func TestUnstableTruncateAndAppend(t *testing.T) {
	tests := []struct {
		entries  []*proto.Entry
		offset   uint64
		toappend []*proto.Entry

		woffset  uint64
		wentries []*proto.Entry
	}{
		// append to the end
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}}, 5,
			[]*proto.Entry{&proto.Entry{Index: 6, Term: 1}, &proto.Entry{Index: 7, Term: 1}},
			5, []*proto.Entry{&proto.Entry{Index: 5, Term: 1}, &proto.Entry{Index: 6, Term: 1}, &proto.Entry{Index: 7, Term: 1}},
		},
		// replace the unstable entries
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}}, 5,
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 2}, &proto.Entry{Index: 6, Term: 2}},
			5, []*proto.Entry{&proto.Entry{Index: 5, Term: 2}, &proto.Entry{Index: 6, Term: 2}},
		},
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}}, 5,
			[]*proto.Entry{&proto.Entry{Index: 4, Term: 2}, &proto.Entry{Index: 5, Term: 2}, &proto.Entry{Index: 6, Term: 2}},
			4, []*proto.Entry{&proto.Entry{Index: 4, Term: 2}, &proto.Entry{Index: 5, Term: 2}, &proto.Entry{Index: 6, Term: 2}},
		},
		// truncate the existing entries and append
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}, &proto.Entry{Index: 6, Term: 1}, &proto.Entry{Index: 7, Term: 1}}, 5,
			[]*proto.Entry{&proto.Entry{Index: 6, Term: 2}},
			5, []*proto.Entry{&proto.Entry{Index: 5, Term: 1}, &proto.Entry{Index: 6, Term: 2}},
		},
		{
			[]*proto.Entry{&proto.Entry{Index: 5, Term: 1}, &proto.Entry{Index: 6, Term: 1}, &proto.Entry{Index: 7, Term: 1}}, 5,
			[]*proto.Entry{&proto.Entry{Index: 7, Term: 2}, &proto.Entry{Index: 8, Term: 2}},
			5, []*proto.Entry{&proto.Entry{Index: 5, Term: 1}, &proto.Entry{Index: 6, Term: 1}, &proto.Entry{Index: 7, Term: 2}, &proto.Entry{Index: 8, Term: 2}},
		},
	}

	for i, tt := range tests {
		u := unstable{
			entries: tt.entries,
			offset:  tt.offset,
		}
		u.truncateAndAppend(tt.toappend)
		if u.offset != tt.woffset {
			t.Errorf("#%d: offset = %d, want %d", i, u.offset, tt.woffset)
		}
		if !reflect.DeepEqual(u.entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, u.entries, tt.wentries)
		}
	}
}
