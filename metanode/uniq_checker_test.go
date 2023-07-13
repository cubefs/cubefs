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
	"testing"
)

func TestLegal(t *testing.T) {
	checker := newUniqChecker()
	for i := 1; i <= 10; i++ {
		if !checker.legalIn(uint64(i)) {
			t.Errorf("failed")
		}
	}
	if checker.legalIn(1) {
		t.Errorf("failed, %v", checker.op)
	}
}

func TestOpQueue(t *testing.T) {
	q := newUniqOpQueue()
	for i := uint64(0); i < 10000; i++ {
		q.append(&uniqOp{
			uniqid: i,
		})
	}

	cnt := uint64(0)
	q.scan(func(op *uniqOp) bool {
		if op.uniqid != cnt {
			t.Fatalf("op queue scan failed")
		}
		cnt++
		return true
	})
	if cnt != 10000 {
		t.Fatalf("scan failed %v", cnt)
	}

	op := q.index(4567)
	if op.uniqid != 4567 {
		t.Fatalf("q.index 4567 failed")
	}

	q.truncate(4567)

	op = q.index(0)
	if op.uniqid != 4568 {
		t.Fatalf("q.index 4568 failed")
	}

	if q.len() != 10000-4567-1 {
		t.Fatalf("op queue trancate failed")
	}

	q.scan(func(op *uniqOp) bool {
		if op.uniqid != 4568 {
			t.Fatalf("op queue trancate scan failed")
		}
		return false
	})

	clone := q.clone()
	for i := uint64(10000); i < 20000; i++ {
		q.append(&uniqOp{uniqid: i})
	}

	if q.len()-clone.len() != 10000 || q.index(1234).uniqid != clone.index(1234).uniqid {
		t.Fatalf("op queue clone failed")
	}

	q.reset()
	if q.len() != 0 || len(q.cur.s) != 0 || len(q.ss) != 1 {
		t.Fatalf("op queue trancate failed")
	}

}

func TestClone(t *testing.T) {
	checker := newUniqChecker()
	for i := 1; i <= 10000; i++ {
		checker.legalIn(uint64(i))
	}

	checker1 := checker.clone()
	if len(checker1.op) != 0 || checker.inQue.len() != checker1.inQue.len() {
		t.Errorf("failed")
	}

	i := 0
	checker.inQue.scan(func(op *uniqOp) bool {
		if op.uniqid != checker1.inQue.index(i).uniqid || op.atime != checker1.inQue.index(i).atime {
			t.Errorf("failed")
			return false
		}
		i++
		return true
	})

}

func TestMarshal(t *testing.T) {
	checker := newUniqChecker()
	for i := 1; i <= 10000; i++ {
		checker.legalIn(uint64(i))
	}

	bts, _, _ := checker.Marshal()
	checker1 := newUniqChecker()
	checker1.UnMarshal(bts)

	if len(checker.op) != len(checker1.op) || checker.inQue.len() != checker1.inQue.len() {
		t.Errorf("failed")
	}

	i := 0
	checker.inQue.scan(func(v *uniqOp) bool {
		if v.uniqid != checker1.inQue.index(i).uniqid || v.atime != checker1.inQue.index(i).atime {
			t.Errorf("failed, id(%v, %v), atime(%v, %v)", v.uniqid, checker1.inQue.index(i).uniqid, v.atime, checker1.inQue.index(i).atime)
			return false
		}

		if _, ok := checker1.op[v.uniqid]; !ok {
			t.Errorf("failed, %v, %v", checker.op[v.uniqid], checker1.op[v.uniqid])
			return false
		}
		i++
		return true
	})
}
