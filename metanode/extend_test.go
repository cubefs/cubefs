// Copyright 2018 The Chubao Authors.
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

func TestExtend_Bytes(t *testing.T) {
	var err error
	const numSamples = 100

	var random = rand.New(rand.NewSource(time.Now().UnixNano()))

	extends := make([]*Extend, numSamples)
	for i := 0; i < numSamples; i++ {
		extend := NewExtend(random.Uint64())
		extend.Put([]byte("msg"), []byte(stringutil.RandomString(16, stringutil.Numeric|stringutil.LowerLetter|stringutil.UpperLetter)))
		extends[i] = extend
	}

	outputs := make([][]byte, numSamples)
	for i := 0; i < numSamples; i++ {
		if outputs[i], err = extends[i].Bytes(); err != nil {
			t.Fatalf("encode extend to bytes fail cause: %v", err)
		}
	}

	// validate result
	for i := 0; i < numSamples; i++ {
		var e *Extend
		if e, err = NewExtendFromBytes(outputs[i]); err != nil {
			t.Fatalf("decode bytes to extend fail cause: %v", err)
		}
		if !reflect.DeepEqual(e, extends[i]) {
			t.Fatalf("result mismatch")
		}
	}

}
