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

package atomicutil

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
)

type Bool struct {
	val uint32
}

func (b *Bool) Load() (v bool) {
	v = atomic.LoadUint32(&b.val) == 1
	return
}

func (b *Bool) Store(v bool) {
	val := uint32(0)
	if v {
		val = 1
	}
	atomic.StoreUint32(&b.val, val)
}

func (b *Bool) CompareAndSwap(old bool, newVal bool) (swaped bool) {
	oldVal := uint32(0)
	if old {
		oldVal = 1
	}
	val := uint32(0)
	if newVal {
		val = 1
	}
	swaped = atomic.CompareAndSwapUint32(&b.val, oldVal, val)
	return
}

func (b *Bool) Swap(new bool) (old bool) {
	tmp := uint32(0)
	if new {
		tmp = 1
	}
	old = atomic.SwapUint32(&b.val, tmp) == 1
	return
}

func (b *Bool) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.Load())
}

func (b *Bool) UnmarshalJSON(data []byte) error {
	// Accept bools from existing persisted cluster values; also handle legacy 0/1.
	var boolVal bool
	if err := json.Unmarshal(data, &boolVal); err == nil {
		b.Store(boolVal)
		return nil
	}

	var intVal int
	if err := json.Unmarshal(data, &intVal); err == nil {
		b.Store(intVal != 0)
		return nil
	}

	var strVal string
	if err := json.Unmarshal(data, &strVal); err == nil {
		switch strVal {
		case "true", "1":
			b.Store(true)
			return nil
		case "false", "0", "":
			b.Store(false)
			return nil
		}
		return fmt.Errorf("atomicutil.Bool: invalid string value %q", strVal)
	}

	return fmt.Errorf("atomicutil.Bool: invalid json %s", string(data))
}
