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

package unboundedchan

import (
	"errors"
	"testing"
)

func TestReadEmptyRingBuffer(t *testing.T) {
	buffer := NewRingBuffer(10)
	v, err := buffer.Read()
	if v != nil || err == nil {
		t.Errorf("expected:(%v %v), got:(%v %v)", nil, errors.New("ringbuffer is empty"), v, err)
	}

}

func TestWriteAndReadOne(t *testing.T) {
	data := "value"
	buffer := NewRingBuffer(10)
	//write
	buffer.Write(data)
	if buffer.Len() != 1 {
		t.Errorf("expected buf len:(%v), got:(%v)", 1, buffer.Len())
	}
	if buffer.wIndex != 1 {
		t.Errorf("expected buf len:(%v), got:(%v)", 1, buffer.wIndex)
	}

	v, err := buffer.Read()
	if err != nil {
		t.Errorf("expected err:(%v), got:(%v)", nil, err)
	}
	if v != data {
		t.Errorf("expected data:(%v), got:(%v)", data, v)
	}
	if buffer.Len() != 0 {
		t.Errorf("expected buf len:(%v), got:(%v)", 0, buffer.Len())
	}
}

func TestBufferScaleUp(t *testing.T) {
	buffer := NewRingBuffer(10)
	//write till buffer full
	for i := 0; i < 9; i++ {
		buffer.Write(i)
	}

	if buffer.size != 10 {
		t.Errorf("expected buffer size:(%v), got(%v", 10, buffer.size)
	}

	//trigger scaling up
	buffer.Write(10)
	//scale buffer size up to double the size of the current buffer
	if buffer.size != 20 {
		t.Errorf("expected buffer size:(%v), got(%v", 20, buffer.size)
	}
	if buffer.Len() != 10 {
		t.Errorf("expected buffer len:(%v), got(%v)", 10, buffer.Len())
	}
}
