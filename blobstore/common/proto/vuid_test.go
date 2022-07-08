// Copyright 2022 The CubeFS Authors.
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

package proto

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVuid(t *testing.T) {
	vid := Vid(rand.Uint32())
	index := uint8(rand.Intn(256))
	epoch := uint32(rand.Int31n(MaxEpoch))
	vuid, err := NewVuid(vid, index, epoch)
	vuidPre := EncodeVuidPrefix(vid, index)
	assert.NoError(t, err)

	assert.Equal(t, vuidPre, vuid.VuidPrefix())
	assert.Equal(t, vid, vuidPre.Vid())
	assert.Equal(t, index, vuidPre.Index())
	assert.Equal(t, vid, vuid.Vid())
	assert.Equal(t, index, vuid.Index())
	assert.Equal(t, epoch, vuid.Epoch())
}

func TestDecodeVuid(t *testing.T) {
	for i := 0; i < 900000; i++ {
		TestVuid(t)
	}
}

func TestDecodeVuid2(t *testing.T) {
	old := Vuid(425335980033)
	new := Vuid(116131889167)
	t.Log(old.Vid(), old.VuidPrefix(), old.Index(), old.Epoch())
	t.Log(new.Vid(), new.VuidPrefix(), new.Index(), new.Epoch())
}
