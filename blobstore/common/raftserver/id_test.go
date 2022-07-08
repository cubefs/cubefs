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

package raftserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIdGenerator(t *testing.T) {
	genetator := NewGenerator(1, time.Now())

	id1 := genetator.Next()
	id2 := genetator.Next()
	assert.Equal(t, id1+1, id2)
}
