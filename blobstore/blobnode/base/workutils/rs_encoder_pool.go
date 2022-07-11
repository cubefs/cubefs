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

package workutils

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/klauspost/reedsolomon"
)

// EncoderPool used for ec
type EncoderPool struct {
	mu       sync.Mutex
	encoders map[string]reedsolomon.Encoder
}

func newEncoderPool() *EncoderPool {
	return &EncoderPool{
		encoders: make(map[string]reedsolomon.Encoder),
	}
}

func encoderKey(N, M int) string {
	return strconv.Itoa(N) + "p" + strconv.Itoa(M)
}

// GetEncoder return ec encoder
func (p *EncoderPool) GetEncoder(N, M int) reedsolomon.Encoder {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := encoderKey(N, M)
	enc, ok := p.encoders[key]
	if !ok {
		var err error
		enc, err = reedsolomon.New(N, M)
		if err != nil {
			panic(fmt.Sprintf("reedsolomon.New: %v", err))
		}
		p.encoders[key] = enc
	}
	return enc
}

var (
	encoderPool     *EncoderPool
	encoderPoolOnce sync.Once
)

// EncoderPoolInst sure only one instance in global
func EncoderPoolInst() *EncoderPool {
	encoderPoolOnce.Do(func() {
		encoderPool = newEncoderPool()
	})
	return encoderPool
}
