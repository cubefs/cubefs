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
	"context"
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/ec"
)

var encoderPool *EncoderPool

// EncoderPool used for ec
type EncoderPool struct {
	mu       sync.RWMutex
	encoders map[codemode.CodeMode]ec.Encoder
}

func init() {
	encoderPool = &EncoderPool{encoders: make(map[codemode.CodeMode]ec.Encoder)}
}

// ExtendReloader syncs extend codemodes from clustermgr when a mode is missing locally.
type ExtendReloader func(ctx context.Context, mode codemode.CodeMode) error

var extendReloader ExtendReloader

// SetExtendReloader registers passive codemode extend reload (typically from blobnode startup).
func SetExtendReloader(fn ExtendReloader) {
	extendReloader = fn
}

// EnsureCodeMode ensures the extend codemode is registered locally, reloading
// extend codemodes from clustermgr through the registered reloader if missing.
func EnsureCodeMode(ctx context.Context, mode codemode.CodeMode) error {
	if mode.IsValid() {
		return nil
	}
	if extendReloader == nil {
		return fmt.Errorf("codemode %d invalid and no reloader registered", mode)
	}
	return extendReloader(ctx, mode)
}

func GetEncoder(ctx context.Context, mode codemode.CodeMode) (ec.Encoder, error) {
	encoderPool.mu.RLock()
	if encoder, ok := encoderPool.encoders[mode]; ok {
		encoderPool.mu.RUnlock()
		return encoder, nil
	}
	encoderPool.mu.RUnlock()

	if err := EnsureCodeMode(ctx, mode); err != nil {
		return nil, err
	}

	encoder, err := ec.NewEncoder(ec.Config{
		CodeMode:     mode.Tactic(),
		EnableVerify: false,
		Concurrency:  0,
	})
	if err != nil {
		return nil, err
	}

	encoderPool.mu.Lock()
	encoderPool.encoders[mode] = encoder
	encoderPool.mu.Unlock()

	return encoder, nil
}
