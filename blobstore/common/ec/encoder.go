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

package ec

import (
	"errors"
	"io"

	"github.com/klauspost/reedsolomon"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/limit/count"
)

const (
	defaultConcurrency = 100
)

// errors
var (
	ErrShortData       = errors.New("short data")
	ErrInvalidCodeMode = errors.New("invalid code mode")
	ErrVerify          = errors.New("shards verify failed")
	ErrInvalidShards   = errors.New("invalid shards")
)

// Encoder normal ec encoder, implements all these functions
type Encoder interface {
	// encode source data into shards, whatever normal ec or LRC
	Encode(shards [][]byte) error
	// reconstruct all missing shards, you should assign the missing or bad idx in shards
	Reconstruct(shards [][]byte, badIdx []int) error
	// only reconstruct data shards, you should assign the missing or bad idx in shards
	ReconstructData(shards [][]byte, badIdx []int) error
	// split source data into adapted shards size
	Split(data []byte) ([][]byte, error)
	// get data shards(No-Copy)
	GetDataShards(shards [][]byte) [][]byte
	// get parity shards(No-Copy)
	GetParityShards(shards [][]byte) [][]byte
	// get local shards(LRC model, No-Copy)
	GetLocalShards(shards [][]byte) [][]byte
	// get shards in an idc
	GetShardsInIdc(shards [][]byte, idx int) [][]byte
	// output source data into dst(io.Writer)
	Join(dst io.Writer, shards [][]byte, outSize int) error
	// verify parity shards with data shards
	Verify(shards [][]byte) (bool, error)
}

// Config ec encoder config
type Config struct {
	CodeMode     codemode.Tactic
	EnableVerify bool
	Concurrency  int
}

type encoder struct {
	Config
	pool   limit.Limiter // concurrency pool
	engine reedsolomon.Encoder
}

// NewEncoder return an encoder which support normal EC or LRC
func NewEncoder(cfg Config) (Encoder, error) {
	if !cfg.CodeMode.IsValid() {
		return nil, ErrInvalidCodeMode
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = defaultConcurrency
	}

	engine, err := reedsolomon.New(cfg.CodeMode.N, cfg.CodeMode.M)
	if err != nil {
		return nil, err
	}
	pool := count.NewBlockingCount(cfg.Concurrency)

	if cfg.CodeMode.L != 0 {
		localN := (cfg.CodeMode.N + cfg.CodeMode.M) / cfg.CodeMode.AZCount
		localM := cfg.CodeMode.L / cfg.CodeMode.AZCount
		localEngine, err := reedsolomon.New(localN, localM)
		if err != nil {
			return nil, err
		}
		return &lrcEncoder{
			Config:      cfg,
			pool:        pool,
			engine:      engine,
			localEngine: localEngine,
		}, nil
	}

	return &encoder{
		Config: cfg,
		pool:   pool,
		engine: engine,
	}, nil
}

func (e *encoder) Encode(shards [][]byte) error {
	e.pool.Acquire()
	defer e.pool.Release()

	if err := e.engine.Encode(shards); err != nil {
		return err
	}
	if e.EnableVerify {
		ok, err := e.engine.Verify(shards)
		if err != nil {
			return err
		}
		if !ok {
			return ErrVerify
		}
	}
	return nil
}

func (e *encoder) Verify(shards [][]byte) (bool, error) {
	e.pool.Acquire()
	defer e.pool.Release()
	return e.engine.Verify(shards)
}

func (e *encoder) Reconstruct(shards [][]byte, badIdx []int) error {
	initBadShards(shards, badIdx)
	e.pool.Acquire()
	defer e.pool.Release()
	return e.engine.Reconstruct(shards)
}

func (e *encoder) ReconstructData(shards [][]byte, badIdx []int) error {
	initBadShards(shards, badIdx)
	e.pool.Acquire()
	defer e.pool.Release()
	return e.engine.ReconstructData(shards)
}

func (e *encoder) Split(data []byte) ([][]byte, error) {
	return e.engine.Split(data)
}

func (e *encoder) GetDataShards(shards [][]byte) [][]byte {
	return shards[:e.CodeMode.N]
}

func (e *encoder) GetParityShards(shards [][]byte) [][]byte {
	return shards[e.CodeMode.N:]
}

func (e *encoder) GetLocalShards(shards [][]byte) [][]byte {
	return nil
}

func (e *encoder) GetShardsInIdc(shards [][]byte, idx int) [][]byte {
	n, m := e.CodeMode.N, e.CodeMode.M
	idcCnt := e.CodeMode.AZCount

	localN, localM := n/idcCnt, m/idcCnt

	return append(shards[idx*localN:(idx+1)*localN], shards[n+localM*idx:n+localM*(idx+1)]...)
}

func (e *encoder) Join(dst io.Writer, shards [][]byte, outSize int) error {
	return e.engine.Join(dst, shards, outSize)
}

func initBadShards(shards [][]byte, badIdx []int) {
	for _, i := range badIdx {
		if shards[i] != nil && len(shards[i]) != 0 && cap(shards[i]) > 0 {
			shards[i] = shards[i][:0]
		}
	}
}
