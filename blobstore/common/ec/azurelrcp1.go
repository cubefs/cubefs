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
	"github.com/cubefs/cubefs/blobstore/util/log"
	"io"

	"github.com/klauspost/reedsolomon"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/limit"
)

type azureLrcP1Encoder struct {
	Config
	pool   limit.Limiter // concurrency pool
	engine reedsolomon.Encoder
}

func (e *azureLrcP1Encoder) Encode(shards [][]byte) error {
	if len(shards) != (e.CodeMode.N + e.CodeMode.M + e.CodeMode.L) {
		return ErrInvalidShards
	}
	e.pool.Acquire()
	defer e.pool.Release()
	fillFullShards(shards)

	if err := e.engine.Encode(shards[:e.CodeMode.N+e.CodeMode.M+e.CodeMode.L]); err != nil {
		return errors.Info(err, "azureLrcP1Encoder.Encode entire failed")
	}
	if e.EnableVerify {
		ok, err := e.engine.Verify(shards[:e.CodeMode.N+e.CodeMode.M+e.CodeMode.L])
		if err != nil {
			return errors.Info(err, "azureLrcP1Encoder.Encode entire verify failed")
		}
		if !ok {
			return ErrVerify
		}
	}

	return nil
}

func (e *azureLrcP1Encoder) Verify(shards [][]byte) (bool, error) {
	e.pool.Acquire()
	defer e.pool.Release()
	log.SetOutputLevel(0)

	// verify the entire stripe
	ok, err := e.engine.Verify(shards[:e.CodeMode.N+e.CodeMode.M+e.CodeMode.L])
	if err != nil {
		err = errors.Info(err, "azureLrcP1Encoder.Verify entire shards failed")
	}
	if !ok && err == nil {
		err = ErrVerify
	}
	return ok, err
}

func (e *azureLrcP1Encoder) Reconstruct(shards [][]byte, badIdx []int) error {
	e.pool.Acquire()
	fillFullShards(shards)

	initBadShards(shards, badIdx)
	defer e.pool.Release()

	isIn := func(elem int, list []int) bool {
		for _, c := range list {
			if elem == c {
				return true
			}
		}
		return false
	}
	azLayout := e.CodeMode.GetECLayoutByAZ()
	survivalIndex, forComputationShardsIdx, err := e.engine.GetSurvivalShards(badIdx, azLayout)
	if err != nil {
		return err
	}

	if len(badIdx) == 1 {
		tmpShards := make([][]byte, e.CodeMode.N+e.CodeMode.M+e.CodeMode.L)
		for i, v := range shards {
			if isIn(i, forComputationShardsIdx) {
				tmpShards[i] = v
			} else {
				tmpShards[i] = v[:0]
			}
		}
		if err := e.engine.PartialReconstruct(tmpShards[:e.CodeMode.N+e.CodeMode.M+e.CodeMode.L], survivalIndex, badIdx); err != nil {
			return errors.Info(err, "azureLrcP1Encoder.ParticailReconstruct ec reconstruct failed (local reconstruct)")
		}
		shards[badIdx[0]] = tmpShards[badIdx[0]]
	} else {
		for i, v := range shards {
			if isIn(i, survivalIndex) == false {
				shards[i] = v[:0]
			}
		}
		if err := e.engine.Reconstruct(shards[:e.CodeMode.N+e.CodeMode.M+e.CodeMode.L]); err != nil {
			return errors.Info(err, "azureLrcP1Encoder.Reconstruct ec reconstruct failed (entire reconstruct)")
		}
	}

	return nil
}

func (e *azureLrcP1Encoder) ReconstructData(shards [][]byte, badIdx []int) error {
	fillFullShards(shards[:e.CodeMode.N+e.CodeMode.M])
	globalBadIdx := make([]int, 0)
	for _, i := range badIdx {
		if i < e.CodeMode.N+e.CodeMode.M {
			globalBadIdx = append(globalBadIdx, i)
		}
	}
	initBadShards(shards, globalBadIdx)
	shards = shards[:e.CodeMode.N+e.CodeMode.M+e.CodeMode.L]
	e.pool.Acquire()
	defer e.pool.Release()
	return e.engine.ReconstructData(shards)
}

func (e *azureLrcP1Encoder) Split(data []byte) ([][]byte, error) {
	shards, err := e.engine.Split(data)
	if err != nil {
		return nil, err
	}
	return shards, nil
}

func (e *azureLrcP1Encoder) GetDataShards(shards [][]byte) [][]byte {
	return shards[:e.CodeMode.N]
}

// GetParityShards : This function means get the all parity shards (global & local)
func (e *azureLrcP1Encoder) GetParityShards(shards [][]byte) [][]byte {
	return shards[e.CodeMode.N:]
}

func (e *azureLrcP1Encoder) GetLocalShards(shards [][]byte) [][]byte {
	return shards[e.CodeMode.N+e.CodeMode.M:]
}

func (e *azureLrcP1Encoder) GetShardsInIdc(shards [][]byte, idx int) [][]byte {
	locals, _, _ := e.CodeMode.LocalStripeInAZ(idx)
	localShards := make([][]byte, len(locals))
	for localIdx, globalIdx := range locals {
		localShards[localIdx] = shards[globalIdx]
	}
	return localShards
}

func (e *azureLrcP1Encoder) Join(dst io.Writer, shards [][]byte, outSize int) error {
	return e.engine.Join(dst, shards[:(e.CodeMode.N+e.CodeMode.M+e.CodeMode.L)], outSize)
}
