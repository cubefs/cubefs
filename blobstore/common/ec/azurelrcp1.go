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
	"context"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"io"

	"github.com/klauspost/reedsolomon"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/task"
)

type azureLrcP1Encoder struct {
	Config
	pool         limit.Limiter // concurrency pool
	globalEngine reedsolomon.Encoder
	localEngine  reedsolomon.Encoder
	entireEngine reedsolomon.Encoder
}

func (e *azureLrcP1Encoder) Encode(shards [][]byte) error {
	if len(shards) != (e.CodeMode.N + e.CodeMode.M + e.CodeMode.L) {
		return ErrInvalidShards
	}
	// For better load balance, we force that m = n/(l-1)
	// Our Encode() & Verify() are based on the limit above
	if e.CodeMode.M != int(float64(e.CodeMode.N)/float64(e.CodeMode.L-1)) {
		return ErrInvalidShards
	}
	e.pool.Acquire()
	defer e.pool.Release()
	fillFullShards(shards)

	// firstly, do global ec encode
	if err := e.globalEngine.Encode(shards[:e.CodeMode.N+e.CodeMode.M]); err != nil {
		return errors.Info(err, "azureLrcP1Encoder.Encode global failed")
	}
	if e.EnableVerify {
		ok, err := e.globalEngine.Verify(shards[:e.CodeMode.N+e.CodeMode.M])
		if err != nil {
			return errors.Info(err, "azureLrcP1Encoder.Encode global verify failed")
		}
		if !ok {
			return ErrVerify
		}
	}

	tasks := make([]func() error, 0, e.CodeMode.AZCount)
	// secondly, do local ec encode
	for i := 0; i < e.CodeMode.AZCount; i++ {
		localShards := e.GetShardsInIdc(shards, i)
		tasks = append(tasks, func() error {
			if err := e.localEngine.Encode(localShards); err != nil {
				return errors.Info(err, "azureLrcP1Encoder.Encode local failed")
			}
			if e.EnableVerify {
				ok, err := e.localEngine.Verify(localShards)
				if err != nil {
					return errors.Info(err, "azureLrcP1Encoder.Encode local verify failed")
				}
				if !ok {
					return ErrVerify
				}
			}
			return nil
		})
	}
	if err := task.Run(context.Background(), tasks...); err != nil {
		return err
	}

	return nil
}

func (e *azureLrcP1Encoder) Verify(shards [][]byte) (bool, error) {
	e.pool.Acquire()
	defer e.pool.Release()
	log.SetOutputLevel(0)

	// verify an AZ stripe
	if len(shards) == e.CodeMode.M+e.CodeMode.L/e.CodeMode.AZCount {
		ok, err := e.localEngine.Verify(shards)
		if err != nil {
			err = errors.Info(err, "azureLrcP1Encoder.Verify local shards failed")
		}
		if !ok && err == nil {
			err = ErrVerify
		}
		return ok, err
	}

	ok, err := e.globalEngine.Verify(shards[:e.CodeMode.N+e.CodeMode.M])
	if err != nil {
		err = errors.Info(err, "azureLrcP1Encoder.Verify entire shards failed")
	}
	if !ok && err == nil {
		err = ErrVerify
	}
	// verify the entire stripe
	ok, err = e.entireEngine.Verify(shards[:e.CodeMode.N+e.CodeMode.M+e.CodeMode.L])
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

	globalBadIdx := make([]int, 0)
	for _, i := range badIdx {
		if i < e.CodeMode.N+e.CodeMode.M {
			globalBadIdx = append(globalBadIdx, i)
		}
	}
	initBadShards(shards, globalBadIdx)
	defer e.pool.Release()

	// use local ec reconstruct, saving network bandwidth
	if len(shards) == e.CodeMode.M+e.CodeMode.L/e.CodeMode.AZCount {
		if err := e.localEngine.Reconstruct(shards); err != nil {
			return errors.Info(err, "azureLrcP1Encoder.Reconstruct local ec reconstruct failed")
		}
		return nil
	}

	// can't reconstruct from local ec
	// Try to two-step repair :  global first, local second.
	// all scenario with no more than g failure
	globalStripeBadCnt := 0
	for _, idx := range badIdx {
		if idx < e.CodeMode.M+e.CodeMode.N {
			globalStripeBadCnt++
		}
	}
	if globalStripeBadCnt <= e.CodeMode.M {
		// firstly, use global ec reconstruct
		if err := e.globalEngine.Reconstruct(shards[:e.CodeMode.N+e.CodeMode.M]); err != nil {
			return errors.Info(err, "azureLrcP1Encoder.Reconstruct global ec reconstruct failed (two step)")
		}

		// secondly, check if it needs to reconstruct the local parity shards
		localReconstructs := make(map[int][]int)
		n, m := e.CodeMode.N, e.CodeMode.M
		// we assume that l is equal to AZcount
		for _, i := range badIdx {
			if i < (n + m) {
				continue
			}
			idcIdx := i - n - m
			localBadIdx := m
			if _, ok := localReconstructs[idcIdx]; !ok {
				localReconstructs[idcIdx] = make([]int, 0)
			}
			localReconstructs[idcIdx] = append(localReconstructs[idcIdx], localBadIdx)
		}
		tasks := make([]func() error, 0, len(localReconstructs))
		for idx, badIdx := range localReconstructs {
			localShards := e.GetShardsInIdc(shards, idx)
			initBadShards(localShards, badIdx)
			tasks = append(tasks, func() error {
				return e.localEngine.Reconstruct(localShards)
			})

		}
		if err := task.Run(context.Background(), tasks...); err != nil {
			return errors.Info(err, "azureLrcP1Encoder.Reconstruct local ec reconstruct after global ec failed")
		}
		return nil
	}

	// no less than g+1 failure
	if err := e.entireEngine.Reconstruct(shards[:e.CodeMode.N+e.CodeMode.M+e.CodeMode.L]); err != nil {
		return errors.Info(err, "azureLrcP1Encoder.Reconstruct ec reconstruct failed (entire reconstruct)")
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
	shards = shards[:e.CodeMode.N+e.CodeMode.M]
	e.pool.Acquire()
	defer e.pool.Release()
	return e.globalEngine.ReconstructData(shards)
}

func (e *azureLrcP1Encoder) Split(data []byte) ([][]byte, error) {
	shards, err := e.globalEngine.Split(data)
	if err != nil {
		return nil, err
	}
	shardN, shardLen := len(shards), len(shards[0])
	if cap(data) >= (e.CodeMode.L+shardN)*shardLen {
		if cap(data) > len(data) {
			data = data[:cap(data)]
		}
		for i := 0; i < e.CodeMode.L; i++ {
			shards = append(shards, data[(shardN+i)*shardLen:(shardN+i+1)*shardLen])
		}
	} else {
		for i := 0; i < e.CodeMode.L; i++ {
			shards = append(shards, make([]byte, shardLen))
		}
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
	return e.globalEngine.Join(dst, shards[:(e.CodeMode.N+e.CodeMode.M)], outSize)
}
