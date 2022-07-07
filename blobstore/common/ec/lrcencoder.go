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
	"io"

	"github.com/klauspost/reedsolomon"

	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/limit"
	"github.com/cubefs/cubefs/blobstore/util/task"
)

type lrcEncoder struct {
	Config
	pool        limit.Limiter // concurrency pool
	engine      reedsolomon.Encoder
	localEngine reedsolomon.Encoder
}

func (e *lrcEncoder) Encode(shards [][]byte) error {
	if len(shards) != (e.CodeMode.N + e.CodeMode.M + e.CodeMode.L) {
		return ErrInvalidShards
	}
	e.pool.Acquire()
	defer e.pool.Release()

	// firstly, do global ec encode
	if err := e.engine.Encode(shards[:e.CodeMode.N+e.CodeMode.M]); err != nil {
		return errors.Info(err, "lrcEncoder.Encode global failed")
	}
	if e.EnableVerify {
		ok, err := e.engine.Verify(shards[:e.CodeMode.N+e.CodeMode.M])
		if err != nil {
			return errors.Info(err, "lrcEncoder.Encode global verify failed")
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
				return errors.Info(err, "lrcEncoder.Encode local failed")
			}
			if e.EnableVerify {
				ok, err := e.localEngine.Verify(localShards)
				if err != nil {
					return errors.Info(err, "lrcEncoder.Encode local verify failed")
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

type verifyError struct {
	error
	verified bool
}

func (e *lrcEncoder) Verify(shards [][]byte) (bool, error) {
	e.pool.Acquire()
	defer e.pool.Release()

	ok, err := e.engine.Verify(shards[:e.CodeMode.N+e.CodeMode.M])
	if !ok || err != nil {
		if err != nil {
			err = errors.Info(err, "lrcEncoder.Verify global shards failed")
		}
		return ok, err
	}

	tasks := make([]func() error, 0, e.CodeMode.AZCount)
	for i := 0; i < e.CodeMode.AZCount; i++ {
		localShards := e.GetShardsInIdc(shards, i)
		tasks = append(tasks, func() error {
			ok, err := e.localEngine.Verify(localShards)
			if !ok || err != nil {
				if err != nil {
					err = errors.Info(err, "lrcEncoder.Verify local shards failed")
				}
				return verifyError{error: err, verified: ok}
			}
			return nil
		})
	}
	if err := task.Run(context.Background(), tasks...); err != nil {
		if verifyErr, succ := err.(verifyError); succ {
			return verifyErr.verified, verifyErr.error
		}
		return false, err
	}

	return true, nil
}

func (e *lrcEncoder) Reconstruct(shards [][]byte, badIdx []int) error {
	globalBadIdx := make([]int, 0)
	for _, i := range badIdx {
		if i < e.CodeMode.N+e.CodeMode.M {
			globalBadIdx = append(globalBadIdx, i)
		}
	}
	initBadShards(shards, globalBadIdx)
	e.pool.Acquire()
	defer e.pool.Release()

	// use local ec reconstruct, saving network bandwidth
	if len(shards) == (e.CodeMode.N+e.CodeMode.M+e.CodeMode.L)/e.CodeMode.AZCount {
		if err := e.localEngine.Reconstruct(shards); err != nil {
			return errors.Info(err, "lrcEncoder.Reconstruct local ec reconstruct failed")
		}
		return nil
	}

	// can't reconstruct from local ec
	// firstly, use global ec reconstruct
	if err := e.engine.Reconstruct(shards[:e.CodeMode.N+e.CodeMode.M]); err != nil {
		return errors.Info(err, "lrcEncoder.Reconstruct global ec reconstruct failed")
	}

	// secondly, check if need to reconstruct the local shards
	localRestructs := make(map[int][]int)
	for _, i := range badIdx {
		if i >= (e.CodeMode.N + e.CodeMode.M) {
			idcIdx := (i - e.CodeMode.N - e.CodeMode.M) * e.CodeMode.AZCount / e.CodeMode.L
			badIdx := idcIdx + (e.CodeMode.N+e.CodeMode.M)/e.CodeMode.AZCount - 1
			if _, ok := localRestructs[idcIdx]; !ok {
				localRestructs[idcIdx] = make([]int, 0)
			}
			localRestructs[idcIdx] = append(localRestructs[idcIdx], badIdx)
		}
	}

	tasks := make([]func() error, 0, len(localRestructs))
	for idx, badIdx := range localRestructs {
		localShards := e.GetShardsInIdc(shards, idx)
		initBadShards(localShards, badIdx)
		tasks = append(tasks, func() error {
			return e.localEngine.Reconstruct(localShards)
		})
	}
	if err := task.Run(context.Background(), tasks...); err != nil {
		return errors.Info(err, "lrcEncoder.Reconstruct local ec reconstruct after global ec failed")
	}
	return nil
}

func (e *lrcEncoder) ReconstructData(shards [][]byte, badIdx []int) error {
	initBadShards(shards, badIdx)
	shards = shards[:e.CodeMode.N+e.CodeMode.M]
	e.pool.Acquire()
	defer e.pool.Release()
	return e.engine.ReconstructData(shards)
}

func (e *lrcEncoder) Split(data []byte) ([][]byte, error) {
	shards, err := e.engine.Split(data)
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

func (e *lrcEncoder) GetDataShards(shards [][]byte) [][]byte {
	return shards[:e.CodeMode.N]
}

func (e *lrcEncoder) GetParityShards(shards [][]byte) [][]byte {
	return shards[e.CodeMode.N : e.CodeMode.N+e.CodeMode.M]
}

func (e *lrcEncoder) GetLocalShards(shards [][]byte) [][]byte {
	return shards[e.CodeMode.N+e.CodeMode.M:]
}

func (e *lrcEncoder) GetShardsInIdc(shards [][]byte, idx int) [][]byte {
	n, m, l := e.CodeMode.N, e.CodeMode.M, e.CodeMode.L
	idcCnt := e.CodeMode.AZCount
	localN, localM, localL := n/idcCnt, m/idcCnt, l/idcCnt

	localShardsInIdc := make([][]byte, localN+localM+localL)

	shardsN := shards[idx*localN : (idx+1)*localN]
	shardsM := shards[(n + localM*idx):(n + localM*(idx+1))]
	shardsL := shards[(n + m + localL*idx):(n + m + localL*(idx+1))]

	copy(localShardsInIdc, shardsN)
	copy(localShardsInIdc[localN:], shardsM)
	copy(localShardsInIdc[localN+localM:], shardsL)
	return localShardsInIdc
}

func (e *lrcEncoder) Join(dst io.Writer, shards [][]byte, outSize int) error {
	return e.engine.Join(dst, shards[:(e.CodeMode.N+e.CodeMode.M)], outSize)
}
