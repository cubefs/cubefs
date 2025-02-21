// Copyright 2025 The CubeFS Authors.
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

package iouring

import "sync/atomic"

type ProxyConfig struct {
	Config
	MaxEngineNum int `json:"max_engine_num"`
}

func NewEngineProxy(cfg ProxyConfig) (Engine, error) {
	var (
		err error
		p   = &engineProxy{engines: make([]Engine, cfg.MaxEngineNum)}
	)
	for i := 0; i < cfg.MaxEngineNum; i++ {
		p.engines[i], err = NewEngine(cfg.Config)
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

type engineProxy struct {
	engines           []Engine
	roundRobinCounter uint64
}

func (e *engineProxy) Read(data []byte, off uint64, size int) error {
	idx := atomic.AddUint64(&e.roundRobinCounter, 1) % uint64(len(e.engines))
	return e.engines[idx].Read(data, off, size)
}

func (e *engineProxy) Write(data []byte, off uint64, size int) error {
	idx := atomic.AddUint64(&e.roundRobinCounter, 1) % uint64(len(e.engines))
	return e.engines[idx].Write(data, off, size)
}

func (e *engineProxy) Close() error {
	for i := range e.engines {
		if err := e.engines[i].Close(); err != nil {
			return err
		}
	}
	return nil
}
