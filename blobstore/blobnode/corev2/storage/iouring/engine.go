// Copyright 2024 The CubeFS Authors.
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

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"

	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/iouring/uring"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/cpuset"
)

type Engine interface {
	Read(data []byte, off uint64, size int) error
	Write(data []byte, off uint64, size int) error
	Close() error
}

type Config struct {
	MaxEntry int    `json:"max_entry"`
	FilePath string `json:"file_path"`
	CPUID    int    `json:"cpu_id"`
}

func NewEngine(cfg Config) (Engine, error) {
	params := &uring.IoUringParams{Flags: uring.IORING_SETUP_IOPOLL}

	ring, err := uring.NewWithParams(uint32(cfg.MaxEntry)*2, params)
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(cfg.FilePath, os.O_RDWR|syscall.O_DIRECT, 0644)
	if err != nil {
		return nil, fmt.Errorf("open temp file failed: %s", err)
	}
	defer f.Close()

	fd := int(f.Fd())
	ring.IO_uring_register_files([]int{fd}, 1)

	s := &engine{
		ring:        ring,
		file:        f,
		submitCh:    make(chan request, 1<<10),
		pendingReqs: &concurrentPendingRequests{},

		closer: closer.New(),
		cfg:    cfg,
	}
	go s.loop()

	return s, nil
}

type engine struct {
	reqIDCounter uint64
	file         *os.File
	ring         *uring.IoUring
	submitCh     chan request
	pendingReqs  *concurrentPendingRequests

	closer closer.Closer
	cfg    Config
}

func (s *engine) Read(data []byte, off uint64, size int) error {
	req := newRequest(opRead, s.getReqID(), data, off, size)
	s.submit(req)
	return req.wait()
}

func (s *engine) Write(data []byte, off uint64, size int) error {
	req := newRequest(opWrite, s.getReqID(), data, off, size)
	s.submit(req)
	return req.wait()
}

func (s *engine) Close() error {
	s.closer.Close()
	s.ring.Close()
	return s.file.Close()
}

func (s *engine) submit(req request) {
	s.pendingReqs.putRequest(req)
	// todo: optimized write channel by other costless struct
	s.submitCh <- req
}

func (s *engine) loop() {
	if s.cfg.CPUID > 0 {
		cpuset.SetAffinity(s.cfg.CPUID)
	}

	var (
		budget int
		sqe    *uring.IoUringSqe
	)

	for {
		select {
		case req := <-s.submitCh:
			sqe = s.ring.GetSqe()
			addr := &(req.buf[0])
			uring.PrepWrite(sqe, int(s.file.Fd()), addr, req.size, req.off)
			sqe.UserData.SetUint64(uint64(req.id))

			budget = s.cfg.MaxEntry - 1
		BUDGET:
			for budget > 0 {
				select {
				case req := <-s.submitCh:
					sqe := s.ring.GetSqe()
					addr := &(req.buf[0])
					uring.PrepWrite(sqe, int(s.file.Fd()), addr, req.size, req.off)
					sqe.UserData.SetUint64(uint64(req.id))
					budget -= 1
				default:
					break BUDGET
				}
			}

			submitted, err := s.ring.SubmitAndWait(uint32(s.cfg.MaxEntry - budget))
			if err != nil {
				panic(fmt.Sprintf("iouring submit and wait failed: %s", err))
			}
			if submitted != s.cfg.MaxEntry-budget {
				panic(fmt.Sprintf("mismatch submitted: %d-%d", submitted, s.cfg.MaxEntry-budget))
			}
			s.getCompletion(submitted)

		case <-s.closer.Done():

			return
		}
	}
}

func (s *engine) getCompletion(submitted int) {
	var cqe *uring.IoUringCqe

	for i := 0; i < submitted; i++ {
	RETRY:
		err := s.ring.WaitCqe(&cqe)
		if errors.Is(err, syscall.EINTR) {
			goto RETRY
		}
		if err != nil {
			panic(fmt.Sprintf("iouring wait cqe failed: %s", err))
		}
		if cqe == nil {
			panic("iouring cqe is nil")
		}

		req := s.pendingReqs.getRequest(cqe.UserData.GetUint64())
		if cqe.Res < 0 {
			req.notify(syscall.Errno(-cqe.Res))
		} else {
			req.notify(nil)
		}

		s.ring.SeenCqe(cqe)
	}
}

func (s *engine) getReqID() reqID {
	return reqID(atomic.AddUint64(&s.reqIDCounter, 1))
}
