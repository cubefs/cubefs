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
	"math"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/blobstore/blobnode/corev2/storage/iouring/uring"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/cpuset"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type Engine interface {
	Read(data []byte, off uint64, size int) error
	Write(data []byte, off uint64, size int) error
	Close() error
}

type Config struct {
	SubmitBudget int    `json:"submit_budget"`
	MaxEntry     int    `json:"max_entry"`
	FilePath     string `json:"file_path"`
	CPUID        int    `json:"cpu_id"`
}

func NewEngine(cfg Config) (Engine, error) {
	defaulter.Equal(&cfg.SubmitBudget, 64)
	defaulter.Equal(&cfg.MaxEntry, 512)

	params := &uring.IoUringParams{Flags: uring.IORING_SETUP_IOPOLL}

	ring, err := uring.NewWithParams(uint32(cfg.MaxEntry)*2, params)
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(cfg.FilePath, os.O_RDWR|syscall.O_DIRECT, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open temp file failed: %s", err)
	}

	fd := int(f.Fd())
	ring.IO_uring_register_files([]int{fd}, 1)

	s := &engine{
		ring:        ring,
		file:        f,
		queue:       newQueue(),
		pendingReqs: newConcurrentPendingRequests(),

		closer: closer.New(),
		cfg:    cfg,
	}
	go s.doBatch2()
	// go s.doBatch1()
	// go s.loop()

	return s, nil
}

type engine struct {
	reqIDCounter uint64
	file         *os.File
	ring         *uring.IoUring
	queue        *queue
	// submitCh     chan request
	pendingReqs *concurrentPendingRequests

	closer closer.Closer
	cfg    Config
}

func (s *engine) Read(data []byte, off uint64, size int) error {
	req := newRequest(opRead, s.getReqID(), data, off, size)
	s.submit(req)
	return req.Wait()
}

func (s *engine) Write(data []byte, off uint64, size int) error {
	req := newRequest(opWrite, s.getReqID(), data, off, size)
	s.submit(req)
	return req.Wait()
}

func (s *engine) Close() error {
	s.closer.Close()
	s.ring.Close()
	return s.file.Close()
}

func (s *engine) submit(req request) {
	s.pendingReqs.putRequest(req)
	s.queue.add(req)
	/*queueIdx := s.queue.add(req)
	// the first submitter will raise write batch loop
	if queueIdx == 1 {
		go s.doBatch()
	}*/
}

/*func (s *engine) loop() {
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
				panic(fmt.Sprintf("iouring submit and Wait failed: %s", err))
			}
			if submitted != s.cfg.MaxEntry-budget {
				panic(fmt.Sprintf("mismatch submitted: %d-%d", submitted, s.cfg.MaxEntry-budget))
			}
			s.getCompletion(submitted)

		case <-s.closer.Done():

			return
		}
	}
}*/

func (s *engine) doBatch1() {
	/*if s.cfg.CPUID > 0 {
		cpuset.SetAffinity(s.cfg.CPUID)
	}*/

	var (
		sqe         *uring.IoUringSqe
		submitLimit = int32(s.cfg.MaxEntry)
	)

	go s.getCompletion1(&submitLimit)

AGAIN:
	budget := s.cfg.SubmitBudget
	limit := atomic.LoadInt32(&submitLimit)
	if limit == 0 {
		time.Sleep(30 * time.Microsecond)
		goto AGAIN
	}
	if budget > int(limit) {
		budget = int(limit)
	}

	reqs, ok := s.queue.drain()
	if !ok {
		/*time.Sleep(1 * time.Microsecond)
		goto AGAIN*/
		for {
			if atomic.LoadInt32(&submitLimit) != int32(s.cfg.MaxEntry) {
				time.Sleep(30 * time.Microsecond)
				continue
			}
			atomic.StoreInt32(&submitLimit, -math.MaxInt32)
			return
		}
	}

	for i := range reqs {
		sqe = s.ring.GetSqe()
		addr := &(reqs[i].buf[0])

		switch reqs[i].op {
		case opWrite:
			uring.PrepWrite(sqe, int(s.file.Fd()), addr, reqs[i].size, reqs[i].off)
		case opRead:
			uring.PrepRead(sqe, int(s.file.Fd()), addr, reqs[i].size, reqs[i].off)
		}
		sqe.UserData.SetUint64(uint64(reqs[i].id))

		budget -= 1
		if budget <= 0 || i == len(reqs)-1 {
			submitted, err := s.ring.Submit()
			if err != nil {
				panic(fmt.Sprintf("iouring submit and Wait failed: %s", err))
			}
			limit = atomic.AddInt32(&submitLimit, int32(-submitted))
		}
		if budget <= 0 {
			budget = s.cfg.SubmitBudget
			if limit == 0 {
				for {
					time.Sleep(30 * time.Microsecond)
					if limit = atomic.LoadInt32(&submitLimit); limit > 0 {
						break
					}
				}
			}
			if budget > int(limit) {
				budget = int(limit)
			}
		}
	}

	s.queue.recycle(reqs)

	goto AGAIN
}

func (s *engine) getCompletion1(submitLimit *int32) {
	var cqe *uring.IoUringCqe

	for {
		limit := atomic.LoadInt32(submitLimit)
		if limit < 0 {
			log.Warn("stop get completion and quit")
			return
		}

		submitted := int32(s.cfg.MaxEntry) - limit
		if submitted == 0 {
			time.Sleep(30 * time.Microsecond)
			continue
		}

		for i := 0; i < int(submitted); i++ {
		RETRY:
			err := s.ring.WaitCqe(&cqe)
			if errors.Is(err, syscall.EINTR) {
				goto RETRY
			}
			if err != nil {
				panic(fmt.Sprintf("iouring Wait cqe failed: %s", err))
			}
			if cqe == nil {
				panic("iouring cqe is nil")
			}

			req := s.pendingReqs.getRequest(reqID(cqe.UserData.GetUint64()))
			if cqe.Res < 0 {
				req.Notify(syscall.Errno(-cqe.Res))
			} else {
				req.Notify(nil)
			}

			s.ring.SeenCqe(cqe)
			atomic.AddInt32(submitLimit, 1)
		}
	}
}

func (s *engine) doBatch2() {
	if s.cfg.CPUID > 0 {
		cpuset.SetAffinity(s.cfg.CPUID)
	}
	//runtime.LockOSThread()

	var (
		sqe         *uring.IoUringSqe
		submitLimit = s.cfg.MaxEntry
	)

	for {
		budget := s.cfg.SubmitBudget
		if submitLimit == 0 {
			s.getCompletion2(&submitLimit, true)
		}
		if budget > submitLimit {
			budget = submitLimit
		}

		reqs, ok := s.queue.drain()
		if !ok {
			if submitLimit != s.cfg.MaxEntry {
				s.getCompletion2(&submitLimit, true)
				continue
			}
			continue
		}

		for i := range reqs {
			sqe = s.ring.GetSqe()
			addr := &(reqs[i].buf[0])

			switch reqs[i].op {
			case opWrite:
				uring.PrepWrite(sqe, int(s.file.Fd()), addr, reqs[i].size, reqs[i].off)
			case opRead:
				uring.PrepRead(sqe, int(s.file.Fd()), addr, reqs[i].size, reqs[i].off)
			}
			sqe.UserData.SetUint64(uint64(reqs[i].id))

			budget -= 1

			if budget <= 0 || i == len(reqs)-1 {
				submitted, err := s.ring.Submit()
				if err != nil {
					panic(fmt.Sprintf("iouring submit failed: %s", err))
				}
				submitLimit -= submitted
				s.getCompletion2(&submitLimit, false)
			}
			if budget <= 0 {
				budget = s.cfg.SubmitBudget
				if submitLimit == 0 {
					s.getCompletion2(&submitLimit, true)
				}
				if budget > submitLimit {
					budget = submitLimit
				}
			}
		}

		s.queue.recycle(reqs)
		s.getCompletion2(&submitLimit, false)
	}
}

func (s *engine) doBatch() {
	/*if s.cfg.CPUID > 0 {
		cpuset.SetAffinity(s.cfg.CPUID)
	}*/

	var sqe *uring.IoUringSqe

AGAIN:
	budget := s.cfg.SubmitBudget

	reqs, ok := s.queue.drain()
	if !ok {
		return
	}

	for i := range reqs {
		sqe = s.ring.GetSqe()
		addr := &(reqs[i].buf[0])

		switch reqs[i].op {
		case opWrite:
			uring.PrepWrite(sqe, int(s.file.Fd()), addr, reqs[i].size, reqs[i].off)
		case opRead:
			uring.PrepRead(sqe, int(s.file.Fd()), addr, reqs[i].size, reqs[i].off)
		}
		sqe.UserData.SetUint64(uint64(reqs[i].id))

		budget -= 1
		if budget <= 0 {
			submitted, err := s.ring.SubmitAndWait(uint32(s.cfg.SubmitBudget))
			if err != nil {
				panic(fmt.Sprintf("iouring submit and Wait failed: %s", err))
			}
			if submitted != s.cfg.SubmitBudget {
				panic(fmt.Sprintf("mismatch submitted: %d-%d", submitted, s.cfg.SubmitBudget-budget))
			}
			s.getCompletion(submitted)

			budget = s.cfg.SubmitBudget
		}
	}

	// the rest write
	if budget < s.cfg.SubmitBudget {
		submitted, err := s.ring.SubmitAndWait(uint32(s.cfg.SubmitBudget - budget))
		if err != nil {
			panic(fmt.Sprintf("iouring submit and Wait failed: %s", err))
		}
		if submitted != s.cfg.SubmitBudget-budget {
			panic(fmt.Sprintf("mismatch submitted: %d-%d", submitted, s.cfg.SubmitBudget-budget))
		}
		s.getCompletion(submitted)
	}

	s.queue.recycle(reqs)

	goto AGAIN
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
			panic(fmt.Sprintf("iouring Wait cqe failed: %s", err))
		}
		if cqe == nil {
			panic("iouring cqe is nil")
		}

		req := s.pendingReqs.getRequest(reqID(cqe.UserData.GetUint64()))
		if cqe.Res < 0 {
			req.Notify(syscall.Errno(-cqe.Res))
		} else {
			req.Notify(nil)
		}

		s.ring.SeenCqe(cqe)
	}
}

func (s *engine) getCompletion2(submitLimit *int, wait bool) {
	var (
		err error
		cqe *uring.IoUringCqe
	)

	submitted := s.cfg.MaxEntry - *submitLimit
	if submitted == 0 {
		return
	}

	for i := 0; i < submitted; i++ {
	RETRY:
		if wait {
			err = s.ring.WaitCqe(&cqe)
		} else {
			err = s.ring.PeekCqe(&cqe)
		}
		if errors.Is(err, syscall.EINTR) {
			if wait {
				goto RETRY
			}
			return
		}
		if err != nil {
			panic(fmt.Sprintf("iouring Wait cqe failed: %s", err))
		}
		if cqe == nil {
			return
		}

		req := s.pendingReqs.getRequest(reqID(cqe.UserData.GetUint64()))
		if cqe.Res < 0 {
			req.Notify(syscall.Errno(-cqe.Res))
		} else {
			req.Notify(nil)
		}

		s.ring.SeenCqe(cqe)
		*submitLimit += 1
	}
}

func (s *engine) getReqID() reqID {
	return reqID(atomic.AddUint64(&s.reqIDCounter, 1))
}
