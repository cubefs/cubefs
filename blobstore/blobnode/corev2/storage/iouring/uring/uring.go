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

package uring

func New(entries uint32, flags uint32) (*IoUring, error) {
	ring := &IoUring{}
	p := new(IoUringParams)
	p.Flags = flags
	err := io_uring_queue_init_params(entries, ring, p)
	if err != nil {
		return nil, err
	}
	return ring, nil
}

func NewWithParams(entries uint32, params *IoUringParams) (*IoUring, error) {
	ring := &IoUring{}
	if params == nil {
		params = new(IoUringParams)
	}
	err := io_uring_queue_init_params(entries, ring, params)
	if err != nil {
		return nil, err
	}
	return ring, nil
}

func (h *IoUring) Close() {
	h.io_uring_queue_exit()
}

func (h *IoUring) GetSqe() *IoUringSqe {
	return h.io_uring_get_sqe()
}

func (h *IoUring) PeekCqes(cqes []*IoUringCqe, count uint32) (n uint32) {
	n = h.io_uring_peek_batch_cqe(cqes, count)
	return
}

func (h *IoUring) WaitCqe(cqePtr **IoUringCqe) error {
	return h.io_uring_wait_cqe(cqePtr)
}

func (h *IoUring) SeenCqe(cqe *IoUringCqe) {
	h.io_uring_cqe_seen(cqe)
}

func (h *IoUring) SeenCqes(n uint32) {
	h.io_uring_cq_advance(n)
}

func (h *IoUring) Submit() (int, error) {
	return h.io_uring_submit()
}

func (h *IoUring) SubmitAndWait(waitNr uint32) (int, error) {
	return h.io_uring_submit_and_wait(waitNr)
}
