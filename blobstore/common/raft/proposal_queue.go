package raft

import (
	"context"
)

func newProposalQueue(bufferSize int) proposalQueue {
	return make(chan proposalRequest, bufferSize)
}

type proposalQueue chan proposalRequest

func (q proposalQueue) Push(ctx context.Context, m proposalRequest) error {
	select {
	case q <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q proposalQueue) Iter(f func(m proposalRequest) bool) {
	for {
		select {
		case m := <-q:
			if !f(m) {
				return
			}
		default:
			return
		}
	}
}

func (q proposalQueue) Len() int {
	return len(q)
}
