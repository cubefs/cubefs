package infra

import "golang.org/x/time/rate"

type Flusher interface {
	// Flush flushes the data to the underlying storage.
	// opsLimiter (operation per second limiter): the rate limiter for the operation.
	// bpsLimiter (bytes pers second limiter): the rate limiter for the bytes.
	Flush(opsLimiter, bpsLimiter *rate.Limiter) error

	// Count returns the number of items to be flushed.
	Count() int
}

type multiFlusher []Flusher

func (m multiFlusher) Flush(opsLimiter, bpsLimiter *rate.Limiter) error {
	for _, f := range m {
		if err := f.Flush(opsLimiter, bpsLimiter); err != nil {
			return err
		}
	}
	return nil
}

func (m multiFlusher) Count() int {
	count := 0
	for _, f := range m {
		count += f.Count()
	}
	return count
}

func NewMultiFlusher(flushers ...Flusher) Flusher {
	return multiFlusher(flushers)
}

type funcFlusher struct {
	flushFunc func(opsLimiter, bpsLimiter *rate.Limiter) error
	countFunc func() int
}

func (f funcFlusher) Flush(opsLimiter, bpsLimiter *rate.Limiter) error {
	return f.flushFunc(opsLimiter, bpsLimiter)
}

func (f funcFlusher) Count() int {
	return f.countFunc()
}

func NewFuncFlusher(flushFunc func(opsLimiter, bpsLimiter *rate.Limiter) error, countFunc func() int) Flusher {
	return funcFlusher{flushFunc, countFunc}
}
