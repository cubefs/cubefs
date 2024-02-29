package infra

type Flusher interface {
	// Flush flushes the data to the underlying storage.
	// ln is the callback function to notify the size of the flushed data.
	Flush(ln func(size int64)) error

	// Count returns the number of items to be flushed.
	Count() int
}

type multiFlusher []Flusher

func (m multiFlusher) Flush(ln func(size int64)) error {
	for _, f := range m {
		if err := f.Flush(ln); err != nil {
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
	flushFunc func(ln func(size int64)) error
	countFunc func() int
}

func (f funcFlusher) Flush(ln func(size int64)) error {
	return f.flushFunc(ln)
}

func (f funcFlusher) Count() int {
	return f.countFunc()
}

func NewFuncFlusher(flushFunc func(ln func(size int64)) error, countFunc func() int) Flusher {
	return funcFlusher{flushFunc, countFunc}
}
