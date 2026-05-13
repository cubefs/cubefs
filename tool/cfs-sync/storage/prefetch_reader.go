package storage

import (
	"context"
	"errors"
	"io"
	"sync"
)

// prefetchReader wraps a random-access fetch function with N worker
// goroutines that fetch fixed-size chunks ahead of the consumer, and
// presents the result as an in-order io.ReadCloser.
//
// The single-streamer SDK read path serialises every 128 KiB packet on
// one TCP conn, so a single-goroutine reader caps at ~330 MB/s. Letting N
// independent goroutines drive the same inode in parallel — each goroutine
// gets its own TCP conn from the SDK's connection pool — lifts the per-file
// ceiling by roughly N× until the NIC, the leader DataNode, or the
// destination becomes the bottleneck.
//
// The fetch function is supplied by the caller so this type is testable
// without an SDK / cluster. The caller is responsible for offset/length
// validation; this reader only assumes:
//   - fetch returns (0, io.EOF) when the requested range is fully past EOF
//   - fetch may return (n, io.EOF) when the range covers the tail
//   - fetch may return a short read with err == nil (must be retried by
//     the caller's fetchFunc itself — prefetchReader treats short reads
//     as the final bytes of that chunk)
//
// Buffers come from a sync.Pool; the consumer must call Close to ensure
// in-flight workers exit and pooled buffers are returned.
type prefetchReader struct {
	fetch       fetchFunc
	startOff    int64 // byte offset within the source where this reader begins
	size        int64 // total bytes to expose
	chunkSize   int
	parallelism int

	bufPool *sync.Pool

	// consumer-side state — touched only by Read / Close (single goroutine
	// per the io.Reader contract).
	cur     *prefetchChunk
	curPos  int
	nextSeq int64
	pending map[int64]*prefetchChunk
	// terminalErr is sticky once any non-EOF error has been observed; every
	// subsequent Read returns it.
	terminalErr error

	// totalChunks is the smallest seq that is fully past EOF. Workers
	// dispatched beyond this never call fetch.
	totalChunks int64

	in       chan *prefetchChunk
	dispatch chan int64

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// fetchFunc reads up to len(p) bytes starting at off into p. Returns the
// number of bytes read and any error. Callers may assume io.Reader-style
// semantics: a short read with err == nil is permitted but reduces the
// effective chunk size.
type fetchFunc func(p []byte, off int64) (int, error)

type prefetchChunk struct {
	seq    int64
	data   []byte  // bytes filled; backed by *bufPtr
	bufPtr *[]byte // owner pointer; required to Put back to sync.Pool
	err    error   // nil on success or pure EOF chunk
}

// newPrefetchReader spawns parallelism workers and primes the first
// parallelism chunk fetches. Caller MUST call Close to release worker
// goroutines and pool buffers.
func newPrefetchReader(fetch fetchFunc, startOff, size int64, chunkSize, parallelism int) *prefetchReader {
	if chunkSize <= 0 {
		chunkSize = 4 * 1024 * 1024
	}
	if parallelism <= 0 {
		parallelism = 4
	}
	if size < 0 {
		size = 0
	}

	totalChunks := (size + int64(chunkSize) - 1) / int64(chunkSize)
	if totalChunks == 0 {
		// Empty reader still has worker goroutines that just see ctx done
		// on Close. Keep the structure regular.
		totalChunks = 0
	}
	// Don't spawn more workers than chunks — pointless and complicates
	// EOF accounting downstream.
	if int64(parallelism) > totalChunks && totalChunks > 0 {
		parallelism = int(totalChunks)
	}
	if parallelism == 0 && totalChunks == 0 {
		// Zero-length file. Build a minimal reader that returns EOF
		// immediately without spawning workers.
		ctx, cancel := context.WithCancel(context.Background())
		return &prefetchReader{
			fetch:       fetch,
			startOff:    startOff,
			size:        size,
			chunkSize:   chunkSize,
			parallelism: 0,
			pending:     nil,
			totalChunks: 0,
			ctx:         ctx,
			cancel:      cancel,
		}
	}

	pr := &prefetchReader{
		fetch:       fetch,
		startOff:    startOff,
		size:        size,
		chunkSize:   chunkSize,
		parallelism: parallelism,
		bufPool: &sync.Pool{
			New: func() interface{} { b := make([]byte, chunkSize); return &b },
		},
		pending:     make(map[int64]*prefetchChunk),
		totalChunks: totalChunks,
		in:          make(chan *prefetchChunk, parallelism),
		// Sized so workers never block on send back-pressure for the
		// in-flight window; dispatch only ever has at most parallelism
		// outstanding entries because each Read consumption injects one
		// more.
		dispatch: make(chan int64, parallelism+1),
	}
	pr.ctx, pr.cancel = context.WithCancel(context.Background())

	for i := 0; i < parallelism; i++ {
		pr.wg.Add(1)
		go pr.workerLoop()
	}

	// Prime initial in-flight window. Each entry corresponds to one
	// dispatched (but not yet returned) chunk.
	for seq := int64(0); seq < int64(parallelism) && seq < totalChunks; seq++ {
		pr.dispatch <- seq
	}
	return pr
}

func (pr *prefetchReader) workerLoop() {
	defer pr.wg.Done()
	for {
		select {
		case <-pr.ctx.Done():
			return
		case seq, ok := <-pr.dispatch:
			if !ok {
				return
			}
			pr.serveOne(seq)
		}
	}
}

func (pr *prefetchReader) serveOne(seq int64) {
	// Compute byte range for this chunk.
	off := int64(seq) * int64(pr.chunkSize)
	if off >= pr.size {
		// Shouldn't happen with the dispatch gating in Read, but be
		// defensive: synthesize an EOF chunk so the consumer makes
		// progress instead of deadlocking on `in`.
		pr.sendOrAbandon(&prefetchChunk{seq: seq, err: io.EOF})
		return
	}
	want := pr.chunkSize
	if off+int64(want) > pr.size {
		want = int(pr.size - off)
	}

	bufPtr := pr.bufPool.Get().(*[]byte)
	chunk := &prefetchChunk{seq: seq, bufPtr: bufPtr}

	// io.ReadFull-style loop: a fetch that returns (n<want, nil) would
	// otherwise create a gap in the output stream because the next chunk
	// starts at (seq+1)*chunkSize. Retry until the chunk is full or the
	// underlying signals EOF / error.
	filled := 0
	for filled < want {
		n, err := pr.fetch((*bufPtr)[filled:want], pr.startOff+off+int64(filled))
		filled += n
		if err == nil {
			if n == 0 {
				// Misbehaving fetch — refuse to spin forever.
				chunk.err = errShortFetchLoop
				break
			}
			continue
		}
		// err != nil
		if err == io.EOF {
			// Partial fill at end of source is fine; carry the bytes
			// we did get, but DON'T mark this as an EOF chunk if we
			// got any data — the consumer needs them before EOF.
			break
		}
		chunk.err = err
		break
	}
	chunk.data = (*bufPtr)[:filled]
	if filled == 0 && chunk.err == nil {
		// Pure-EOF chunk: no data and no error to report. Release the
		// pooled buffer immediately; chunk.bufPtr=nil so the consumer
		// won't try to recycle it again.
		chunk.err = io.EOF
		pr.bufPool.Put(bufPtr)
		chunk.bufPtr = nil
	}
	pr.sendOrAbandon(chunk)
}

var errShortFetchLoop = errors.New("prefetchReader: fetch returned 0 bytes with nil error")

// sendOrAbandon delivers chunk on pr.in unless the reader has been Closed.
// On abandon, the pooled buffer (if any) is released so it can be reused
// for the next reader, instead of being held by an orphan goroutine.
func (pr *prefetchReader) sendOrAbandon(chunk *prefetchChunk) {
	select {
	case pr.in <- chunk:
	case <-pr.ctx.Done():
		if chunk.bufPtr != nil {
			pr.bufPool.Put(chunk.bufPtr)
			chunk.bufPtr = nil
		}
	}
}

// Read serves bytes from prefetched chunks in file order. It blocks until
// the next required chunk arrives. Caller goroutine ownership is the same
// as any io.Reader: do not call from multiple goroutines concurrently.
func (pr *prefetchReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if pr.terminalErr != nil {
		return 0, pr.terminalErr
	}
	if pr.size == 0 {
		return 0, io.EOF
	}

	// Fast path: current chunk has bytes left.
	if pr.cur != nil && pr.curPos < len(pr.cur.data) {
		n := copy(p, pr.cur.data[pr.curPos:])
		pr.curPos += n
		if pr.curPos >= len(pr.cur.data) {
			pr.recycle(pr.cur)
			pr.cur = nil
			pr.curPos = 0
			pr.scheduleAhead()
		}
		return n, nil
	}

	// All chunks consumed → EOF. Avoid blocking on a seq we never
	// dispatched.
	if pr.nextSeq >= pr.totalChunks {
		pr.terminalErr = io.EOF
		return 0, io.EOF
	}

	// Need to advance to the next sequence number.
	c, err := pr.acquireSeq(pr.nextSeq)
	if err != nil {
		pr.terminalErr = err
		return 0, err
	}
	pr.cur = c
	pr.curPos = 0
	pr.nextSeq++
	// Now serve from the freshly-acquired chunk.
	return pr.Read(p)
}

// acquireSeq returns the chunk for the given seq, draining pr.in into
// pr.pending until the needed seq appears. EOF / errors are converted to
// (nil, err) — io.EOF is normal termination, anything else is fatal.
func (pr *prefetchReader) acquireSeq(seq int64) (*prefetchChunk, error) {
	if c, ok := pr.pending[seq]; ok {
		delete(pr.pending, seq)
		return pr.unwrapChunk(c)
	}
	for {
		select {
		case <-pr.ctx.Done():
			return nil, io.ErrClosedPipe
		case c := <-pr.in:
			if c.seq == seq {
				return pr.unwrapChunk(c)
			}
			// Out-of-order arrival — stash it.
			pr.pending[c.seq] = c
		}
	}
}

func (pr *prefetchReader) unwrapChunk(c *prefetchChunk) (*prefetchChunk, error) {
	if c.err != nil && c.err != io.EOF {
		if c.bufPtr != nil {
			pr.bufPool.Put(c.bufPtr)
			c.bufPtr = nil
		}
		return nil, c.err
	}
	if len(c.data) == 0 {
		// Pure-EOF chunk.
		if c.bufPtr != nil {
			pr.bufPool.Put(c.bufPtr)
			c.bufPtr = nil
		}
		return nil, io.EOF
	}
	return c, nil
}

// recycle returns a chunk's buffer to the pool.
func (pr *prefetchReader) recycle(c *prefetchChunk) {
	if c == nil || c.bufPtr == nil {
		return
	}
	pr.bufPool.Put(c.bufPtr)
	c.bufPtr = nil
	c.data = nil
}

// scheduleAhead dispatches one more chunk to maintain the in-flight
// window of parallelism chunks. No-op once every chunk has been queued.
func (pr *prefetchReader) scheduleAhead() {
	next := pr.nextSeq + int64(pr.parallelism) - 1
	if next < 0 || next >= pr.totalChunks {
		return
	}
	select {
	case pr.dispatch <- next:
	case <-pr.ctx.Done():
	}
}

// Close stops all prefetch workers and releases pooled buffers. Safe to
// call multiple times. After Close, Read returns io.ErrClosedPipe.
func (pr *prefetchReader) Close() error {
	pr.closeOnce.Do(func() {
		pr.cancel()
		// Drain `in` in a goroutine so workers blocked on `pr.in <- chunk`
		// (rare — `in` is sized to the in-flight window) unblock and exit
		// promptly; otherwise their select would fall through to
		// ctx.Done() on the next iteration anyway.
		if pr.in != nil {
			go func() {
				for {
					select {
					case c := <-pr.in:
						if c != nil && c.bufPtr != nil {
							pr.bufPool.Put(c.bufPtr)
							c.bufPtr = nil
						}
					default:
						return
					}
				}
			}()
		}
		pr.wg.Wait()
		// Final drain after workers are done — they may have sent chunks
		// while the drainer was racing.
		if pr.in != nil {
			for {
				select {
				case c := <-pr.in:
					if c != nil && c.bufPtr != nil {
						pr.bufPool.Put(c.bufPtr)
						c.bufPtr = nil
					}
				default:
					goto doneDrain
				}
			}
		doneDrain:
		}
		// Release any pending out-of-order chunks and the current one.
		for _, c := range pr.pending {
			pr.recycle(c)
		}
		pr.pending = nil
		pr.recycle(pr.cur)
		pr.cur = nil
	})
	if pr.terminalErr != nil && !errors.Is(pr.terminalErr, io.EOF) {
		return pr.terminalErr
	}
	return nil
}
