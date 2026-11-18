package fs

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cubefs/cubefs/depends/bazil.org/fuse"
)

// fakeRequest implements fuse.Request without depending on a real kernel
// connection, so the active-request registry (reqs slice) can be exercised
// under -race in pure unit tests.
type fakeRequest struct {
	hdr       *fuse.Header
	responded int32
}

func (r *fakeRequest) Hdr() *fuse.Header { return r.hdr }
func (r *fakeRequest) RespondError(err error) {
	atomic.StoreInt32(&r.responded, 1)
}
func (r *fakeRequest) String() string { return "fakeRequest" }

func newServeServerForRace() *Server {
	return &Server{
		debug: func(interface{}) {},
		reqs:  make([]*serveRequest, 0, 1024),
	}
}

func (c *Server) reqsLen() int {
	c.reqMux.Lock()
	defer c.reqMux.Unlock()
	return len(c.reqs)
}

// registerOne registers a request and returns its serveRequest.
func registerOne(c *Server, id fuse.RequestID) *serveRequest {
	r := &fakeRequest{hdr: &fuse.Header{ID: id}}
	sr := &serveRequest{Request: r, cancel: func() {}}
	c.checkNode(r, sr)
	return sr
}

// TestServeReqRegistryDoubleDone reproduces the double-done corruption: a
// request unregistered twice (reachable via serveWithTimeOut timeout plus a
// late-successful handler) deletes a *different* live request. Before the
// doneOnce guard this left len(reqs)=1 instead of 2.
func TestServeReqRegistryDoubleDone(t *testing.T) {
	c := newServeServerForRace()
	a := registerOne(c, 1)
	b := registerOne(c, 2)
	cc := registerOne(c, 3)
	_ = b
	_ = cc
	done := c.done(a, a.Request.Hdr())
	done(nil) // first unregister
	done(nil) // second unregister (stale index) must be a no-op
	if n := c.reqsLen(); n != 2 {
		t.Fatalf("double-done corrupted registry: len=%d, want 2 (B/C must survive)", n)
	}
}

// TestServeReqRegistryConcurrentDoubleDone exercises the same scenario with
// two goroutines unregistering the same request concurrently, mirroring the
// serveWithTimeOut timeout path (done(ETIME)) racing the late-successful
// handler (done(nil)).
func TestServeReqRegistryConcurrentDoubleDone(t *testing.T) {
	c := newServeServerForRace()
	a := registerOne(c, 1)
	b := registerOne(c, 2)
	cc := registerOne(c, 3)
	d := registerOne(c, 4)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // timeout path
		defer wg.Done()
		c.done(a, a.Request.Hdr())(fuse.ETIME)
	}()
	go func() { // late-successful handler
		defer wg.Done()
		c.done(a, a.Request.Hdr())(nil)
	}()
	wg.Wait()

	if n := c.reqsLen(); n != 3 {
		t.Fatalf("concurrent double-done corrupted registry: len=%d, want 3 (B/C/D must survive)", n)
	}
	// survivors can still be unregistered normally
	for _, sr := range []*serveRequest{b, cc, d} {
		c.done(sr, sr.Request.Hdr())(nil)
	}
	if n := c.reqsLen(); n != 0 {
		t.Fatalf("reqs leaked after survivors done: len=%d, want 0", n)
	}
}

// TestServeReqRegistryConcurrent stresses the registry with concurrent
// register/unregister of distinct requests: no race, no leak.
func TestServeReqRegistryConcurrent(t *testing.T) {
	c := newServeServerForRace()
	const N = 2000
	const G = 16
	var wg sync.WaitGroup
	for g := 0; g < G; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < N; i++ {
				r := &fakeRequest{hdr: &fuse.Header{ID: fuse.RequestID(base + i)}}
				sr := &serveRequest{Request: r, cancel: func() {}}
				c.checkNode(r, sr)
				c.done(sr, r.Hdr())(nil)
			}
		}(g * N)
	}
	wg.Wait()
	if n := c.reqsLen(); n != 0 {
		t.Fatalf("reqs leaked: len=%d, want 0", n)
	}
}

// TestServeInterruptConcurrent exercises the InterruptRequest lookup (walking
// reqs to cancel a target) racing concurrent unregister: no race.
func TestServeInterruptConcurrent(t *testing.T) {
	c := newServeServerForRace()
	const N = 1000
	srs := make([]*serveRequest, N)
	for i := 0; i < N; i++ {
		srs[i] = registerOne(c, fuse.RequestID(i+1))
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		target := fuse.RequestID(500)
		for c.reqsLen() > 0 {
			c.reqMux.Lock()
			for _, ireq := range c.reqs {
				if target == ireq.Request.Hdr().ID && ireq.cancel != nil {
					ireq.cancel()
					ireq.cancel = nil
				}
			}
			c.reqMux.Unlock()
		}
	}()
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := base; i < N; i += 8 {
				c.done(srs[i], srs[i].Request.Hdr())(nil)
			}
		}(g)
	}
	wg.Wait()
	if n := c.reqsLen(); n != 0 {
		t.Fatalf("reqs leaked: len=%d, want 0", n)
	}
}
