//go:build linux && rdma

package rdma

import (
	"fmt"
	"sync"
	"time"
)

// RDMAPoolConfig configures a per-target-address connection pool.
type RDMAPoolConfig struct {
	Device      string
	Port        int
	NumSlots    int
	SlotSize    int
	MaxConns    int
	IdleTimeout time.Duration
}

// singlePool manages connections to one remote address.
type singlePool struct {
	mu       sync.Mutex
	idle     []*RDMAConn
	active   int
	maxConns int
	addr     string
	cfg      RDMAConnConfig
}

func (p *singlePool) get() (*RDMAConn, error) {
	p.mu.Lock()
	// Return an idle connection if available
	for len(p.idle) > 0 {
		c := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		if !c.IsClosed() {
			p.active++
			p.mu.Unlock()
			return c, nil
		}
	}
	// Create a new connection if under limit
	if p.maxConns > 0 && p.active >= p.maxConns {
		p.mu.Unlock()
		return nil, fmt.Errorf("rdma: connection pool for %s at capacity (%d)", p.addr, p.maxConns)
	}
	p.active++
	p.mu.Unlock()

	c, err := Dial(p.addr, p.cfg)
	if err != nil {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
		return nil, err
	}
	return c, nil
}

func (p *singlePool) put(c *RDMAConn, forceClose bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active--
	if forceClose || c.IsClosed() {
		c.Close()
		return
	}
	p.idle = append(p.idle, c)
}

func (p *singlePool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.idle {
		c.Close()
	}
	p.idle = nil
}

// RDMAConnPool manages RDMA connections to multiple DataNode addresses.
// API mirrors util.ConnectPool for easy caller-side symmetry.
type RDMAConnPool struct {
	mu    sync.RWMutex
	pools map[string]*singlePool
	cfg   RDMAPoolConfig
}

// NewRDMAConnPool creates a connection pool. Call Close when done.
func NewRDMAConnPool(cfg RDMAPoolConfig) (*RDMAConnPool, error) {
	if cfg.NumSlots <= 0 || cfg.NumSlots > maxSlots {
		return nil, fmt.Errorf("rdma: NumSlots %d out of range [1,%d]", cfg.NumSlots, maxSlots)
	}
	if cfg.SlotSize <= 0 {
		return nil, fmt.Errorf("rdma: SlotSize must be positive")
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 4
	}
	return &RDMAConnPool{
		pools: make(map[string]*singlePool),
		cfg:   cfg,
	}, nil
}

// GetConnect returns a connection to addr, creating one if needed.
// addr must be "host:port" of the DataNode's RDMA listener.
func (p *RDMAConnPool) GetConnect(addr string) (*RDMAConn, error) {
	p.mu.RLock()
	sp := p.pools[addr]
	p.mu.RUnlock()

	if sp == nil {
		p.mu.Lock()
		sp = p.pools[addr]
		if sp == nil {
			sp = &singlePool{
				addr:     addr,
				maxConns: p.cfg.MaxConns,
				cfg: RDMAConnConfig{
					NumSlots: p.cfg.NumSlots,
					SlotSize: p.cfg.SlotSize,
				},
			}
			p.pools[addr] = sp
		}
		p.mu.Unlock()
	}
	return sp.get()
}

// PutConnect returns c to the pool. Set forceClose to true to discard the connection.
func (p *RDMAConnPool) PutConnect(c *RDMAConn, forceClose bool) {
	p.mu.RLock()
	sp := p.pools[c.RemoteAddr()]
	p.mu.RUnlock()
	if sp != nil {
		sp.put(c, forceClose)
	} else {
		c.Close()
	}
}

// Close closes all connections in the pool.
func (p *RDMAConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, sp := range p.pools {
		sp.closeAll()
	}
	p.pools = make(map[string]*singlePool)
}
