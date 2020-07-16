package reactive

import "sync"

// node is the core of the observable dependency invalidation DAG and resource
// freeing DAG
//
// Each node has a set of dependencies, listed in in, and a set of dependees,
// listed in out. When a node becomes invalidated, so becomes all it dependees in
// out. When a node's out list becomes empty, it gets released and removed from
// it's dependencies' out lists, which might get released in turn.
//
// Releases and invalidations happen independently so that an invalidated
// computation can still keep a potentially expensive resource that has not yet
// been invalidated around and cached.
//
// Both releases and invalidations happen asynchronously, running in a Goroutine
// that triggered the initial release or invalidation.
//
// To interact with computations, nodes have an optional afterInvalidate and
// afterRelease handler.
//
// One tricky aspect of the reference counting is that releases will happen
// only on a transition from 1->0 out. That's because nodes start with empty
// out, and starting with a release would be bad. This means that for safety,
// all new nodes should have at least call to addOut.
//
// After an invalidation or release, the node still has a valid representation,
// but the code must be careful to not assume new nodes might be invalidated or
// released. Similarly, handlers should no longer be changed.
//
// To prevent dead locks, all operations hold locks on nodes only for brief
// periods of times (before recursing to other nodes). When a pair of nodes
// needs to be locked, the dependency node gets locked first to maintain an
// ordering over locks.
//
// All functions on node are thread-safe.
type node struct {
	mu sync.Mutex

	in  []*node
	out map[*node]struct{}

	invalidated bool
	released    bool

	afterInvalidate func()
	afterRelease    func()
}

// Invalidated returns if the node has been invalidated
func (n *node) Invalidated() bool {
	n.mu.Lock()
	result := n.invalidated
	n.mu.Unlock()
	return result
}

// strobe invalidates all child nodes while keeping the node itself valid
func (n *node) strobe() {
	// copy out to safely strobe without holding mu
	n.mu.Lock()
	out := make([]*node, 0, len(n.out))
	for to := range n.out {
		out = append(out, to)
	}
	n.mu.Unlock()

	for _, to := range out {
		to.invalidate()
	}
}

// invalidate invalidates node if it has not yet been invalidated
func (n *node) invalidate() {
	// check if we should invalidate, and figure out who we should invalidate
	n.mu.Lock()
	if n.invalidated {
		n.mu.Unlock()
		return
	}

	n.invalidated = true
	// Copy out to safely strobe without holding mu. We keep out around for
	// reference counting even after we are invalidated, but no new nodes will
	// be added so taking a snapshot is a safe operation.
	//
	// Released dependencies might removed themselves from out and still get
	// invalidated, but that's fine as releasing implies an invalidation.
	out := make([]*node, 0, len(n.out))
	for to := range n.out {
		out = append(out, to)
	}
	n.mu.Unlock()

	if n.afterInvalidate != nil {
		n.afterInvalidate()
	}

	// recursively invalidate dependencies
	for _, to := range out {
		to.invalidate()
	}
}

func (n *node) release() {
	n.invalidate()

	// check if we should release
	n.mu.Lock()
	if n.released {
		n.mu.Unlock()
		return
	}

	n.released = true
	n.mu.Unlock()

	if n.afterRelease != nil {
		n.afterRelease()
	}

	// we can access in safely as it will no longer be modified after we set
	// released to true
	for _, from := range n.in {
		// removes ourselves as a dependency and maybe recursively release
		from.mu.Lock()
		delete(from.out, n)
		shouldRelease := len(from.out) == 0
		from.mu.Unlock()

		if shouldRelease {
			from.release()
		}
	}
	// set in to nil to help garbage collection
	n.in = nil
}

// add registers that to depends on n, adding to to n's out
func (n *node) addOut(to *node) {
	// lock both nodes to atomically register the dependency
	// lock the dependency first to prevent deadlocks
	n.mu.Lock()
	to.mu.Lock()

	// register a dependency only if to has not yet been released
	//
	// it's fine to add a dependency if n has already been invalidated; from a
	// reference counting perspective, that's what we want
	if !to.released {
		if n.out == nil {
			n.out = make(map[*node]struct{})
		}
		n.out[to] = struct{}{}
		to.in = append(to.in, n)
	}

	// invalidate to if n is invalidated
	shouldInvalidate := n.invalidated && !to.invalidated
	// Release out if we did not add a dependency. This fulfills the contract
	// that after one call to addOut, n is guaranteed to be eventually released.
	shouldRelease := len(n.out) == 0

	to.mu.Unlock()
	n.mu.Unlock()

	if shouldInvalidate {
		go to.invalidate()
	}
	if shouldRelease {
		go n.release()
	}
}

func (n *node) handleInvalidate(f func()) {
	n.mu.Lock()
	if n.invalidated {
		go f()
	} else {
		if n.afterInvalidate != nil {
			panic(n)
		}
		n.afterInvalidate = f
	}
	n.mu.Unlock()
}

func (n *node) handleRelease(f func()) {
	n.mu.Lock()
	if n.released {
		go f()
	} else {
		if n.afterRelease != nil {
			panic(n)
		}
		n.afterRelease = f
	}
	n.mu.Unlock()
}
