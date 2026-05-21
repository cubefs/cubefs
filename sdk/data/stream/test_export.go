// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package stream

// NewTestExtentClient returns a minimal ExtentClient for cross-package unit tests.
func NewTestExtentClient(getExtents GetExtentsFunc) *ExtentClient {
	return &ExtentClient{
		streamers:  make(map[uint64]*Streamer),
		getExtents: getExtents,
	}
}

// RegisterTestStreamer attaches a streamer used by RefreshExtentsCache / ForceRefreshExtentsCache tests.
func (client *ExtentClient) RegisterTestStreamer(s *Streamer) {
	if client.streamers == nil {
		client.streamers = make(map[uint64]*Streamer)
	}
	client.streamers[s.inode] = s
}

// NewTestStreamer builds a streamer wired to client for extent refresh tests.
func NewTestStreamer(client *ExtentClient, inode uint64) *Streamer {
	return &Streamer{
		inode:     inode,
		client:    client,
		extents:   NewExtentCache(inode),
		dirtylist: NewDirtyExtentList(),
		isOpen:    true,
	}
}
