// Copyright 2018 The Container File System Authors.
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

package stream

import (
	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"io"
	"sync"
)

type Streamer struct {
	client *ExtentClient
	inode  uint64

	status int32

	refcnt       int
	openWriteCnt int
	authid       uint64

	extents *ExtentCache
	once    sync.Once

	handler   *ExtentHandler   // current open handler
	dirtylist *ExtentDirtyList // dirty handlers
	dirty     bool             // whether current open handler is in the dirty list

	request chan interface{} // request channel, write/flush/close
	done    chan struct{}    // stream writer is being closed
}

func NewStreamer(client *ExtentClient, inode uint64) *Streamer {
	s := new(Streamer)
	s.client = client
	s.inode = inode
	s.extents = NewExtentCache(inode)
	s.request = make(chan interface{}, 1000)
	s.done = make(chan struct{})
	s.dirtylist = NewExtentDirtyList()
	go s.server()
	return s
}

func (s *Streamer) String() string {
	return fmt.Sprintf("Streamer{ino(%v)}", s.inode)
}

func (s *Streamer) GetExtents() error {
	return s.extents.Refresh(s.inode, s.client.getExtents)
}

//TODO: use memory pool
func (s *Streamer) GetExtentReader(ek *proto.ExtentKey) (*ExtentReader, error) {
	partition, err := gDataWrapper.GetDataPartition(ek.PartitionId)
	if err != nil {
		return nil, err
	}
	reader := NewExtentReader(s.inode, ek, partition)
	return reader, nil
}

func (s *Streamer) read(data []byte, offset int, size int) (total int, err error) {
	var (
		readBytes int
		reader    *ExtentReader
		requests  []*ExtentRequest
		flushed   bool
	)

	requests = s.extents.PrepareReadRequest(offset, size, data)
	for _, req := range requests {
		if req.ExtentKey == nil {
			continue
		}
		if req.ExtentKey.PartitionId == 0 && req.ExtentKey.ExtentId == 0 {
			if err = s.IssueFlushRequest(); err != nil {
				return 0, err
			}
			flushed = true
			break
		}
	}

	if flushed {
		requests = s.extents.PrepareReadRequest(offset, size, data)
	}

	filesize, _ := s.extents.Size()
	log.LogDebugf("read: ino(%v) requests(%v) filesize(%v)", s.inode, requests, filesize)
	for _, req := range requests {
		if req.ExtentKey == nil {
			for i := range req.Data {
				req.Data[i] = 0
			}

			if req.FileOffset+req.Size > filesize {
				if req.FileOffset > filesize {
					return
				}
				req.Size = filesize - req.FileOffset
				total += req.Size
				err = io.EOF
				return
			}

			// Reading a hole, just fill zero
			total += req.Size
			log.LogDebugf("Stream read hole: ino(%v) req(%v) total(%v)", s.inode, req, total)
		} else {
			reader, err = s.GetExtentReader(req.ExtentKey)
			if err != nil {
				break
			}
			readBytes, err = reader.Read(req)
			log.LogDebugf("Stream read: ino(%v) req(%v) readBytes(%v) err(%v)", s.inode, req, readBytes, err)
			total += readBytes
			if err != nil || readBytes < req.Size {
				break
			}
		}
	}
	return
}
