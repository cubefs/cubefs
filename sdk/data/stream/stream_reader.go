// Copyright 2018 The Containerfs Authors.
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
	client  *ExtentClient
	extents *ExtentCache
	inode   uint64
	refcnt  uint64
	authid  uint64
	writer  *StreamWriter
	done    chan struct{}
	once    sync.Once
}

func NewStreamer(client *ExtentClient, inode uint64) *Streamer {
	stream := &Streamer{
		client:  client,
		extents: NewExtentCache(inode),
		inode:   inode,
	}
	return stream
}

func (s *Streamer) String() string {
	return fmt.Sprintf("inode(%v) extents(%v)", s.inode, s.extents.List())
}

func (stream *Streamer) GetExtents() error {
	return stream.extents.Refresh(stream.inode, stream.client.getExtents)
}

//TODO: use memory pool
func (stream *Streamer) GetExtentReader(ek *proto.ExtentKey) (*ExtentReader, error) {
	partition, err := gDataWrapper.GetDataPartition(ek.PartitionId)
	if err != nil {
		return nil, err
	}
	reader := NewExtentReader(stream.inode, ek, partition)
	return reader, nil
}

func (stream *Streamer) read(data []byte, offset int, size int) (total int, err error) {
	var (
		readBytes int
		reader    *ExtentReader
		requests  []*ExtentRequest
		flushed   bool
	)

	requests = stream.extents.PrepareReadRequest(offset, size, data)
	for _, req := range requests {
		if req.ExtentKey == nil {
			continue
		}
		if req.ExtentKey == nil || (req.ExtentKey.PartitionId == 0 && req.ExtentKey.ExtentId == 0) {
			if err = stream.client.Flush(stream.inode); err != nil {
				return 0, err
			}
			flushed = true
			break
		}
	}

	if flushed {
		requests = stream.extents.PrepareReadRequest(offset, size, data)
	}

	filesize, _ := stream.extents.Size()
	log.LogDebugf("stream read: ino(%v) requests(%v) filesize(%v)", stream.inode, requests, filesize)
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
			log.LogDebugf("Stream read hole: ino(%v) req(%v) total(%v)", stream.inode, req, total)
		} else {
			reader, err = stream.GetExtentReader(req.ExtentKey)
			if err != nil {
				break
			}
			readBytes, err = reader.Read(req)
			log.LogDebugf("Stream read: ino(%v) req(%v) readBytes(%v) err(%v)", stream.inode, req, readBytes, err)
			total += readBytes
			if err != nil || readBytes < req.Size {
				break
			}
		}
	}
	return
}
