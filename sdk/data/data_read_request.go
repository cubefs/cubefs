package data

import (
	"context"
	"fmt"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

type ReadRequest struct {
	Req       *ExtentRequest
	Partition *DataPartition
}

func (req *ReadRequest) String() string {
	if req == nil {
		return ""
	}
	return fmt.Sprintf("req(%v) host(%v)", req.Req, req.Partition)
}

func (client *ExtentClient) GetReadRequests(ctx context.Context, inode uint64, data []byte, offset uint64, size int) (readRequests []*ReadRequest, fileSize uint64, err error) {
	if size == 0 {
		return
	}

	if client.dataWrapper.volNotExists {
		err = proto.ErrVolNotExists
		return
	}

	s := client.GetStreamer(inode)
	if s == nil {
		err = fmt.Errorf("Read: stream is not opened yet, ino(%v) offset(%v) size(%v)", inode, offset, size)
		return
	}

	s.once.Do(func() {
		s.GetExtents(ctx)
	})

	var (
		requests        []*ExtentRequest
		revisedRequests []*ExtentRequest
	)

	requests, fileSize = s.extents.PrepareRequests(offset, size, data)
	for _, req := range requests {
		if req.ExtentKey == nil || req.ExtentKey.PartitionId > 0 {
			continue
		}
		s.writeLock.Lock()
		if err = s.IssueFlushRequest(ctx); err != nil {
			s.writeLock.Unlock()
			return
		}
		revisedRequests, fileSize = s.extents.PrepareRequests(offset, size, data)
		s.writeLock.Unlock()
		break
	}

	if revisedRequests != nil {
		requests = revisedRequests
	}

	if log.IsDebugEnabled() {
		log.LogDebugf("Stream read: ino(%v) userExpectOffset(%v) userExpectSize(%v) requests(%v) filesize(%v)", s.inode, offset, size, requests, fileSize)
	}
	for _, req := range requests {
		var readReq *ReadRequest
		if req.ExtentKey == nil {
			readReq = &ReadRequest{Req: req}
		} else if req.ExtentKey.StoreType == proto.InnerData {
			err = fmt.Errorf("Read: can't read extent of inner type, ino(%v) offset(%v) size(%v)", inode, offset, size)
			return
		} else {
			partition, err := s.client.dataWrapper.GetDataPartition(req.ExtentKey.PartitionId)
			if err != nil {
				log.LogWarnf("GetReadRequests: get dp err(%v) pid(%v)", err, req.ExtentKey.PartitionId)
				return nil, 0, err
			}
			readReq = &ReadRequest{Req: req, Partition: partition}
		}
		readRequests = append(readRequests, readReq)
	}
	return
}
