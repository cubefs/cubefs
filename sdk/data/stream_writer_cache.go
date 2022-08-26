package data

import (
	"context"
	"fmt"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	PendingPacketFlushThreshold = 32
	PendingPacketMaxLength		= 64
)

func (s *Streamer) WritePendingPacket(data []byte, offset uint64, size int, direct bool) (ek *proto.ExtentKey, err error) {
	if len(s.pendingPacketList) > PendingPacketMaxLength {
		return nil, fmt.Errorf("ExtentHandler: pending packet is full, offset(%v) size(%v) eh(%v)", offset, size, s.handler)
	}
	blksize := util.BlockSize
	var (
		total, write 		int
		pendingPacketIndex 	int
		curPendingPacket	*Packet
	)
	// find index to insert
	pendingPacketIndex = 0
	for ; pendingPacketIndex < len(s.pendingPacketList); pendingPacketIndex++ {
		pendingPacket := s.pendingPacketList[pendingPacketIndex]
		if pendingPacket.KernelOffset + uint64(pendingPacket.Size) == offset {
			curPendingPacket = pendingPacket
			break
		}
		if pendingPacket.KernelOffset >= offset+uint64(size) {
			break
		}
	}
	var newPendingPacketList []*Packet
	newPendingPacketList = append(newPendingPacketList, s.pendingPacketList[:pendingPacketIndex]...)
	if curPendingPacket != nil {
		newPendingPacketList = append(newPendingPacketList, curPendingPacket)
		pendingPacketIndex++
	}
	// write current pending packet
	for total < size {
		if curPendingPacket == nil {
			curPendingPacket = NewWritePacket(context.Background(), s.handler.inode, offset+uint64(total), s.handler.storeMode, blksize)
			if direct {
				curPendingPacket.Opcode = proto.OpSyncWrite
			}
			newPendingPacketList = append(newPendingPacketList, curPendingPacket)
			if log.IsDebugEnabled() {
				log.LogDebugf("ExtentHandler WritePendingPacket: eh(%v) new pending packet(%v)", s.handler, curPendingPacket)
			}
		}
		packsize := int(curPendingPacket.Size)
		write = util.Min(size-total, blksize-packsize)
		if write > 0 {
			copy(curPendingPacket.Data[packsize:packsize+write], data[total:total+write])
			curPendingPacket.Size += uint32(write)
			total += write
		}
		if int(curPendingPacket.Size) >= blksize {
			curPendingPacket = nil
		}
	}
	// append the rest packets
	newPendingPacketList = append(newPendingPacketList, s.pendingPacketList[pendingPacketIndex:]...)
	if log.IsDebugEnabled() {
		log.LogDebugf("ExtentHandler WritePendingPacket: eh(%v) origin pending list(%v) new list(%v)", s.handler, s.pendingPacketList, newPendingPacketList)
	}
	s.pendingPacketList = newPendingPacketList
	ek = &proto.ExtentKey {
		FileOffset: uint64(offset),
		Size:       uint32(size),
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("ExtentHandler WritePendingPacket: eh(%v) pending packet length(%v)", s.handler, len(s.pendingPacketList))
	}
	return ek, nil
}

func (s *Streamer) FlushContinuousPendingPacket(ctx context.Context) (newPendingPacketList []*Packet) {
	for _, pendingPacket := range s.pendingPacketList {
		if log.IsDebugEnabled() {
			log.LogDebugf("FlushContinuousPendingPacket: eh(%v) packet(%v) check pending packet(%v)", s.handler, s.handler.packet, pendingPacket)
		}
		if pendingPacket.KernelOffset != s.handler.fileOffset + uint64(s.handler.size) {
			newPendingPacketList = append(newPendingPacketList, pendingPacket)
			continue
		}
		if log.IsDebugEnabled() {
			log.LogDebugf("FlushContinuousPendingPacket: eh(%v) continuous pending packet(%v) after flush packet(%v)", s.handler, pendingPacket, s.handler.packet)
		}
		// flush current packet
		s.handler.storeMode = proto.NormalExtentType
		s.handler.flushPacket(ctx)
		s.handler.size += int(pendingPacket.Size)
		s.handler.packet = pendingPacket
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("FlushContinuousPendingPacket: eh(%v) pending packet list(%v) replace with new list(%v)", s.handler, s.pendingPacketList, newPendingPacketList)
	}
	return newPendingPacketList
}

func (s *Streamer) FlushAllPendingPacket(ctx context.Context) {
	var pendingEh *ExtentHandler
	for _, pendingPacket := range s.pendingPacketList {
		if pendingEh != nil && pendingEh.fileOffset + uint64(pendingEh.size) != pendingPacket.KernelOffset {
			pendingEh.setClosed()
			pendingEh = nil
		}
		if pendingEh == nil {
			pendingEh = NewExtentHandler(s, pendingPacket.KernelOffset, proto.NormalExtentType, false)
			s.dirtylist.Put(pendingEh)
		}
		pendingEh.packet = pendingPacket
		pendingEh.size += int(pendingPacket.Size)
		pendingEh.flushPacket(ctx)
	}
	if pendingEh != nil {
		pendingEh.setClosed()
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("FlushAllPendingPacket: inode(%v) clean pending list(%v)", s.inode, s.pendingPacketList)
	}
	s.pendingPacketList = make([]*Packet, 0)
}

func (s *Streamer) OverwriteLocalPacket(req *ExtentRequest) bool {
	if s.client.EnableWriteCache() {
		if s.handler != nil {
			s.handler.overwriteLocalPacketMutex.Lock()
			if s.handler.packet != nil {
				if req.FileOffset >= s.handler.packet.KernelOffset && req.FileOffset + uint64(req.Size) <= s.handler.packet.KernelOffset + uint64(s.handler.packet.Size)  {
					s.doOverwriteLocalPacket(s.handler.packet, req)
					s.handler.overwriteLocalPacketMutex.Unlock()
					if log.IsDebugEnabled() {
						log.LogDebugf("OverwriteLocalPacket: eh(%v) packet(%v) request(%v)", s.handler, s.handler.packet, req)
					}
					return true
				}
			}
			s.handler.overwriteLocalPacketMutex.Unlock()
		}
		for _, pendingPacket := range s.pendingPacketList {
			if req.FileOffset >= pendingPacket.KernelOffset && req.FileOffset + uint64(req.Size) <= pendingPacket.KernelOffset + uint64(pendingPacket.Size)  {
				s.doOverwriteLocalPacket(pendingPacket, req)
				if log.IsDebugEnabled() {
					log.LogDebugf("OverwriteLocalPacket: eh(%v) packet(%v) request(%v) pendingPacketList(%v)",
						s.handler, pendingPacket, req, len(s.pendingPacketList))
				}
				return true
			}
		}
	}
	return false
}

func (s *Streamer) doOverwriteLocalPacket(packet *Packet, req *ExtentRequest) {
	copy(packet.Data[(req.FileOffset-packet.KernelOffset):(req.FileOffset+uint64(req.Size)-packet.KernelOffset)], req.Data[:req.Size])
}