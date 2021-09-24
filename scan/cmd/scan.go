package cmd

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/stream"
	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"net"
	"strings"
)

type Scan struct {
	mc *masterSDK.MasterClient
}

func NewScan(master string) *Scan {
	var masters = strings.Split(master, ",")
	masterClient := masterSDK.NewMasterClient(masters, false)
	sc := &Scan{
		mc: masterClient,
	}
	return sc
}

func (s *Scan) ReadByPartitionIdAndExtentId(partitionId, extentId uint64) (err error) {
	dataPartitionInfo, err := s.mc.AdminAPI().GetDataPartition("", partitionId)
	if err != nil {
		log.LogErrorf("GetDataPartition failed! e(%v)", err)
	}
	size := util.ExtentSize
	ek := &proto.ExtentKey{
		PartitionId:  partitionId,
		ExtentId:     extentId,
		ExtentOffset: 0,
		FileOffset:   0,
		Size:         uint32(size),
	}
	fmt.Printf("starting bad_crc[dpid:%v, eid:%v]\n", partitionId, extentId)
	result := make([][]byte, len(dataPartitionInfo.Hosts))
	h := make([]string, len(dataPartitionInfo.Hosts))
	for i, host := range dataPartitionInfo.Hosts {
		h[i] = host
		reqPacket := stream.NewReadPacket(ek, 0, size, 0, 0, true)
		var combine bytes.Buffer
		conn := stream.NewStreamConnByHost(host)
		err := conn.Send(reqPacket, func(conn *net.TCPConn) (error, bool) {
			readBytes := 0
			for readBytes < size {
				replyPacket := stream.NewReply(reqPacket.ReqID, partitionId, extentId)
				bufSize := util.Min(util.ReadBlockSize, size-readBytes)
				replyPacket.Size = uint32(bufSize)
				e := replyPacket.ReadFromConn(conn, proto.ReadDeadlineTime)
				if e != nil {
					log.LogWarnf("ReadByPartitionIdAndExtentId: failed to read from connect, partitionId(%v) extentId(%v) req(%v) readBytes(%v) err(%v)", partitionId, extentId, reqPacket, readBytes, e)
					return nil, false
				}
				combine.Write(replyPacket.Data)
				if replyPacket.ResultCode == proto.OpAgain {
					return nil, true
				}
				readBytes += int(replyPacket.Size)
			}
			return nil, false
		})
		if err != nil {
			log.LogErrorf("ReadByPartitionIdAndExtentId: failed to send, partitionId(%v) extentId(%v)", partitionId, extentId)
			return nil
		}
		result[i] = combine.Bytes()
	}

	flag := compare(result, h)

	if !flag {
		fmt.Printf("bad_crc[dpid:%v, eid:%v], replicas nomatch.\n", partitionId, extentId)
		log.LogErrorf("ReadByPartitionIdAndExtentId is inconsistent: partitionId(%v) extentId(%v)", partitionId, extentId)
	} else {
		fmt.Printf("bad_crc[dpid:%v, eid:%v], replicas match.\n", partitionId, extentId)
	}

	return nil
}

func compare(result [][]byte, host []string) bool {
	com := make([][16]byte, len(result))
	for i, r := range result {
		has := md5.Sum(r)
		com[i] = has
		fmt.Printf("%v md5 = %v fsize:%v \n", host[i], has, len(result[i]))
	}
	for i := 1; i < len(com); i++ {
		if com[0] != com[i] {
			return false
		}
	}
	return true
}
