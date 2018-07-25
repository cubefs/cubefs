package blob

import (
	"fmt"
	"github.com/juju/errors"
	"io"
	"net"
	"time"

	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
)

type DataConn struct {
	conn *net.TCPConn
	id   uint64 //Data Partition ID
	addr string //Data Node Addr
}

func (dc *DataConn) String() string {
	return fmt.Sprintf("DataPID(%v) Addr(%v)", dc.id, dc.addr)
}

func (client *BlobClient) sendToDataPartition(dp *DataPartition, req *proto.Packet) (resp *proto.Packet, err error) {
	var (
		addrs []string
		dc    *DataConn
		retry int
	)

	// Write should retry only the leader, and Read should retry all the hosts.
	if isReadPacket(req) {
		addrs = append([]string{}, dp.Hosts...)
		retry = ReadRetryLimit
	} else {
		addrs = append([]string{}, dp.Hosts[0])
		retry = WriteRetryLimit
	}

	for i := 0; i < retry; i++ {
		for _, addr := range addrs {
			dc, err = client.getConn(dp.PartitionID, addr)
			if err != nil {
				continue
			}
			resp, err = dc.send(req)
			client.putConn(dc, err)
			if err == nil && !resp.ShallRetry() {
				break
			}
			log.LogErrorf("sendToDataPartition: dp(%v) dc(%v) err(%v)", dp, dc, err)
		}
		time.Sleep(SendRetryInterval)
	}

	if err != nil {
		return nil, errors.New(fmt.Sprintf("sendToDataPartition faild: dp(%v) op(%v)", dp, req.GetOpMsg()))
	}
	log.LogDebugf("sendToDataPartition successful: dc(%v) op(%v) result(%v)", dc, req.GetOpMsg(), resp.GetResultMesg())
	return resp, nil
}

func (dc *DataConn) send(req *proto.Packet) (resp *proto.Packet, err error) {
	err = req.WriteToConn(dc.conn)
	if err != nil {
		return nil, errors.Annotatef(err, "Failed to write to conn")
	}
	resp = proto.NewPacket()
	err = resp.ReadFromConn(dc.conn, proto.ReadDeadlineTime)
	if err != nil && err != io.EOF {
		return nil, errors.Annotatef(err, "Failed to read from conn")
	}
	log.LogDebugf("send: dc(%v) result(%v) retry in (%v)", dc, resp.GetResultMesg(), SendRetryInterval)
	return resp, nil
}

func isReadPacket(pkt *proto.Packet) bool {
	return pkt.Opcode == proto.OpRead
}

func (client *BlobClient) getConn(pid uint64, addr string) (*DataConn, error) {
	conn, err := client.conns.Get(addr)
	if err != nil {
		log.LogErrorf("Get conn: addr(%v) err(%v)", addr, err)
		return nil, err
	}
	dc := &DataConn{conn: conn, id: pid, addr: addr}
	log.LogDebugf("Get connection: dc(%v)", dc)
	return dc, nil
}

func (client *BlobClient) putConn(dc *DataConn, err error) {
	if err != nil {
		client.conns.Put(dc.conn, true)
	} else {
		client.conns.Put(dc.conn, false)
	}
}
