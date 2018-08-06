package metanode

import "github.com/chubaoio/cbfs/proto"

type Packet struct {
	proto.Packet
}

// For send delete request to dataNode
func NewExtentDeletePacket(dp *DataPartition, extentId uint64) *Packet {
	p := new(Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpMarkDelete
	p.StoreMode = proto.ExtentStoreMode
	p.PartitionID = dp.PartitionID
	p.FileID = extentId
	p.ReqID = proto.GetReqID()
	p.Nodes = uint8(len(dp.Hosts) - 1)
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.Arglen = uint32(len(p.Arg))

	return p
}
