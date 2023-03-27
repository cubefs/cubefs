package data

import (
	"bytes"
	"encoding/binary"

	"github.com/cubefs/cubefs/datanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore/walreader/common"
	"github.com/cubefs/cubefs/util/errors"
)

const (
	DecoderName = "data"
)

const (
	columnWidthSync   = 4
	columnWidthExtent = 6
	columnWidthOffset = 10
	columnWidthSize   = 10
	columnWidthCrc    = 10
)

type RndWrtOpItem struct {
	opcode   uint8
	extentID uint64
	offset   int64
	size     int64
	data     []byte
	crc      uint32
}

func UnmarshalRandWriteRaftLog(raw []byte) (opItem *RndWrtOpItem, err error) {
	opItem = new(RndWrtOpItem)
	buff := bytes.NewBuffer(raw)
	var version uint32
	if err = binary.Read(buff, binary.BigEndian, &version); err != nil {
		return
	}

	if version != datanode.BinaryMarshalMagicVersion {
		err = errors.New("old version command")
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.opcode); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.extentID); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.offset); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.crc); err != nil {
		return
	}
	opItem.data = make([]byte, opItem.size)
	if _, err = buff.Read(opItem.data); err != nil {
		return
	}
	return
}

type DataCommandDecoder struct {
}

func (*DataCommandDecoder) Name() string {
	return DecoderName
}

func (*DataCommandDecoder) Header() common.ColumnValues {
	var values = common.NewColumnValues(
		common.ColumnValue{Value: "EXTENT", Width: columnWidthExtent},
		common.ColumnValue{Value: "OFFSET", Width: columnWidthOffset},
		common.ColumnValue{Value: "SIZE", Width: columnWidthSize},
		common.ColumnValue{Value: "CRC", Width: columnWidthCrc},
		common.ColumnValue{Value: "SYNC", Width: columnWidthSync},
	)
	return values
}

func (*DataCommandDecoder) DecodeCommand(command []byte) (values common.ColumnValues, err error) {
	var opItem *RndWrtOpItem
	if opItem, err = UnmarshalRandWriteRaftLog(command); err != nil {
		return
	}
	values = common.NewColumnValues(
		common.ColumnValue{Value: opItem.extentID, Width: columnWidthExtent},
		common.ColumnValue{Value: opItem.offset, Width: columnWidthOffset},
		common.ColumnValue{Value: opItem.size, Width: columnWidthSize},
		common.ColumnValue{Value: opItem.crc, Width: columnWidthCrc},
		common.ColumnValue{Value: opItem.opcode == proto.OpSyncRandomWrite, Width: columnWidthSync},
	)
	return
}
