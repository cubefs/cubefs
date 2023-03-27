package meta_dbback

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/raftstore/walreader/common"
	"github.com/cubefs/cubefs/raftstore/walreader/decode/meta_dbback/metanode"
	"github.com/cubefs/cubefs/raftstore/walreader/decode/meta_dbback/proto"
)

const (
	opFSMCreateInode uint32 = iota
	opFSMDeleteInode
	opFSMCreateDentry
	opFSMDeleteDentry
	opFSMOpen
	opFSMDeletePartition
	opFSMUpdatePartition
	opFSMOfflinePartition
	opFSMExtentsAdd
	opFSMStoreTick
	opFSMStartStoreTick
	opFSMStopStoreTick
	opFSMUpdateDentry
	opFSMExtentTruncate
	opFSMCreateLinkInode
	opFSMEvictInode
	opFSMInternalDeleteInode
	opFSMSyncCursor
)

const (
	DecoderName = "meta_dbback"
)

const (
	columnWidthOp    = 24
	columnWidthAttrs = 0
)

type MetadataOpKvData struct {
	Op uint32 `json:"op"`
	K  []byte `json:"k"`
	V  []byte `json:"v"`
}

type MetadataCommandDecoder struct {
}

func (*MetadataCommandDecoder) Name() string {
	return DecoderName
}

func (*MetadataCommandDecoder) Header() common.ColumnValues {
	var values = common.NewColumnValues()
	values.Add(
		common.ColumnValue{Value: "OP", Width: columnWidthOp},
		common.ColumnValue{Value: "ATTRIBUTES", Width: columnWidthAttrs},
	)
	return values
}

func (decoder *MetadataCommandDecoder) DecodeCommand(command []byte) (values common.ColumnValues, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
			return
		}
	}()

	var opKVData = new(MetadataOpKvData)
	if err = json.Unmarshal(command, opKVData); err != nil {
		return
	}

	var (
		columnValOp    = common.ColumnValue{Width: columnWidthOp}
		columnValAttrs = common.ColumnValue{Width: columnWidthAttrs}
	)

	switch opKVData.Op {
	case opFSMCreateInode:
		ino := &metanode.Inode{}
		if err = ino.Unmarshal(opKVData.V); err != nil {
			return
		}
		columnValOp.SetValue("CreateInode")
		columnValAttrs.SetValue(fmt.Sprintf("inode: %v", ino.Inode))
	case opFSMDeleteInode:
		ino := &metanode.Inode{}
		if err = ino.Unmarshal(opKVData.V); err != nil {
			return
		}
		columnValOp.SetValue("DeleteInode")
		columnValAttrs.SetValue(fmt.Sprintf("inode: %v", ino.Inode))
	case opFSMCreateDentry:
		den := &metanode.Dentry{}
		if err = den.Unmarshal(opKVData.V); err != nil {
			return
		}
		columnValOp.SetValue("CreateDentry")
		columnValAttrs.SetValue(fmt.Sprintf("parent: %v, name: %v, inode: %v, type: %v", den.ParentId, den.Name, den.Inode, den.Type))
	case opFSMDeleteDentry:
		den := &metanode.Dentry{}
		if err = den.Unmarshal(opKVData.V); err != nil {
			return
		}
		columnValOp.SetValue("DeleteDentry")
		columnValAttrs.SetValue(fmt.Sprintf("parent: %v, name: %v", den.ParentId, den.Name))
	case opFSMExtentTruncate:
		ino := metanode.Inode{}
		if err = ino.Unmarshal(opKVData.V); err != nil {
			return
		}
		columnValOp.SetValue("ExtentTruncate")
		columnValAttrs.SetValue(fmt.Sprintf("inode: %v, size: %v", ino.Inode, ino.Size))
	case opFSMCreateLinkInode:
		ino := metanode.Inode{}
		if err = ino.Unmarshal(opKVData.V); err != nil {
			return
		}
		columnValOp.SetValue("LinkInode")
		columnValAttrs.SetValue(fmt.Sprintf("inode: %v", ino.Inode))
	case opFSMEvictInode:
		ino := metanode.Inode{}
		if err = ino.Unmarshal(opKVData.V); err != nil {
			return
		}
		columnValOp.SetValue("EvictInode")
		columnValAttrs.SetValue(fmt.Sprintf("inode: %v", ino.Inode))
	case opFSMInternalDeleteInode:
		buf := bytes.NewBuffer(opKVData.V)
		sb := strings.Builder{}
		ino := metanode.Inode{}
		for {
			err = binary.Read(buf, binary.BigEndian, &ino.Inode)
			if err != nil {
				if err == io.EOF {
					err = nil
					break
				}
				return
			}
			if sb.Len() > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%v", ino.Inode))
		}
		columnValOp.SetValue("InternalDeleteInode")
		columnValAttrs.SetValue(fmt.Sprintf("inodes: %v", sb.String()))
	case opFSMExtentsAdd:
		ino := metanode.Inode{}
		if err = ino.Unmarshal(opKVData.V); err != nil {
			return
		}
		columnValOp.SetValue("ExtentsAdd")
		columnValAttrs.SetValue(fmt.Sprintf("inode: %v, eks: %v", ino.Inode, decoder.formatExtentKeys(ino.Extents)))
	case opFSMSyncCursor:
		var cursor uint64
		cursor = binary.BigEndian.Uint64(opKVData.V)
		columnValOp.SetValue("SyncCursor")
		columnValAttrs.SetValue(fmt.Sprintf("cursor: %v", cursor))
	case opFSMStoreTick:
		columnValOp.SetValue("StoreTick")
	case opFSMStartStoreTick:
		columnValOp.SetValue("StartStoreTick")
	case opFSMStopStoreTick:
		columnValOp.SetValue("StopStoreTick")
	case opFSMUpdateDentry:
		den := &metanode.Dentry{}
		if err = den.Unmarshal(opKVData.V); err != nil {
			return
		}
		columnValOp.SetValue("UpdateDentry")
		columnValAttrs.SetValue(fmt.Sprintf("parent: %v, name: %v, inode: %v, type: %v", den.ParentId, den.Name, den.Inode, den.Type))
	default:
		columnValOp.SetValue(strconv.Itoa(int(opKVData.Op)))
		columnValAttrs.SetValue("N/A")
	}
	values = common.NewColumnValues()
	values.Add(columnValOp, columnValAttrs)
	return
}

func (decoder *MetadataCommandDecoder) formatExtentKeys(extents *proto.StreamKey) string {
	sb := strings.Builder{}
	for _, ek := range extents.Extents {
		if sb.Len() > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%v_%v_%v_%v", ek.PartitionId, ek.ExtentId, ek.ExtentOffset, ek.Size))
	}
	return sb.String()
}
