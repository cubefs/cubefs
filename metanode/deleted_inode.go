package metanode

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/chubaofs/chubaofs/util/log"
)

const (
	DeletedInodeTimeStampLen   = 8
	DeletedInodeExpiredFlagLen = 1
)

type DeletedINodeBatch []*DeletedINode

type DeletedINode struct {
	Inode
	Timestamp int64
	IsExpired bool
}

func NewDeletedInode(ino *Inode, timestamp int64) *DeletedINode {
	di := new(DeletedINode)
	di.Inode.Inode = ino.Inode
	di.version = ino.version
	di.Type = ino.Type
	di.Uid = ino.Uid
	di.Gid = ino.Gid
	di.Size = ino.Size
	di.Generation = ino.Generation
	di.CreateTime = ino.CreateTime
	di.AccessTime = ino.AccessTime
	di.ModifyTime = ino.ModifyTime
	di.LinkTarget = ino.LinkTarget
	di.Flag = ino.Flag
	di.Reserved = ino.Reserved
	di.Extents = ino.Extents
	di.InnerDataSet = ino.InnerDataSet
	di.Timestamp = timestamp
	di.NLink = ino.NLink
	di.IsExpired = false
	return di
}

/* The function does not initialize Extents, so be careful */
func NewDeletedInodeByID(ino uint64) *DeletedINode {
	di := new(DeletedINode)
	di.Inode.Inode = ino
	return di
}

func (di *DeletedINode) Less(than BtreeItem) bool {
	ino, ok := than.(*DeletedINode)
	if !ok {
		log.LogDebugf("ThanTypeIsError, %v", reflect.TypeOf(than))
		return false
	}

	if di.Inode.Inode < ino.Inode.Inode {
		return true
	}

	return false
}

func (di *DeletedINode) Copy() BtreeItem {
	newIno := new(DeletedINode)
	newIno.Inode.Inode = di.Inode.Inode
	newIno.version = di.version
	newIno.Type = di.Type
	newIno.Uid = di.Uid
	newIno.Gid = di.Gid
	newIno.Size = di.Size
	newIno.Generation = di.Generation
	newIno.CreateTime = di.CreateTime
	newIno.ModifyTime = di.ModifyTime
	newIno.AccessTime = di.AccessTime
	if size := len(di.LinkTarget); size > 0 {
		newIno.LinkTarget = make([]byte, size)
		copy(newIno.LinkTarget, di.LinkTarget)
	}
	newIno.NLink = di.NLink
	newIno.Flag = di.Flag
	newIno.Reserved = di.Reserved
	newIno.Extents = di.Extents.Clone()
	newIno.Timestamp = di.Timestamp
	newIno.IsExpired = di.IsExpired
	newIno.InnerDataSet = di.InnerDataSet.Clone()
	return newIno
}

func (di *DeletedINode) buildInode() (ino *Inode) {
	ino = NewInode(0, 0)
	ino.Inode = di.Inode.Inode
	ino.Type = di.Type
	ino.Uid = di.Uid
	ino.Gid = di.Gid
	ino.Size = di.Size
	ino.Generation = di.Generation
	ino.CreateTime = di.CreateTime
	ino.AccessTime = di.AccessTime
	ino.ModifyTime = di.ModifyTime
	ino.LinkTarget = di.LinkTarget
	ino.NLink = di.NLink
	ino.Flag = di.Flag
	ino.Reserved = di.Reserved
	ino.Extents = di.Extents
	ino.InnerDataSet = di.InnerDataSet
	return ino
}

/*
func (di *DeletedINode) MarshalToJSON() (data []byte, err error) {
	data, err = json.Marshal(di)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	return
}

func UnmarshalDeletedInodeFromJSON(data []byte) (di *DeletedINode, err error) {
	di = new(DeletedINode)
	err = json.Unmarshal(data, di)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	return
}

*/

func (di *DeletedINode) setExpired() {
	di.IsExpired = true
}

func (di *DeletedINode) String() string {
	buff := bytes.NewBuffer(nil)
	buff.Grow(128)
	buff.WriteString("DeletedInode{")
	buff.WriteString(fmt.Sprintf("Inode[%d]", di.Inode.Inode))
	buff.WriteString(fmt.Sprintf("Type[%d]", di.Type))
	buff.WriteString(fmt.Sprintf("Uid[%d]", di.Uid))
	buff.WriteString(fmt.Sprintf("Gid[%d]", di.Gid))
	buff.WriteString(fmt.Sprintf("Size[%d]", di.Size))
	buff.WriteString(fmt.Sprintf("Gen[%d]", di.Generation))
	buff.WriteString(fmt.Sprintf("CT[%d]", di.CreateTime))
	buff.WriteString(fmt.Sprintf("AT[%d]", di.AccessTime))
	buff.WriteString(fmt.Sprintf("MT[%d]", di.ModifyTime))
	buff.WriteString(fmt.Sprintf("LinkT[%s]", di.LinkTarget))
	buff.WriteString(fmt.Sprintf("NLink[%d]", di.NLink))
	buff.WriteString(fmt.Sprintf("Flag[%d]", di.Flag))
	buff.WriteString(fmt.Sprintf("Reserved[%d]", di.Reserved))
	if di.Extents != nil {
		buff.WriteString(fmt.Sprintf("Extents[%s]", di.Extents))
	}
	buff.WriteString(fmt.Sprintf("DeleteTime[%d]", di.Timestamp))
	buff.WriteString(fmt.Sprintf("IsExpired[%v]", di.IsExpired))
	buff.WriteString(fmt.Sprintf("Version[%v]", di.version))
	if di.InnerDataSet != nil {
		buff.WriteString(fmt.Sprintf("InnerDataSet[%s]", di.InnerDataSet))
	}
	buff.WriteString("}")
	return buff.String()
}

func (di *DeletedINode) Marshal() (result []byte, err error) {
	keyBytes := di.MarshalKey()
	valBytes := di.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0, keyLen + valLen + 8))
	if err = binary.Write(buff, binary.BigEndian, keyLen); err != nil {
		return
	}
	if _, err = buff.Write(keyBytes); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, valLen); err != nil {
		return
	}
	if _, err = buff.Write(valBytes); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

func (di *DeletedINode) MarshalKey() (k []byte) {
	k = make([]byte, BaseInodeKeyLen)
	binary.BigEndian.PutUint64(k, di.Inode.Inode)
	return
}

func (di *DeletedINode) MarshalValue() (val []byte) {
	var (
		err           error
		inodeValBytes []byte
	)
	di.RLock()
	defer di.RUnlock()

	switch di.Inode.version {
	case InodeMarshalVersion3:
		inodeValBytes, err = di.Inode.MarshalValueV3()
	default:
		panic("error version")
	}

	buff := bytes.NewBuffer(make([]byte, 0, len(inodeValBytes) + DeletedInodeTimeStampLen + DeletedInodeExpiredFlagLen))
	if err = binary.Write(buff, binary.BigEndian, inodeValBytes); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &di.Timestamp); err != nil {
		panic(err)
	}

	if err = binary.Write(buff, binary.BigEndian, &di.IsExpired); err != nil {
		panic(err)
	}

	val = buff.Bytes()
	return
}

func (di *DeletedINode) Unmarshal(ctx context.Context, raw []byte) (err error) {
	inodeValueLen := len(raw) - DeletedInodeTimeStampLen - DeletedInodeExpiredFlagLen
	if err = di.Inode.UnmarshalByVersion(ctx, raw[0:inodeValueLen]); err != nil {
		return
	}
	offset := inodeValueLen
	di.Timestamp = int64(binary.BigEndian.Uint64(raw[offset:offset+DeletedInodeTimeStampLen]))
	offset += 8
	di.IsExpired = raw[offset] != 0
	return
}

func (di *DeletedINode) UnmarshalKey(k []byte) (err error) {
	di.Inode.Inode = binary.BigEndian.Uint64(k)
	return
}

func (di *DeletedINode) UnmarshalValue(ctx context.Context, val []byte) (err error) {
	versionOffset := 0
	inodeV := binary.BigEndian.Uint32(val[versionOffset:versionOffset+4])
	switch inodeV {
	case InodeMarshalVersion3:
		if err = di.Inode.UnmarshalValueV3(ctx, val[:len(val) - DeletedInodeTimeStampLen - DeletedInodeExpiredFlagLen]); err != nil {
			return
		}
	default:
		panic(fmt.Sprintf("error version:%v", inodeV))
	}
	offset := len(val) - DeletedInodeTimeStampLen - DeletedInodeExpiredFlagLen
	di.Timestamp = int64(binary.BigEndian.Uint64(val[offset:offset+8]))
	offset += 8
	di.IsExpired = val[offset] != 0
	return
}

/*
type DeletedINodeBatch []*DeletedINode

func (db DeletedINodeBatch) Marshal(ctx context.Context) ([]byte, error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("DeletedINodeBatch.Marshal").SetTag("len", len(db))
	defer tracer.Finish()
	ctx = tracer.Context()

	buff := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buff, binary.BigEndian, uint32(len(db))); err != nil {
		return nil, err
	}
	for _, inode := range db {
		bs, err := inode.Marshal()
		if err != nil {
			return nil, err
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
			return nil, err
		}
		if _, err := buff.Write(bs); err != nil {
			return nil, err
		}
	}
	return buff.Bytes(), nil
}

func DeletedINodeBatchUnmarshal(ctx context.Context, raw []byte) (DeletedINodeBatch, error) {
	var tracer = tracing.TracerFromContext(ctx).ChildTracer("DeletedINodeBatchUnmarshal")
	defer tracer.Finish()
	ctx = tracer.Context()

	buff := bytes.NewBuffer(raw)
	var batchLen uint32
	if err := binary.Read(buff, binary.BigEndian, &batchLen); err != nil {
		return nil, err
	}

	result := make(DeletedINodeBatch, 0, int(batchLen))

	var dataLen uint32
	for j := 0; j < int(batchLen); j++ {
		if err := binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return nil, err
		}
		data := make([]byte, int(dataLen))
		if _, err := buff.Read(data); err != nil {
			return nil, err
		}
		ino := NewDeletedInodeByID(0)
		if err := ino.Unmarshal(ctx, data); err != nil {
			return nil, err
		}
		result = append(result, ino)
	}

	return result, nil
}

*/
