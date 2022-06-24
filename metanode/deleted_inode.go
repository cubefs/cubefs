package metanode

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/chubaofs/chubaofs/util/log"
	se "github.com/chubaofs/chubaofs/util/sortedextent"
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
	return newIno
}

func (di *DeletedINode) buildInode() (ino *Inode) {
	ino = new(Inode)
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
	buff.WriteString("}")
	return buff.String()
}

func (di *DeletedINode) Marshal() (result []byte, err error) {
	keyBytes := di.MarshalKey()
	valBytes := di.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0, 128))
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
	k = make([]byte, 8)
	binary.BigEndian.PutUint64(k, di.Inode.Inode)
	return
}

func (di *DeletedINode) MarshalValue() (val []byte) {
	var err error
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	buff.Grow(64)
	di.RLock()
	di.RUnlock()
	if err = binary.Write(buff, binary.BigEndian, &di.Type); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &di.Uid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &di.Gid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &di.Size); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &di.Generation); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &di.CreateTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &di.AccessTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &di.ModifyTime); err != nil {
		panic(err)
	}
	symSize := uint32(len(di.LinkTarget))
	if err = binary.Write(buff, binary.BigEndian, &symSize); err != nil {
		panic(err)
	}
	if symSize > 0 {
		if _, err = buff.Write(di.LinkTarget); err != nil {
			panic(err)
		}
	}

	if err = binary.Write(buff, binary.BigEndian, &di.NLink); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &di.Flag); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &di.Reserved); err != nil {
		panic(err)
	}

	var extDataLen uint32
	if di.Extents != nil {
		extData, err := di.Extents.MarshalBinary()
		if err != nil {
			panic(err)
		}
		extDataLen = uint32(len(extData))
		if err = binary.Write(buff, binary.BigEndian, extDataLen); err != nil {
			panic(err)
		}
		if extDataLen > 0 {
			if _, err = buff.Write(extData); err != nil {
				panic(err)
			}
		}
	} else {
		extDataLen = 0
		if err = binary.Write(buff, binary.BigEndian, extDataLen); err != nil {
			panic(err)
		}
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
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		return
	}
	if err = di.UnmarshalKey(keyBytes); err != nil {
		return
	}

	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	err = di.UnmarshalValue(ctx, valBytes)
	return
}

func (di *DeletedINode) UnmarshalKey(k []byte) (err error) {
	di.Inode.Inode = binary.BigEndian.Uint64(k)
	return
}

func (di *DeletedINode) UnmarshalValue(ctx context.Context, val []byte) (err error) {
	buff := bytes.NewBuffer(val)
	if err = binary.Read(buff, binary.BigEndian, &di.Type); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &di.Uid); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &di.Gid); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &di.Size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &di.Generation); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &di.CreateTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &di.AccessTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &di.ModifyTime); err != nil {
		return
	}

	symSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &symSize); err != nil {
		return
	}
	if symSize > 0 {
		di.LinkTarget = make([]byte, symSize)
		if _, err = io.ReadFull(buff, di.LinkTarget); err != nil {
			return
		}
	}

	if err = binary.Read(buff, binary.BigEndian, &di.NLink); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &di.Flag); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &di.Reserved); err != nil {
		return
	}

	var extentBytesLen uint32
	if err = binary.Read(buff, binary.BigEndian, &extentBytesLen); err != nil {
		return
	}
	di.Extents = se.NewSortedExtents()
	if extentBytesLen > 0 {
		extentBytes := make([]byte, extentBytesLen)
		_, err = buff.Read(extentBytes)
		if err != nil {
			return
		}
		if err = di.Extents.UnmarshalBinary(ctx, extentBytes); err != nil {
			return
		}
	}

	if err = binary.Read(buff, binary.BigEndian, &di.Timestamp); err != nil {
		return
	}

	if err = binary.Read(buff, binary.BigEndian, &di.IsExpired); err != nil {
		return
	}

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
