package metanode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

const (
	EncodeTypeExtent = 1
	EncodeTypeObjExt = 2

	MultiSnapVerSeqLen             = 8
	HybridCloudExtentsTypeLen      = 1
	HybridCloudExtentsMigrationLen = 13
)

func (i *Inode) MarshalRocksdb() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	buff.Grow(64)

	valBytes, err := i.MarshalRocksdbValue()
	if err != nil {
		log.LogErrorf("inode MarshalRocksdb failed, err: %v", err)
		return
	}
	if err = binary.Write(buff, binary.BigEndian, uint32(len(valBytes))); err != nil {
		panic(err)
	}
	if _, err = buff.Write(valBytes); err != nil {
		return
	}

	if i.multiSnap != nil {
		multiSnapLen := uint32(len(i.multiSnap.multiVersions)) + MultiSnapVerSeqLen
		if err = binary.Write(buff, binary.BigEndian, multiSnapLen); err != nil {
			panic(err)
		}
		if err = binary.Write(buff, binary.BigEndian, i.multiSnap.verSeq); err != nil {
			panic(err)
		}
		for _, ino := range i.multiSnap.multiVersions {
			valBytes, err = ino.MarshalRocksdb()
			if err != nil {
				log.LogErrorf("inode MarshalRocksdb failed, err: %v", err)
				return
			}
			if err = binary.Write(buff, binary.BigEndian, uint32(len(valBytes))); err != nil {
				panic(err)
			}
			if _, err = buff.Write(valBytes); err != nil {
				return
			}
		}
	} else {
		if err = binary.Write(buff, binary.BigEndian, uint32(0)); err != nil {
			panic(err)
		}
	}

	result = buff.Bytes()

	return
}

func (i *Inode) MarshalRocksdbValue() (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0, 128))
	buff.Grow(64)

	if err = binary.Write(buff, binary.BigEndian, &i.Inode); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Type); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Uid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Gid); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Size); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Generation); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.CreateTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.AccessTime); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		panic(err)
	}
	// write SymLink
	symSize := uint32(len(i.LinkTarget))
	if err = binary.Write(buff, binary.BigEndian, &symSize); err != nil {
		panic(err)
	}
	if symSize > 0 {
		if _, err = buff.Write(i.LinkTarget); err != nil {
			panic(err)
		}
	}

	if err = binary.Write(buff, binary.BigEndian, &i.NLink); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Flag); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.Reserved); err != nil {
		panic(err)
	}
	// marshal StorageClass
	if err = binary.Write(buff, binary.BigEndian, &i.StorageClass); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.ClientID); err != nil {
		panic(err)
	}
	if err = binary.Write(buff, binary.BigEndian, &i.LeaseExpireTime); err != nil {
		panic(err)
	}
	if i.HybridCloudExtents != nil && i.HybridCloudExtents.sortedEks != nil {
		sortExtents, ok := i.HybridCloudExtents.sortedEks.(*SortedExtents)
		if ok {
			tmpBuf1 := GetInodeBuf()
			defer PutInodeBuf(tmpBuf1)

			err := sortExtents.MarshalBinary(tmpBuf1, true)
			if err != nil {
				panic(err)
			}
			extData := tmpBuf1.Bytes()
			extSize := uint32(len(extData)) + HybridCloudExtentsTypeLen
			if err = binary.Write(buff, binary.BigEndian, extSize); err != nil {
				panic(err)
			}
			if err = binary.Write(buff, binary.BigEndian, uint8(EncodeTypeExtent)); err != nil {
				panic(err)
			}
			if _, err = buff.Write(extData); err != nil {
				panic(err)
			}
		} else {
			ObjExtents, ok2 := i.HybridCloudExtents.sortedEks.(*SortedObjExtents)
			if !ok2 {
				err = fmt.Errorf("storage class(%d) HybridCloudExtents.sortedEks transfer failed", i.StorageClass)
				panic(err)
			}
			objExtData, err := ObjExtents.MarshalBinary()
			if err != nil {
				panic(err)
			}
			extSize := uint32(len(objExtData)) + HybridCloudExtentsTypeLen
			if err = binary.Write(buff, binary.BigEndian, extSize); err != nil {
				panic(err)
			}
			if err = binary.Write(buff, binary.BigEndian, uint8(EncodeTypeObjExt)); err != nil {
				panic(err)
			}
			if _, err = buff.Write(objExtData); err != nil {
				panic(err)
			}
		}
	} else {
		if err = binary.Write(buff, binary.BigEndian, uint32(0)); err != nil {
			panic(err)
		}
	}
	if i.HybridCloudExtentsMigration != nil && i.HybridCloudExtentsMigration.sortedEks != nil {
		sem := i.HybridCloudExtentsMigration
		replicaExtents, ok := sem.sortedEks.(*SortedExtents)
		if ok {
			tmpBuf := GetInodeBuf()
			defer PutInodeBuf(tmpBuf)

			err := replicaExtents.MarshalBinary(tmpBuf, true)
			if err != nil {
				panic(err)
			}
			extData := tmpBuf.Bytes()

			extSize := uint32(len(extData)) + HybridCloudExtentsMigrationLen
			if err = binary.Write(buff, binary.BigEndian, extSize); err != nil {
				panic(err)
			}
			if err = binary.Write(buff, binary.BigEndian, uint8(EncodeTypeExtent)); err != nil {
				panic(err)
			}
			if err = binary.Write(buff, binary.BigEndian, &sem.storageClass); err != nil {
				panic(err)
			}
			if err = binary.Write(buff, binary.BigEndian, &sem.expiredTime); err != nil {
				panic(err)
			}
			if _, err = buff.Write(extData); err != nil {
				panic(err)
			}
		} else {
			ObjExtents, ok2 := sem.sortedEks.(*SortedObjExtents)
			if !ok2 {
				err = fmt.Errorf("storage class(%d) HybridCloudExtentsMigration.sortedEks transfer failed", i.StorageClass)
				panic(err)
			}
			objExtData, err := ObjExtents.MarshalBinary()
			if err != nil {
				panic(err)
			}
			extSize := uint32(len(objExtData)) + HybridCloudExtentsMigrationLen
			if err = binary.Write(buff, binary.BigEndian, extSize); err != nil {
				panic(err)
			}
			if err = binary.Write(buff, binary.BigEndian, uint8(EncodeTypeObjExt)); err != nil {
				panic(err)
			}
			if err = binary.Write(buff, binary.BigEndian, &sem.storageClass); err != nil {
				panic(err)
			}
			if err = binary.Write(buff, binary.BigEndian, &sem.expiredTime); err != nil {
				panic(err)
			}
			if _, err = buff.Write(objExtData); err != nil {
				panic(err)
			}
		}
	} else {
		if err = binary.Write(buff, binary.BigEndian, uint32(0)); err != nil {
			panic(err)
		}
	}

	result = buff.Bytes()

	return
}

func (i *Inode) UnmarshalRocksdb(raw []byte) (err error) {
	buff := bytes.NewBuffer(raw)

	return i.UnmarshalRocksdbBody(buff)
}

func (i *Inode) UnmarshalRocksdbBody(buff *bytes.Buffer) (err error) {
	var (
		multiSnapLen uint32
		buffSize     uint32
	)

	if err = binary.Read(buff, binary.BigEndian, &buffSize); err != nil {
		err = UnmarshalInodeFiledError("buffSize", err)
		return
	}

	err = i.UnmarshalRocksdbValue(buff)
	if err != nil {
		err = errors.NewErrorf("[Unmarshal] inode(%v) UnmarshalValue: %s", i.Inode, err.Error())
		return
	}

	if err = binary.Read(buff, binary.BigEndian, &multiSnapLen); err != nil {
		err = fmt.Errorf("Error to get multiSnap length: %s", err.Error())
		return
	}

	if multiSnapLen > 0 {
		if i.multiSnap == nil {
			i.multiSnap = NewMultiSnap(0)
		}
		if i.multiSnap.ekRefMap == nil {
			i.multiSnap.ekRefMap = new(sync.Map)
		}

		if err = binary.Read(buff, binary.BigEndian, &i.multiSnap.verSeq); err != nil {
			err = fmt.Errorf("Error to get multiSnap verSeq: %s", err.Error())
			return
		}
		multiSnapLen -= MultiSnapVerSeqLen

		for cnt := uint32(0); cnt < multiSnapLen; cnt++ {
			ino := NewInode(0, 0)
			if err = binary.Read(buff, binary.BigEndian, &buffSize); err != nil {
				err = UnmarshalInodeFiledError("buffSize", err)
				return
			}
			err = ino.UnmarshalRocksdbBody(buff)
			if err != nil {
				log.LogErrorf("UnmarshalRocksdbBody err: %s", err.Error())
				return err
			}
			i.multiSnap.multiVersions = append(i.multiSnap.multiVersions, ino)
		}
	}

	return
}

func (i *Inode) UnmarshalRocksdbValue(buff *bytes.Buffer) (err error) {
	if err = binary.Read(buff, binary.BigEndian, &i.Inode); err != nil {
		err = UnmarshalInodeFiledError("Inode", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Type); err != nil {
		err = UnmarshalInodeFiledError("Type", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Uid); err != nil {
		err = UnmarshalInodeFiledError("Uid", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Gid); err != nil {
		err = UnmarshalInodeFiledError("Gid", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Size); err != nil {
		err = UnmarshalInodeFiledError("Size", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Generation); err != nil {
		err = UnmarshalInodeFiledError("Generation", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.CreateTime); err != nil {
		err = UnmarshalInodeFiledError("CreateTime", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.AccessTime); err != nil {
		err = UnmarshalInodeFiledError("AccessTime", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		err = UnmarshalInodeFiledError("ModifyTime", err)
		return
	}
	// read symLink
	symSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &symSize); err != nil {
		err = UnmarshalInodeFiledError("symSize", err)
		return
	}
	if symSize > 0 {
		if symSize > proto.MaxBufferSize {
			return proto.ErrBufferSizeExceedMaximum
		}
		i.LinkTarget = make([]byte, symSize)
		if _, err = io.ReadFull(buff, i.LinkTarget); err != nil {
			err = UnmarshalInodeFiledError("LinkTarget", err)
			return
		}
	}

	if err = binary.Read(buff, binary.BigEndian, &i.NLink); err != nil {
		err = UnmarshalInodeFiledError("NLink", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Flag); err != nil {
		err = UnmarshalInodeFiledError("Flag", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Reserved); err != nil {
		err = UnmarshalInodeFiledError("Reserved", err)
		return
	}

	if err = binary.Read(buff, binary.BigEndian, &i.StorageClass); err != nil {
		err = UnmarshalInodeFiledError("StorageClass", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.ClientID); err != nil {
		err = UnmarshalInodeFiledError("ClientID", err)
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.LeaseExpireTime); err != nil {
		err = UnmarshalInodeFiledError("LeaseExpireTime", err)
		return
	}

	sortExtentSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &sortExtentSize); err != nil {
		err = UnmarshalInodeFiledError("HybridCloudExtents.ObjExtSize", err)
		return
	}
	if sortExtentSize > 0 {
		extentType := uint8(0)
		if err = binary.Read(buff, binary.BigEndian, &extentType); err != nil {
			err = UnmarshalInodeFiledError("HybridCloudExtents.extentType", err)
			return
		}
		sortExtentSize -= HybridCloudExtentsTypeLen
		if i.HybridCloudExtents == nil {
			i.HybridCloudExtents = NewSortedHybridCloudExtents()
		}
		extBytes := make([]byte, sortExtentSize)
		if _, err = io.ReadFull(buff, extBytes); err != nil {
			err = UnmarshalInodeFiledError("HybridCloudExtents.extBytes(v4)", err)
			return
		}
		switch extentType {
		case EncodeTypeExtent:
			var ekRef *sync.Map
			eks := NewSortedExtents()
			if err, ekRef = eks.UnmarshalBinary(extBytes, true); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtents.SortedExtents(v4)", err)
				return
			}
			i.HybridCloudExtents.sortedEks = eks
			if ekRef != nil {
				if i.multiSnap == nil {
					i.multiSnap = NewMultiSnap(0)
				}
				i.multiSnap.ekRefMap = ekRef
			}
		case EncodeTypeObjExt:
			ObjExtents := NewSortedObjExtents()
			if err = ObjExtents.UnmarshalBinary(extBytes); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtents.ObjExtents(v4)", err)
				return
			}
			i.HybridCloudExtents.sortedEks = ObjExtents
		default:
			err = fmt.Errorf("HybridCloudExtents.ObjExtSize unknown type(%d)", extentType)
			return
		}
	}

	sortExtentSize = uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &sortExtentSize); err != nil {
		err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.ObjExtSize", err)
		return
	}
	if sortExtentSize > 0 {
		extentType := uint8(0)
		if err = binary.Read(buff, binary.BigEndian, &extentType); err != nil {
			err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.extentType", err)
			return
		}
		if i.HybridCloudExtentsMigration == nil {
			i.HybridCloudExtentsMigration = NewSortedHybridCloudExtentsMigration()
		}
		if err = binary.Read(buff, binary.BigEndian, &i.HybridCloudExtentsMigration.storageClass); err != nil {
			err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.storageClass", err)
			return
		}
		if err = binary.Read(buff, binary.BigEndian, &i.HybridCloudExtentsMigration.expiredTime); err != nil {
			err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.expiredTime", err)
			return
		}
		sortExtentSize -= HybridCloudExtentsMigrationLen
		extBytes := make([]byte, sortExtentSize)
		if _, err = io.ReadFull(buff, extBytes); err != nil {
			err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.objExtBytes(v4)", err)
			return
		}
		switch extentType {
		case EncodeTypeExtent:
			sortExtents := NewSortedExtents()
			if err, _ = sortExtents.UnmarshalBinary(extBytes, true); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.ObjExtents(v4)", err)
				return
			}
			i.HybridCloudExtentsMigration.sortedEks = sortExtents
		case EncodeTypeObjExt:
			ObjExtents := NewSortedObjExtents()
			if err = ObjExtents.UnmarshalBinary(extBytes); err != nil {
				err = UnmarshalInodeFiledError("HybridCloudExtentsMigration.ObjExtents(v4)", err)
				return
			}
			i.HybridCloudExtentsMigration.sortedEks = ObjExtents
		default:
			err = fmt.Errorf("HybridCloudExtentsMigration.ObjExtSize unknown type(%d)", extentType)
			return
		}
	}

	return
}
