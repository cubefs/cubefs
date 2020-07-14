package metanode

import (
	"bytes"
	"encoding/binary"
)

const (
	InodeNumSize = 8
)

// Marshal marshals the inode into a byte array.
func (i *Inode) MarshalWithBuffer(orgData []byte) (result []byte, err error) {
	result = orgData
	keyLen, err := i.MarshalKeyWithBuff(result)
	if err != nil {
		return
	}
	valueStart := keyLen + 4
	valueLen := i.MarshalValueWithBuffer(result[valueStart:])
	binary.BigEndian.PutUint32(result[keyLen:valueStart], valueLen)
	valueEnd := valueLen + uint32(valueStart)
	result = result[0:valueEnd]
	return
}

func (i *Inode) MarshalKeyWithBuff(data []byte) (keyLen uint32, err error) {
	// inode is uint64,first write Key
	buff := bytes.NewBuffer(data)
	if err = binary.Write(buff, binary.BigEndian, uint32(InodeNumSize)); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, i.Inode); err != nil {
		return
	}
	keyLen = uint32(buff.Len())
	return
}

// MarshalValue marshals the value to bytes.
func (i *Inode) MarshalValueWithBuffer(data []byte) (vLen uint32) {
	var err error
	buff := bytes.NewBuffer(data)
	i.RLock()
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
	if _, err = buff.Write(i.LinkTarget); err != nil {
		panic(err)
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
	// marshal ExtentsKey
	err = i.Extents.MarshalBinaryWithBuffer(buff)
	if err != nil {
		panic(err)
	}

	i.RUnlock()
	vLen = uint32(buff.Len())
	return
}

func (se *SortedExtents) MarshalBinaryWithBuffer(buff *bytes.Buffer) (err error) {
	se.RLock()
	defer se.RUnlock()

	for _, ek := range se.eks {
		err := ek.MarshalBinaryWithBuffer(buff)
		if err != nil {
			return err
		}
	}
	return nil
}
