package metanode

import (
	"bytes"
	"encoding/binary"
)

// MarshalKey is the bytes version of the MarshalKey method which returns the byte slice result.
func (d *Dentry) MarshalKeyWithBuff(data []byte) (keyLen uint32) {
	buff := bytes.NewBuffer(data)
	if err := binary.Write(buff, binary.BigEndian, &d.ParentId); err != nil {
		panic(err)
	}
	buff.Write([]byte(d.Name))
	keyLen = uint32(buff.Len())
	return
}

// MarshalValue marshals the exporterKey to bytes.
func (d *Dentry) MarshalValueWithBuff(data []byte) (vLen uint32) {
	buff := bytes.NewBuffer(data)
	if err := binary.Write(buff, binary.BigEndian, &d.Inode); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, &d.Type); err != nil {
		panic(err)
	}
	vLen = uint32(buff.Len())
	return
}

// Marshal marshals a dentry into a byte array.
func (d *Dentry) MarshalWithBuff(orgData []byte) (result []byte, err error) {
	result = orgData
	//write keyInfo to result[4:]
	keyLen := d.MarshalKeyWithBuff(result[4:])
	//write KeyInfoLen to result[0:4]
	binary.BigEndian.PutUint32(result[0:4], keyLen)

	//keyEndOffset:=keyLen+4
	keyEnd := keyLen + 4

	///valStart skip 4 byte start Write
	valStart := keyEnd + 4

	//write valueInfo to result
	valLen := d.MarshalValueWithBuff(result[valStart:])

	//write valLen to keyEnd:valStart
	binary.BigEndian.PutUint32(result[keyEnd:valStart], valLen)
	result = result[0 : valStart+valLen]
	return
}
