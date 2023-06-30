package metanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

const (
	BaseDeletedDentryLen       = 52
	BaseDeletedDentryKeyLen    = 20
	BaseDeletedDentryValueLen  = 24
	DentryNameAppendTimeFormat = "20060102150405.000000"
)

type DeletedDentry struct {
	Dentry
	Timestamp int64  `json:"ts"`
	From      string `json:"from"`
}

func newPrimaryDeletedDentry(parentID uint64, name string, timestamp int64, ino uint64) *DeletedDentry {
	ddentry := new(DeletedDentry)
	ddentry.ParentId = parentID
	ddentry.Name = name
	ddentry.Timestamp = timestamp
	ddentry.Inode = ino
	return ddentry
}

func newDeletedDentry(de *Dentry, ts int64, from string) *DeletedDentry {
	dd := new(DeletedDentry)
	dd.ParentId = de.ParentId
	dd.Name = de.Name
	dd.Inode = de.Inode
	dd.Type = de.Type
	dd.Timestamp = ts
	dd.From = from
	return dd
}

func (dd *DeletedDentry) buildDentry() *Dentry {
	d := new(Dentry)
	d.ParentId = dd.ParentId
	d.Name = dd.Name
	d.Inode = dd.Inode
	d.Type = dd.Type
	return d
}

func (dd *DeletedDentry) appendTimestampToName() {
	timeStr := time.Unix(dd.Timestamp/1000000, dd.Timestamp%1000000*1000).Format(DentryNameAppendTimeFormat)
	dd.Name = fmt.Sprintf("%v_%v", dd.Name, timeStr)
}

func (dd *DeletedDentry) Less(than BtreeItem) (less bool) {
	ddentry, ok := than.(*DeletedDentry)
	if !ok {
		log.LogDebugf("DeletedDentry type is error")
		return false
	}

	if dd.ParentId < ddentry.ParentId {
		return true
	}

	if dd.ParentId == ddentry.ParentId {
		if dd.Name < ddentry.Name {
			return true
		}

		if dd.Name == ddentry.Name && dd.Timestamp < ddentry.Timestamp {
			return true
		}
	}

	return false
}

func (dd *DeletedDentry) Copy() BtreeItem {
	newDentry := *dd
	return &newDentry
}

func (dd *DeletedDentry) MarshalToJSON() (data []byte, err error) {
	data, err = json.Marshal(dd)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	return
}

func (dd *DeletedDentry) UnmarshalFromJSON(data []byte) (err error) {
	err = json.Unmarshal(data, dd)
	if err != nil {
		log.LogError(err.Error())
	}
	return
}

func (dd *DeletedDentry) String() string {
	return fmt.Sprintf("ParentID: %v, INodeID: %v, Name: %v, Type: %v, DeleteTime: %v, From: %v",
		dd.ParentId, dd.Inode, dd.Name, dd.Type, dd.Timestamp, dd.From)
}

func (dd *DeletedDentry) Unmarshal(raw []byte) (err error) {
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		log.LogError(err.Error())
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		log.LogError(err.Error())
		return
	}
	if err = dd.UnmarshalKey(keyBytes); err != nil {
		log.LogError(err.Error())
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		log.LogError(err.Error())
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		log.LogError(err.Error())
		return
	}
	err = dd.UnmarshalValue(valBytes)
	if err != nil {
		log.LogError(err.Error())
	}
	return
}

func (dd *DeletedDentry) Marshal() (result []byte, err error) {
	keyBytes := dd.MarshalKey()
	valBytes := dd.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(96)
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

func (dd *DeletedDentry) MarshalKey() (k []byte) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(48)
	err := binary.Write(buff, binary.BigEndian, &dd.ParentId)
	if err != nil {
		panic(err)
	}
	err = binary.Write(buff, binary.BigEndian, &dd.Timestamp)
	if err != nil {
		panic(err)
	}
	nameLen := uint32(len(dd.Name))
	err = binary.Write(buff, binary.BigEndian, nameLen)
	if err != nil {
		panic(err)
	}
	if nameLen > 0 {
		_, err = buff.Write([]byte(dd.Name))
		if err != nil {
			panic(err)
		}
	}
	k = buff.Bytes()
	return
}

func (dd *DeletedDentry) UnmarshalKey(k []byte) (err error) {
	buff := bytes.NewBuffer(k)
	err = binary.Read(buff, binary.BigEndian, &dd.ParentId)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = binary.Read(buff, binary.BigEndian, &dd.Timestamp)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	var nameLen uint32
	err = binary.Read(buff, binary.BigEndian, &nameLen)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	if nameLen > 0 {
		nameBytes := make([]byte, nameLen)
		_, err = buff.Read(nameBytes)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		dd.Name = string(nameBytes)
	}
	return
}

// MarshalValue marshals the exporterKey to bytes.
func (dd *DeletedDentry) MarshalValue() (k []byte) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(39)
	if err := binary.Write(buff, binary.BigEndian, &dd.Inode); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, &dd.Type); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, &dd.Timestamp); err != nil {
		panic(err)
	}
	fromLen := uint32(len(dd.From))
	if err := binary.Write(buff, binary.BigEndian, fromLen); err != nil {
		panic(err)
	}
	if fromLen > 0 {
		if _, err := buff.Write([]byte(dd.From)); err != nil {
			panic(err)
		}
	}

	k = buff.Bytes()
	return
}

// UnmarshalValue unmarshals the value from bytes.
func (dd *DeletedDentry) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)
	err = binary.Read(buff, binary.BigEndian, &dd.Inode)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	err = binary.Read(buff, binary.BigEndian, &dd.Type)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	err = binary.Read(buff, binary.BigEndian, &dd.Timestamp)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	var fromLen uint32
	err = binary.Read(buff, binary.BigEndian, &fromLen)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	if fromLen > 0 {
		fromBytes := make([]byte, fromLen)
		_, err = buff.Read(fromBytes)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		dd.From = string(fromBytes)
	}

	return
}

func (dd *DeletedDentry) DeletedDentryKeyLen() int {
	return BaseDeletedDentryKeyLen + len(dd.Name)
}

func (dd *DeletedDentry) DeletedDentryValueLen() int {
	return BaseDeletedDentryValueLen + len(dd.From)
}

func (dd *DeletedDentry) BinaryDataLen() int {
	return BaseDeletedDentryLen + len(dd.Name) + len(dd.From)
}

func (dd *DeletedDentry) EncodeBinary(data []byte) (dataLen int, err error) {
	dataLen = dd.BinaryDataLen()
	if len(data) < dataLen {
		err = fmt.Errorf("data len %v less than deleted dentry len %v", len(data), dataLen)
		return
	}

	offset := 0
	binary.BigEndian.PutUint32(data[offset:offset+Uint32Size], uint32(dd.DeletedDentryKeyLen()))
	offset += Uint32Size
	binary.BigEndian.PutUint64(data[offset:offset+Uint64Size], dd.ParentId)
	offset += Uint64Size
	binary.BigEndian.PutUint64(data[offset:offset+Uint64Size], uint64(dd.Timestamp))
	offset += Uint64Size
	binary.BigEndian.PutUint32(data[offset:offset+Uint32Size], uint32(len(dd.Name)))
	offset += Uint32Size
	copy(data[offset:offset+len(dd.Name)], dd.Name)
	offset += len(dd.Name)
	binary.BigEndian.PutUint32(data[offset:offset+Uint32Size], uint32(dd.DeletedDentryValueLen()))
	offset += Uint32Size
	binary.BigEndian.PutUint64(data[offset:offset+Uint64Size], dd.Inode)
	offset += Uint64Size
	binary.BigEndian.PutUint32(data[offset:offset+Uint32Size], dd.Type)
	offset += Uint32Size
	binary.BigEndian.PutUint64(data[offset:offset+Uint64Size], uint64(dd.Timestamp))
	offset += Uint64Size
	binary.BigEndian.PutUint32(data[offset:offset+Uint32Size], uint32(len(dd.From)))
	offset += Uint32Size
	if len(dd.From) > 0 {
		copy(data[offset:offset+len(dd.From)], dd.From)
	}
	return
}

type DeletedDentryBatch []*DeletedDentry

func (db DeletedDentryBatch) Marshal() ([]byte, error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buff, binary.BigEndian, uint32(len(db))); err != nil {
		return nil, err
	}
	for _, dentry := range db {
		bs, err := dentry.Marshal()
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

func DeletedDentryBatchUnmarshal(raw []byte) (DeletedDentryBatch, error) {
	buff := bytes.NewBuffer(raw)
	var batchLen uint32
	if err := binary.Read(buff, binary.BigEndian, &batchLen); err != nil {
		return nil, err
	}

	result := make(DeletedDentryBatch, 0, int(batchLen))

	var dataLen uint32
	for j := 0; j < int(batchLen); j++ {
		if err := binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return nil, err
		}
		data := make([]byte, int(dataLen))
		if _, err := buff.Read(data); err != nil {
			return nil, err
		}
		den := new(DeletedDentry)
		if err := den.Unmarshal(data); err != nil {
			return nil, err
		}
		result = append(result, den)
	}

	return result, nil
}

func appendTimestampToName(name string, ts int64) string {
	timeStr := time.Unix(ts/1000000, ts%1000000*1000).Format(DentryNameAppendTimeFormat)
	return fmt.Sprintf("%v_%v", name, timeStr)
}