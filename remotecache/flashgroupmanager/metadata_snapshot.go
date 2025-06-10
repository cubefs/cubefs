package flashgroupmanager

import (
	"fmt"
	"io"

	"github.com/tecbot/gorocksdb"
)

// MetadataSnapshot represents the snapshot of a meta partition
type MetadataSnapshot struct {
	fsm      *MetadataFsm
	applied  uint64
	snapshot *gorocksdb.Snapshot
	iterator *gorocksdb.Iterator
}

// ApplyIndex implements the Snapshot interface
func (ms *MetadataSnapshot) ApplyIndex() uint64 {
	return ms.applied
}

// Close implements the Snapshot interface
func (ms *MetadataSnapshot) Close() {
	ms.fsm.store.ReleaseSnapshot(ms.snapshot)
}

// Next implements the Snapshot interface
func (ms *MetadataSnapshot) Next() (data []byte, err error) {
	md := new(RaftCmd)
	if ms.iterator.Valid() {
		key := ms.iterator.Key()
		md.K = string(key.Data())
		md.setOpType()
		value := ms.iterator.Value()
		if value != nil {
			md.V = value.Data()
		}
		if data, err = md.Marshal(); err != nil {
			err = fmt.Errorf("action[Next],marshal kv:%v,err:%v", md, err.Error())
			return nil, err
		}
		ms.iterator.Next()
		return data, nil
	}
	return nil, io.EOF
}
