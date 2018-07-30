package master

import (
	"fmt"
	"github.com/chubaoio/cbfs/util/gorocksdb"
	"io"
)

type MetadataSnapshot struct {
	fsm      *MetadataFsm
	applied  uint64
	snapshot *gorocksdb.Snapshot
	iterator *gorocksdb.Iterator
}

func (ms *MetadataSnapshot) ApplyIndex() uint64 {
	return ms.applied
}

func (ms *MetadataSnapshot) Close() {
	ms.fsm.store.ReleaseSnapshot(ms.snapshot)
}

func (ms *MetadataSnapshot) Next() (data []byte, err error) {
	md := new(Metadata)
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
		key.Free()
		value.Free()
		ms.iterator.Next()
		return data, nil
	}
	return nil, io.EOF
}
