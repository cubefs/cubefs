package proto

import (
	"bytes"
	"github.com/cubefs/cubefs/util/btree"
)

type HybridCloudObjectExtentKey interface {
	String() string
	Less(than btree.Item) bool
	Copy() btree.Item
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(buf *bytes.Buffer) (err error)
}
