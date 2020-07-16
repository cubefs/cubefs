package proto

import (
	"github.com/chubaofs/chubaofs/util/bytes"
	"sync"
)

var (
	CommonSize            =512
	commonBytesBufferPool =&sync.Pool{New: func() interface{} {
		return bytes.NewBuffer(make([]byte,CommonSize))
	}}
	emptyBytesBufferPool=&sync.Pool{New: func() interface{} {
		return bytes.NewBufferWithNill()
	}}
)

func GetCommonBytesBufferFromPool()(b *bytes.Buffer) {
	b= commonBytesBufferPool.Get().(*bytes.Buffer)
	b.Reset()
	return
}


func PutCommonBytesBufferToPool(b *bytes.Buffer) {
	b.Reset()
	commonBytesBufferPool.Put(b)
}

func GetEmptyBytesBufferFromPool()(b *bytes.Buffer) {
	b= emptyBytesBufferPool.Get().(*bytes.Buffer)
	b.Clean()
	return
}


func PutEmptyBytesBufferToPool(b *bytes.Buffer) {
	b.Clean()
	emptyBytesBufferPool.Put(b)
}


