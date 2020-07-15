package proto

import (
	"bytes"
	"sync"
)

var (
	CommonSize=512
	bytesBufferPool=&sync.Pool{New: func() interface{} {
		return bytes.NewBuffer(make([]byte,CommonSize))
	}}
)

func GetBytesBufferFromPool()(b *bytes.Buffer) {
	b=bytesBufferPool.Get().(*bytes.Buffer)
	b.Reset()
	return
}


func PutBytesBufferToPool(b *bytes.Buffer) {
	b.Reset()
	bytesBufferPool.Put(b)
}