package proto

import (
	"sync"
	"testing"
)

var (
	epool=&sync.Pool{New: func() interface{} {
		return new(Entry)
	}}
)

func TestCodec(t *testing.T) {

	entrys:=make([]*Entry,0)
	for i:=0;i<10;i++{
		e:=epool.Get().(*Entry)
		e.Index=uint64(i)
		entrys=append(entrys,e)
		epool.Put(e)
	}
	for i:=0;i<10;i++{
		e:=epool.Get().(*Entry)
		e.Index=2*uint64(i)
		epool.Put(e)
	}
	for i:=0;i,

}