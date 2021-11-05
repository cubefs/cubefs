package blackmagic

import (
	"reflect"
	"unsafe"
)

func StringToBytes(s string) (b []byte) {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

func BytesToString(b []byte) (s string) {
	return *(*string)(unsafe.Pointer(&b))
}
