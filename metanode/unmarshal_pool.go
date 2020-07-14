package metanode

import "sync"

const (
	MaxCommonUnmarshalSize = 1024
	storeUnmarshalLength   = 4
)

var (
	commonUnmarshalPool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, MaxCommonUnmarshalSize)
		},
	}
	storeUnmarshalLengthPool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, storeUnmarshalLength)
		},
	}
)

func GetCommonUnmarshalData(inoUnmarshalSize int) (data []byte) {
	if inoUnmarshalSize <= MaxCommonUnmarshalSize {
		data = commonUnmarshalPool.Get().([]byte)
	} else {
		data = make([]byte, inoUnmarshalSize)
	}
	return
}

func PutCommonUnmarshalData(data []byte) {
	if len(data) == MaxCommonUnmarshalSize {
		commonUnmarshalPool.Put(data)
	}
	return
}

func GetStoreUnmarshalLengthData() (data []byte) {
	return storeUnmarshalLengthPool.Get().([]byte)
}

func PutStoreUnmarshalLengthData(data []byte) {
	if len(data) == storeUnmarshalLength {
		storeUnmarshalLengthPool.Put(data)
	}
	return
}
