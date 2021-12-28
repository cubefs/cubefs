package blobstore

import (
	"github.com/chubaofs/chubaofs/proto"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	objEks := make([]proto.ObjExtentKey, 0)
	eks := make([]proto.ExtentKey, 0)
	objEkLen := rand.Intn(20)
	expectedFileSize := 0
	for i := 0; i < objEkLen; i++ {
		size := rand.Intn(1000)
		objEks = append(objEks, proto.ObjExtentKey{Size: uint64(size), FileOffset: uint64(expectedFileSize)})
		eks = append(eks, proto.ExtentKey{FileOffset: uint64(expectedFileSize), Size: uint32(size)})
		expectedFileSize += size
	}

	rSlices := make([]rwSlice, 0)
	for i := 0; i < objEkLen; i++ {
		rSlices = append(rSlices, rwSlice{
			index:        0,
			fileOffset:   0,
			size:         uint32(expectedFileSize),
			rOffset:      0,
			rSize:        0,
			read:         0,
			Data:         nil,
			extentKey:    eks[i],
			objExtentKey: objEks[i],
		})
	}

	sliceSize := len(rSlices)

	assert.Equal(t, int(sliceSize), int(objEkLen))

	var wg sync.WaitGroup
	pool := New(3, sliceSize)
	wg.Add(sliceSize)
	for _, rs := range rSlices {
		//rs_ := rs
		pool.Execute(&rs, func(param *rwSlice) {
			//syslog.Printf("pool.Execute rs = %v", rs_)
			time.Sleep(1 * time.Second)
			wg.Done()
		})
	}
	wg.Wait()
	pool.Close()
}
