package ttlstore

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestTTLStore(t *testing.T) {
	var (
		em                = NewTTLStore()
		cnt               = 1000
		exSeconds   int64 = 5
		waitSeconds int64 = 3
	)
	v := struct{}{}
	for i := 1; i <= cnt; i++ {
		em.Store(i, v, exSeconds)
	}
	assert.Equal(t, em.Len(), cnt)
	time.Sleep(time.Second * time.Duration(waitSeconds))
	index := 0
	em.Range(func(k interface{}, v *Val) bool {
		index++
		if v.GetTTL() > exSeconds-waitSeconds {
			assert.FailNowf(t, "", "ttl should be %v, but be %v", exSeconds-waitSeconds, v.GetTTL())
			return false
		}
		return true
	})
	assert.Equal(t, cnt, index)
	em.ClearAll()
	assert.Equal(t, em.Len(), 0)
}

func TestTTLStoreLoad(t *testing.T) {
	var (
		em              = NewTTLStore()
		key             = 123
		v               = 12
		exSeconds int64 = 1
	)
	em.Store(key, v, exSeconds)
	assert.Equal(t, em.TTL(key), exSeconds)
	if val, ok := em.Load(key); !ok {
		assert.Equal(t, true, ok)
		return
	} else {
		assert.Equal(t, v, val.data)
	}
	time.Sleep(time.Second)
	assert.Equal(t, int64(-1), em.TTL(key))
	noKey := "avc"
	if _, ok := em.Load(noKey); ok {
		assert.Equal(t, false, ok)
	}
	assert.Equal(t, int64(-1), em.TTL(noKey))
}

func TestParallelTTLStore(t *testing.T) {
	var (
		em        = NewTTLStore()
		taskCnt   = 5
		excSecond = 20
	)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < taskCnt; i++ {
		// clearAll
		go func() {
			for {
				em.ClearAll()
				time.Sleep(time.Second)
			}
		}()
		// 存
		go func() {
			index := 1
			for {
				r := rand.Intn(excSecond - 1)
				em.Store(index, 0, int64(r))
				index++
			}
		}()
		// 取
		go func() {
			for {
				em.Load(em.Len())
				em.TTL(em.Len())
			}
		}()
		// 删除
		go func() {
			for {
				em.Delete(rand.Int())
			}
		}()
		// 遍历
		go func() {
			for {
				em.Range(func(key interface{}, value *Val) bool {
					return true
				})
			}
		}()
	}
	// stop
	go func() {
		time.Sleep(time.Second * time.Duration(excSecond/2))
	}()
	time.Sleep(time.Second * time.Duration(excSecond))
}
