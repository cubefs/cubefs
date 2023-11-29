package ttlstore

import (
	"sync"
	"time"
)

type Val struct {
	data           interface{}
	expirationTime int64
}

func (v *Val) GetData() interface{} {
	return v.data
}

func (v *Val) GetExpirationTime() int64 {
	return v.expirationTime
}

func (v *Val) GetTTL() int64 {
	return v.expirationTime - time.Now().Unix()
}

type TTLStore struct {
	kv       sync.Map     //key: interface{} value: *Val
}

func NewTTLStore() *TTLStore {
	em := &TTLStore{}
	return em
}

func (em *TTLStore) Store(key, value interface{}, expireSeconds int64) {
	if expireSeconds <= 0 {
		return
	}
	expirationTime := time.Now().Unix() + expireSeconds
	em.kv.Store(key, &Val{
		data:           value,
		expirationTime: expirationTime,
	})
}

func (em *TTLStore) Load(key interface{}) (value *Val, ok bool) {
	var v interface{}
	if v, ok = em.kv.Load(key); ok {
		value = v.(*Val)
		if value.expirationTime <= time.Now().Unix() {
			em.kv.Delete(key)
			value = nil
			ok = false
		}
	}
	return
}

func (em *TTLStore) Delete(key interface{}) {
	em.kv.Delete(key)
}

func (em *TTLStore) Len() (len int) {
	em.kv.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}

func (em *TTLStore) TTL(key interface{}) (ttl int64) {
	if v, ok := em.kv.Load(key); !ok {
		ttl = -1
	} else {
		value := v.(*Val)
		if value.expirationTime <= time.Now().Unix() {
			em.kv.Delete(key)
			ttl = -1
		} else {
			ttl = value.expirationTime - time.Now().Unix()
		}
	}
	return
}

func (em *TTLStore) Range(f func(key interface{}, value *Val) bool) {
	em.kv.Range(func(k, v interface{}) bool {
		value := v.(*Val)
		if value.expirationTime <= time.Now().Unix() {
			em.kv.Delete(k)
			return true
		} else {
			return f(k, value)
		}
	})
}

func (em *TTLStore) ClearAll() {
	em.kv.Range(func(k, v interface{}) bool {
		em.kv.Delete(k)
		return true
	})
}

func (em *TTLStore) FreeLockInfo() {
	now := time.Now().Unix()
	var keys []interface{}
	em.kv.Range(func(k, v interface{}) bool {
		value := v.(*Val)
		if value.expirationTime <= now {
			keys = append(keys, k)
		}
		return true
	})
	em.batchDelete(keys)
}

func (em *TTLStore) batchDelete(keys []interface{}) {
	for _, key := range keys {
		em.kv.Delete(key)
	}
}