package cache

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDentry(t *testing.T)  {
	dentryMap := make(map[string]uint64)
	dentryMap["file1"] = 10
	dentryMap["file2"] = 20000
	dentryMap["file3"] = 1677310

	dc := NewDentryCache(1*time.Minute, true)
	for name, ino := range dentryMap {
		dc.Put(name, ino)
	}
	for name, ino := range dentryMap {
		actualIno, ok := dc.Get(name)
		assert.Equal(t, true, ok, "get existed dentry")
		assert.Equal(t, ino, actualIno, "get inode ID of dentry")
	}
	dc.Delete("file1")
	_, ok := dc.Get("file1")
	assert.Equal(t, false, ok, "get not existed dentry")
	assert.Equal(t, 2, dc.Count(), "get count of dentry cache")
	assert.Equal(t, false, dc.IsEmpty(), "get not empty dentry cache")
	assert.Equal(t, false, dc.IsExpired(), "get valid dentry cache")
	dc.ResetExpiration(5 * time.Second)
	assert.Equal(t, true, dc.Expiration().Before(time.Now().Add(5*time.Second)), "the expiration of dentry cache")

	time.Sleep(5 * time.Second)
	assert.Equal(t, true, dc.IsExpired(), "get invalid dentry cache")
	assert.Equal(t, 2, dc.Count(), "get count of dentry cache")
	_, ok = dc.Get("file2")
	assert.Equal(t, false, ok, "get not existed dentry")
	assert.Equal(t, true, dc.IsEmpty(), "get empty dentry cache")
}