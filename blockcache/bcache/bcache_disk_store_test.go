package bcache

import (
	"os"
	"testing"
	"time"
)

var bconf = &bcacheConfig{
	CacheDir: "/tmp/block1:1024000",
}

var bm = newBcacheManager(bconf)

func TestCheckFileTimeout(t *testing.T) {
	f, err := os.Create("test.tmp")
	defer func() {
		f.Close()
		os.Remove("test.tmp")
	}()
	if err != nil {
		t.Fatalf("create file failed %v", err)
	} else {
		f.Write([]byte("123456"))
		time.Sleep(time.Duration(1) * time.Second)
		if checkoutTempFileOuttime("test.tmp") {
			t.Fatalf("test file should not be timeout")
		}
	}
}
