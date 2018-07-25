package blob

import (
	"testing"
	"time"

	"github.com/tiglabs/baudstorage/util/log"
)

const (
	TestVolName    = "intest"
	TestMasterAddr = "10.196.31.173:80,10.196.31.141:80,10.196.30.200:80"
	TestLogPath    = "testlog"
)

var gBlobClient *BlobClient

func init() {
	_, err := log.NewLog(TestLogPath, "Blob_UT", log.DebugLevel)
	if err != nil {
		panic(err)
	}

	bc, err := NewBlobClient(TestVolName, TestMasterAddr)
	if err != nil {
		panic(err)
	}
	gBlobClient = bc
}

func TestGetVol(t *testing.T) {
	dplist := gBlobClient.partitions.List()
	for _, dp := range dplist {
		t.Logf("%v", dp)
	}
	time.Sleep(2 * time.Second)
}

func TestWrite(t *testing.T) {
	data := []byte("1234")
	key, err := gBlobClient.Write(data)
	if err != nil {
		t.Errorf("Write: data(%v) err(%v)", string(data), err)
	}
	t.Logf("Write: key(%v)", key)

	time.Sleep(2 * time.Second)
}
