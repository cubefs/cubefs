package stream

import (
	"encoding/json"
	"fmt"
	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util"
	"github.com/chubaoio/cbfs/util/log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

var (
	sk       *proto.StreamKey
	extentId uint64
)

func updateKey123(inode uint64) (extents []proto.ExtentKey, err error) {
	return sk.Extents, nil
}

type ReaderInfo struct {
	extent       *ExtentReader
	ExtentString string
	Offset       int
	Size         int
}

func TestStreamReader_GetReader(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	log.NewLog("log", "test", log.DebugLevel)
	sk = proto.NewStreamKey(2)
	for i := 0; i < 10000; i++ {
		rand.Seed(time.Now().UnixNano())
		ek := proto.ExtentKey{PartitionId: uint32(i), ExtentId: atomic.AddUint64(&extentId, 1),
			Size: uint32(rand.Intn(util.ExtentSize))}
		sk.Put(ek)

	}
	reader, _ := NewStreamReader(2, nil, updateKey123)
	sumSize := sk.Size()
	haveReadSize := 0
	addSize := 0
	for {
		if sumSize <= 0 {
			break
		}
		rand.Seed(time.Now().UnixNano())
		currReadSize := rand.Intn(util.ExtentSize)
		if haveReadSize+currReadSize > int(sk.Size()) {
			currReadSize = int(sk.Size()) - haveReadSize
		}
		canRead, err := reader.initCheck(haveReadSize, currReadSize)
		if err != nil {
			log := fmt.Sprintf("Offset(%v) Size(%v) fileSize(%v) canRead(%v) err(%v)",
				haveReadSize, currReadSize, sk.Size(), canRead, err)
			t.Log(log)
			t.FailNow()
		}
		extents, extentsOffset, extentsSizes := reader.GetReader(haveReadSize, currReadSize)
		readerInfos := make([]*ReaderInfo, 0)
		for index, e := range extents {
			ri := &ReaderInfo{ExtentString: e.toString(), extent: e, Offset: extentsOffset[index], Size: extentsSizes[index]}
			readerInfos = append(readerInfos, ri)
		}
		body, _ := json.Marshal(readerInfos)
		cond := int(extents[0].startInodeOffset)+extentsOffset[0] == haveReadSize
		if !cond {
			t.Logf("cond0 failed,readerInfos(%v),offset(%v) size(%v)", string(body), haveReadSize, currReadSize)
			t.FailNow()
		}
		if len(extents) == 1 {
			cond1 := int(extents[0].startInodeOffset)+extentsOffset[0]+extentsSizes[0] == haveReadSize+currReadSize
			if !cond1 {
				t.Logf("cond1 failed,readerInfos(%v),offset(%v) size(%v)", string(body), haveReadSize, currReadSize)
				t.FailNow()
			}
		}
		if len(extents) == 2 {
			cond2 := int(extents[0].startInodeOffset)+extentsOffset[0]+extentsSizes[0] == int(extents[1].startInodeOffset)
			if !cond2 {
				t.Logf("cond2 failed,readerInfos(%v),offset(%v) size(%v)", string(body), haveReadSize, currReadSize)
				t.FailNow()
			}
			cond3 := int(extents[0].startInodeOffset)+extentsOffset[0]+extentsSizes[0]+extentsOffset[1]+extentsSizes[1] == haveReadSize+currReadSize
			if !cond3 {
				t.Logf("cond3 failed,readerInfos(%v),offset(%v) size(%v)", string(body), haveReadSize, currReadSize)
				t.FailNow()
			}
		}
		if haveReadSize > util.PB {
			fmt.Printf("filesize(%v) haveReadOffset(%v)", sk.Size(), haveReadSize)
			break
		}
		addSize += currReadSize
		if addSize > util.TB {
			fmt.Printf("filesize(%v) haveReadOffset(%v)\n", sk.Size(), haveReadSize)
			addSize = 0
		}
		haveReadSize += currReadSize
		rand.Seed(time.Now().UnixNano())
		ek := proto.ExtentKey{PartitionId: uint32(rand.Intn(1000)), ExtentId: atomic.AddUint64(&extentId, 1),
			Size: uint32(rand.Intn(util.ExtentSize))}
		sk.Put(ek)
	}
}
