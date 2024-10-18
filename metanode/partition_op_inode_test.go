package metanode

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestInodeGet(t *testing.T) {
	initMp(t)

	inoId := uint64(time.Now().Unix())
	ino := NewInode(inoId, 0)
	ino.AccessTime = time.Now().Unix() - 3600
	mp.inodeTree.ReplaceOrInsert(ino, false)

	req := &InodeGetReq{
		Inode: inoId,
	}

	now := time.Now()
	time.Sleep(time.Second * 2)

	pkt := &Packet{}
	err := mp.InodeGet(req, pkt)

	require.True(t, pkt.ResultCode == proto.OpOk)
	require.NoError(t, err)

	resp := &proto.InodeGetResponse{}

	err = json.Unmarshal(pkt.Data, resp)
	require.NoError(t, err)

	t.Logf("now %s, atime %s", now.String(), resp.Info.AccessTime.String())

	require.True(t, resp.Info.AccessTime.After(now))

	req.InnerReq = true
	err = mp.InodeGet(req, pkt)
	require.NoError(t, err)
	err = json.Unmarshal(pkt.Data, resp)
	require.NoError(t, err)
	require.True(t, resp.Info.AccessTime.Unix() == ino.AccessTime)
}

func TestInodeGetPerf(t *testing.T) {
	initMp(t)
	cnt := 10240000
	for idx := 1; idx < cnt; idx++ {
		ino := NewInode(uint64(idx), 0)
		mp.inodeTree.ReplaceOrInsert(ino, true)
	}

	testNum := 1024
	for i := 1; i < 10; i++ {
		ids := make([]uint64, 0, testNum)
		for i := 0; i < testNum; i++ {
			ids = append(ids, rand.Uint64()%uint64(testNum)+1)
		}

		start := time.Now()
		for _, id := range ids {
			ino := NewInode(id, 0)
			item := mp.inodeTree.CopyGet(ino)
			require.NotNil(t, item)
			newIno := item.(*Inode)
			newIno.AccessTime = timeutil.GetCurrentTimeUnix()
		}
		t.Logf("TestInodeGetPerf: cnt %d, cost %dus", testNum, time.Since(start).Microseconds())
	}
}
