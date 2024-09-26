package metanode

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
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
