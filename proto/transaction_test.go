package proto

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
	"time"
)

func TestGetMaskFromString(t *testing.T) {
	cs := []struct {
		op  TxOpMask
		str string
	}{
		{TxPause, "pause"},
		{TxOpMaskAll, "all"},
		{TxOpMaskOff, "off"},
		{TxOpMaskCreate | TxOpMaskRename, "create|rename"},
	}

	for _, c := range cs {
		str := GetMaskString(c.op)
		require.Equal(t, str, c.str)
	}
}

func TestMaskContains(t *testing.T) {
	cs := []struct {
		total  TxOpMask
		sub    TxOpMask
		result bool
	}{
		{TxOpMaskOff, TxOpMaskOff, true},
		{TxOpMaskOff, TxOpMaskRemove, false},
		{TxOpMaskRemove | TxOpMaskRename, TxOpMaskRename, true},
		{TxOpMaskAll, TxPause, false},
		{TxOpMaskAll, TxOpMaskRename, true},
		{TxPause, TxPause, true},
	}
	for _, c := range cs {
		got := MaskContains(c.total, c.sub)
		require.Equal(t, c.result, got)
	}
}

func TestTxInodeInfoMarshal(t *testing.T) {
	ifo := &TxInodeInfo{
		Ino:        101,
		MpMembers:  "m1,m2,m3",
		MpID:       11,
		CreateTime: 10110,
		Timeout:    112,
		//TxID:       "tx123",
	}

	bs, err := ifo.Marshal()
	require.NoError(t, err)

	nIfo := &TxInodeInfo{}
	err = nIfo.Unmarshal(bs)
	require.NoError(t, err)

	require.True(t, reflect.DeepEqual(ifo, nIfo))

	nIfo.TxID = "txId1"
	require.False(t, reflect.DeepEqual(ifo, nIfo))
}

func TestTxDentryInfoMarshal(t *testing.T) {
	ifo := &TxDentryInfo{
		ParentId:   101,
		Name:       "dir1",
		MpMembers:  "m1,m2,m3",
		MpID:       11,
		CreateTime: 10110,
		Timeout:    112,
		//TxID:       "tx123",
	}

	bs, err := ifo.Marshal()
	require.NoError(t, err)

	nIfo := &TxDentryInfo{}
	err = nIfo.Unmarshal(bs)
	require.NoError(t, err)

	require.True(t, reflect.DeepEqual(ifo, nIfo))

	nIfo.TxID = "txId1"
	require.False(t, reflect.DeepEqual(ifo, nIfo))
}

func TestTransactionInfo_Marshal(t *testing.T) {
	tx := NewTransactionInfo(1, TxTypeRename)
	tx.RMFinish = true
	tx.DoneTime = 1012
	tx.CreateTime = 101
	tx.State = TxStateRollbackDone

	require.False(t, tx.IsInitialized())

	inoIfo := &TxInodeInfo{
		Ino:  10,
		TxID: "tx_11",
	}
	tx.TxInodeInfos[inoIfo.GetKey()] = inoIfo

	dIfo := &TxDentryInfo{
		ParentId: 10,
		Name:     "na1",
		TxID:     "tx_11",
	}
	tx.TxDentryInfos[dIfo.GetKey()] = dIfo

	bs, err := tx.Marshal()
	require.NoError(t, err)

	ntx := NewTransactionInfo(2, TxTypeLink)
	err = ntx.Unmarshal(bs)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(tx, ntx))

	tx.TxID = "txId_1"
	require.False(t, reflect.DeepEqual(tx, ntx))

	cpTx := tx.Copy().(*TransactionInfo)
	require.True(t, reflect.DeepEqual(tx, cpTx))
}

func TestTransactionInfo_SetCreateInodeId(t *testing.T) {
	tx := NewTransactionInfo(1, TxTypeRename)
	inoIfo := &TxInodeInfo{
		TxID: "tx_11",
	}
	tx.TxInodeInfos[inoIfo.GetKey()] = inoIfo

	ino := uint64(11)
	tx.SetCreateInodeId(ino)

	nifo := tx.TxInodeInfos[ino]
	require.True(t, reflect.DeepEqual(nifo, inoIfo))
	require.True(t, len(tx.TxInodeInfos) == 1)
}

func TestTransactionInfo_Less(t *testing.T) {
	tx := &TransactionInfo{
		TxID: "10_11",
	}
	tx2 := &TransactionInfo{
		TxID: "11_10",
	}
	require.True(t, tx.Less(tx2))
}

func TestTransactionInfo_GroupByMp(t *testing.T) {
	mp1, mp2 := uint64(1), uint64(2)
	addr1, addr2 := "m1,m1", "m2,m2"

	ino1 := &TxInodeInfo{
		Ino:       1,
		MpID:      mp1,
		MpMembers: addr1,
	}
	ino2 := &TxInodeInfo{
		Ino:       2,
		MpID:      mp2,
		MpMembers: addr2,
	}
	d1 := &TxDentryInfo{
		ParentId:  1,
		MpID:      mp1,
		MpMembers: addr1,
	}
	d2 := &TxDentryInfo{
		ParentId:  2,
		MpID:      mp2,
		MpMembers: addr2,
	}

	tx := NewTransactionInfo(0, TxTypeRename)
	tx.TxInodeInfos[ino1.GetKey()] = ino1
	tx.TxInodeInfos[ino2.GetKey()] = ino2
	tx.TxDentryInfos[d1.GetKey()] = d1
	tx.TxDentryInfos[d2.GetKey()] = d2

	mps := tx.GroupByMp()
	require.True(t, len(mps) == 2)
	for id, mp := range mps {
		if id == mp1 {
			require.Equal(t, addr1, mp.Members)
		} else {
			require.Equal(t, addr2, mp.Members)
		}
		require.Equal(t, len(mp.TxDentryInfos), 1)
		require.Equal(t, len(mp.TxInodeInfos), 1)
	}
}

func TestTxFunc(t *testing.T) {
	tx := NewTransactionInfo(1, TxTypeRename)

	require.False(t, tx.IsDone())
	tx.State = TxStateRollbackDone
	require.True(t, tx.IsDone())
	tx.State = TxStateCommitDone
	require.True(t, tx.IsDone())

	// can finish
	tx.RMFinish = true
	tx.DoneTime = time.Now().Unix() - 130
	require.True(t, tx.CanDelete())

	tx.DoneTime = time.Now().Unix() - 61
	require.True(t, tx.IsExpired())
}
