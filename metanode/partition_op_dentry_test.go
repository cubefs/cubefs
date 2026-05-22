// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const txUpdateDentryTestTxID = "10001_tx_update_idempotent"

func prepareInodeForDentryTest(t *testing.T, mp MetaPartition, mode uint32) (resp *proto.CreateInodeResponse) {
	p := &Packet{}
	req := &proto.CreateInodeRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Mode:        mode,
		StorageType: proto.StorageClass_Replica_SSD,
		PoolId:      proto.DefaultSSDPoolId,
	}
	resp = &proto.CreateInodeResponse{}
	err := mp.CreateInode(req, p, "")
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func getDentryForDentryTest(t *testing.T, mp MetaPartition, parent uint64, name string) (resp *proto.LookupResponse) {
	p := &Packet{}
	req := &proto.LookupRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		ParentID:    parent,
		Name:        name,
	}
	resp = &proto.LookupResponse{}
	err := mp.Lookup(req, p)
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func createDentryForDentryTest(t *testing.T, mp MetaPartition, parent uint64, name string, ino uint64) {
	p := &Packet{}
	req := &proto.CreateDentryRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		ParentID:    parent,
		Name:        name,
		Inode:       ino,
	}
	err := mp.CreateDentry(req, p, "")
	require.NoError(t, err)
}

func getInodeForDentryTest(t *testing.T, mp MetaPartition, ino uint64) (resp *proto.InodeGetResponse) {
	p := &Packet{}
	req := &proto.InodeGetRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		Inode:       ino,
	}
	resp = &proto.InodeGetResponse{}
	err := mp.InodeGet(req, p)
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func testOpCreateDentry(t *testing.T, mp MetaPartition) {
	dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
	fileIno := prepareInodeForDentryTest(t, mp, FileModeType)

	createDentryForDentryTest(t, mp, dirIno.Info.Inode, "test", fileIno.Info.Inode)

	resp := getDentryForDentryTest(t, mp, dirIno.Info.Inode, "test")
	require.EqualValues(t, fileIno.Info.Inode, resp.Inode)

	getResp := getInodeForDentryTest(t, mp, dirIno.Info.Inode)
	require.EqualValues(t, 3, getResp.Info.Nlink)
}

func TestOpCreateDentry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeMem)
	testOpCreateDentry(t, mp)
}

func TestOpCreateDentry_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeRocksDb)
	testOpCreateDentry(t, mp)
}

func deleteDentryForDentryTest(t *testing.T, mp MetaPartition, parent uint64, name string) (resp *proto.DeleteDentryResponse) {
	p := &Packet{}
	req := &proto.DeleteDentryRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		ParentID:    parent,
		Name:        name,
	}
	resp = &proto.DeleteDentryResponse{}
	err := mp.DeleteDentry(req, p, "")
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func testOpDeleteDentry(t *testing.T, mp MetaPartition) {
	dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
	fileIno := prepareInodeForDentryTest(t, mp, FileModeType)

	createDentryForDentryTest(t, mp, dirIno.Info.Inode, "test", fileIno.Info.Inode)

	deleteDentryForDentryTest(t, mp, dirIno.Info.Inode, "test")

	getResp := getInodeForDentryTest(t, mp, dirIno.Info.Inode)
	require.EqualValues(t, 2, getResp.Info.Nlink)
}

func TestOpDeleteDentry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeMem)
	testOpDeleteDentry(t, mp)
}

func TestOpDeleteDentry_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeRocksDb)
	testOpDeleteDentry(t, mp)
}

func updateDentryForDentryTest(t *testing.T, mp MetaPartition, parent uint64, name string, newIno uint64) (resp *proto.UpdateDentryResponse) {
	p := &Packet{}
	req := &proto.UpdateDentryRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		ParentID:    parent,
		Name:        name,
		Inode:       newIno,
	}
	resp = &proto.UpdateDentryResponse{}
	err := mp.UpdateDentry(req, p, "")
	require.NoError(t, err)
	err = p.UnmarshalData(resp)
	require.NoError(t, err)
	return
}

func testUpdateDentry(t *testing.T, mp MetaPartition) {
	dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
	fileIno := prepareInodeForDentryTest(t, mp, FileModeType)
	otherFileIno := prepareInodeForDentryTest(t, mp, FileModeType)

	createDentryForDentryTest(t, mp, dirIno.Info.Inode, "test", fileIno.Info.Inode)

	updateDentryForDentryTest(t, mp, dirIno.Info.Inode, "test", otherFileIno.Info.Inode)

	getResp := getDentryForDentryTest(t, mp, dirIno.Info.Inode, "test")
	require.EqualValues(t, otherFileIno.Info.Inode, getResp.Inode)
}

func TestUpdateDentry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeMem)
	testUpdateDentry(t, mp)
}

func TestUpdateDentry_Rocksdb(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeRocksDb)
	testUpdateDentry(t, mp)
}

func TestDentryOpAuditDeferOnErrors(t *testing.T) {
	const remote = "127.0.0.1"
	vol := func(mp *metaPartition) string { return mp.GetBaseConfig().VolName }
	pid := func(mp *metaPartition) uint64 { return mp.GetBaseConfig().PartitionId }

	cases := []struct {
		name string
		run  func(mp *metaPartition)
	}{
		{
			name: "CreateDentry parent not exist",
			run: func(mp *metaPartition) {
				p := &Packet{}
				_ = mp.CreateDentry(&CreateDentryReq{
					VolName: vol(mp), PartitionID: pid(mp), ParentID: 99999,
					Name: "missing-parent", Inode: 1,
				}, p, remote)
			},
		},
		{
			name: "CreateDentry parent equals inode",
			run: func(mp *metaPartition) {
				p := &Packet{}
				_ = mp.CreateDentry(&CreateDentryReq{
					VolName: vol(mp), PartitionID: pid(mp), ParentID: 10,
					Name: "bad", Inode: 10,
				}, p, remote)
			},
		},
		{
			name: "DeleteDentry not exist",
			run: func(mp *metaPartition) {
				p := &Packet{}
				_ = mp.DeleteDentry(&DeleteDentryReq{
					VolName: vol(mp), PartitionID: pid(mp), ParentID: 1, Name: "no-such-dentry",
				}, p, remote)
			},
		},
		{
			name: "UpdateDentry parent equals inode",
			run: func(mp *metaPartition) {
				p := &Packet{}
				_ = mp.UpdateDentry(&UpdateDentryReq{
					VolName: vol(mp), PartitionID: pid(mp), ParentID: 20,
					Name: "x", Inode: 20,
				}, p, remote)
			},
		},
		{
			name: "DeleteDentryBatch",
			run: func(mp *metaPartition) {
				p := &Packet{}
				_ = mp.DeleteDentryBatch(&BatchDeleteDentryReq{
					VolName: vol(mp), PartitionID: pid(mp), ParentID: 1,
					Dens: []proto.Dentry{{Name: "batch-miss", Inode: 99}},
				}, p, remote)
			},
		},
		{
			name: "TxCreateDentry parent not exist",
			run: func(mp *metaPartition) {
				p := &Packet{}
				_ = mp.TxCreateDentry(&proto.TxCreateDentryRequest{
					VolName: vol(mp), PartitionID: pid(mp), ParentID: 99998,
					Name: "tx-miss", Inode: 2, TxInfo: &proto.TransactionInfo{TxID: "tx1"},
				}, p, remote)
			},
		},
		{
			name: "QuotaCreateDentry parent not exist",
			run: func(mp *metaPartition) {
				p := &Packet{}
				_ = mp.QuotaCreateDentry(&proto.QuotaCreateDentryRequest{
					VolName: vol(mp), PartitionID: pid(mp), ParentID: 99997,
					Name: "quota-miss", Inode: 3,
				}, p, remote)
			},
		},
		{
			name: "TxDeleteDentry not exist",
			run: func(mp *metaPartition) {
				p := &Packet{}
				_ = mp.TxDeleteDentry(&proto.TxDeleteDentryRequest{
					VolName: vol(mp), PartitionID: pid(mp), ParentID: 1,
					Name: "tx-del-miss", Ino: 4, TxInfo: &proto.TransactionInfo{TxID: "tx2"},
				}, p, remote)
			},
		},
		{
			name: "TxUpdateDentry parent equals inode",
			run: func(mp *metaPartition) {
				p := &Packet{}
				_ = mp.TxUpdateDentry(&proto.TxUpdateDentryRequest{
					VolName: vol(mp), PartitionID: pid(mp), ParentID: 30,
					Name: "tx-upd", Inode: 30, TxInfo: &proto.TransactionInfo{TxID: "tx3"},
				}, p, remote)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeMem)
			mp.SetEnableAuditLog(true)
			tc.run(mp)
		})
	}
}

func TestOpDeleteDentryCoversFsmCopyGet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmDentryTest(t, ctrl, proto.StoreModeMem)
	testOpDeleteDentry(t, mp)
}

func insertTxRbDentryForTxUpdateTest(t *testing.T, mp *metaPartition, dentry *Dentry, txID string) {
	t.Helper()
	txDI := proto.NewTxDentryInfo("", dentry.ParentId, dentry.Name, mp.config.PartitionId)
	txDI.TxID = txID
	rbDentry := NewTxRollbackDentry(dentry, txDI, TxUpdate)
	txRsc := mp.txProcessor.txResource
	handle, err := txRsc.txRbDentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := txRsc.addTxRollbackDentry(handle, rbDentry)
	require.NoError(t, err)
	require.Equal(t, proto.OpOk, status)
	err = txRsc.txRbDentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func txUpdateDentryReq(mp *metaPartition, parent uint64, name string, oldIno, newIno uint64, txID string) *proto.TxUpdateDentryRequest {
	return &proto.TxUpdateDentryRequest{
		VolName:     mp.GetBaseConfig().VolName,
		PartitionID: mp.GetBaseConfig().PartitionId,
		ParentID:    parent,
		Name:        name,
		Inode:       newIno,
		OldIno:      oldIno,
		TxInfo:      &proto.TransactionInfo{TxID: txID},
	}
}

func setDentryInodeInTreeForTxUpdateTest(t *testing.T, mp *metaPartition, parent uint64, name string, newIno uint64) {
	t.Helper()
	den := getDentryForFsmDentryTest(t, mp, parent, name)
	handle, err := mp.dentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	den.Inode = newIno
	require.NoError(t, mp.dentryTree.Update(handle, den))
	require.NoError(t, mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false))
}

func callTxUpdateDentry(t *testing.T, mp *metaPartition, req *proto.TxUpdateDentryRequest) *Packet {
	t.Helper()
	p := &Packet{}
	_ = mp.TxUpdateDentry(req, p, "127.0.0.1")
	return p
}

func newMPForTxUpdateDentryTest(t *testing.T, storeMode proto.StoreMode) *metaPartition {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	return mockPartitionRaftForFsmDentryTest(t, ctrl, storeMode)
}

func testTxUpdateDentryIdempotent(t *testing.T, storeMode proto.StoreMode) {
	const dentryName = "tx-update-link"

	t.Run("already updated by current tx returns ok", func(t *testing.T) {
		mp := newMPForTxUpdateDentryTest(t, storeMode)
		dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
		oldFileIno := prepareInodeForDentryTest(t, mp, FileModeType)
		newFileIno := prepareInodeForDentryTest(t, mp, FileModeType)
		parent := dirIno.Info.Inode
		createDentryForDentryTest(t, mp, parent, dentryName, oldFileIno.Info.Inode)
		oldDentry := &Dentry{ParentId: parent, Name: dentryName, Inode: oldFileIno.Info.Inode}
		insertTxRbDentryForTxUpdateTest(t, mp, oldDentry, txUpdateDentryTestTxID)
		setDentryInodeInTreeForTxUpdateTest(t, mp, parent, dentryName, newFileIno.Info.Inode)

		p := callTxUpdateDentry(t, mp, txUpdateDentryReq(mp, parent, dentryName,
			oldFileIno.Info.Inode, newFileIno.Info.Inode, txUpdateDentryTestTxID))
		require.Equal(t, proto.OpOk, p.ResultCode)
		resp := &proto.TxUpdateDentryResponse{}
		require.NoError(t, p.UnmarshalData(resp))
		require.Equal(t, oldFileIno.Info.Inode, resp.Inode)
	})

	t.Run("dentry missing but owned by current tx returns ok", func(t *testing.T) {
		mp := newMPForTxUpdateDentryTest(t, storeMode)
		dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
		oldFileIno := prepareInodeForDentryTest(t, mp, FileModeType)
		newFileIno := prepareInodeForDentryTest(t, mp, FileModeType)
		parent := dirIno.Info.Inode
		oldDentry := &Dentry{ParentId: parent, Name: dentryName, Inode: oldFileIno.Info.Inode}
		insertTxRbDentryForTxUpdateTest(t, mp, oldDentry, txUpdateDentryTestTxID)

		p := callTxUpdateDentry(t, mp, txUpdateDentryReq(mp, parent, dentryName,
			oldFileIno.Info.Inode, newFileIno.Info.Inode, txUpdateDentryTestTxID))
		require.Equal(t, proto.OpOk, p.ResultCode)
	})

	t.Run("dentry missing and not owned returns not exist", func(t *testing.T) {
		mp := newMPForTxUpdateDentryTest(t, storeMode)
		dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
		newFileIno := prepareInodeForDentryTest(t, mp, FileModeType)
		parent := dirIno.Info.Inode

		p := callTxUpdateDentry(t, mp, txUpdateDentryReq(mp, parent, "missing-link",
			100, newFileIno.Info.Inode, txUpdateDentryTestTxID))
		require.Equal(t, proto.OpNotExistErr, p.ResultCode)
	})

	t.Run("already updated but wrong tx returns exist err", func(t *testing.T) {
		mp := newMPForTxUpdateDentryTest(t, storeMode)
		dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
		oldFileIno := prepareInodeForDentryTest(t, mp, FileModeType)
		newFileIno := prepareInodeForDentryTest(t, mp, FileModeType)
		parent := dirIno.Info.Inode
		createDentryForDentryTest(t, mp, parent, dentryName, oldFileIno.Info.Inode)
		oldDentry := &Dentry{ParentId: parent, Name: dentryName, Inode: oldFileIno.Info.Inode}
		insertTxRbDentryForTxUpdateTest(t, mp, oldDentry, "other_tx_id")
		setDentryInodeInTreeForTxUpdateTest(t, mp, parent, dentryName, newFileIno.Info.Inode)

		p := callTxUpdateDentry(t, mp, txUpdateDentryReq(mp, parent, dentryName,
			oldFileIno.Info.Inode, newFileIno.Info.Inode, txUpdateDentryTestTxID))
		require.Equal(t, proto.OpExistErr, p.ResultCode)
	})

	t.Run("old inode mismatch returns not exist", func(t *testing.T) {
		mp := newMPForTxUpdateDentryTest(t, storeMode)
		dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
		oldFileIno := prepareInodeForDentryTest(t, mp, FileModeType)
		newFileIno := prepareInodeForDentryTest(t, mp, FileModeType)
		parent := dirIno.Info.Inode
		createDentryForDentryTest(t, mp, parent, dentryName, oldFileIno.Info.Inode)

		p := callTxUpdateDentry(t, mp, txUpdateDentryReq(mp, parent, dentryName,
			99999, newFileIno.Info.Inode, txUpdateDentryTestTxID))
		require.Equal(t, proto.OpNotExistErr, p.ResultCode)
	})

	t.Run("txUpdateDentryOwnedByCurrentTx", func(t *testing.T) {
		mp := newMPForTxUpdateDentryTest(t, storeMode)
		dirIno := prepareInodeForDentryTest(t, mp, DirModeType)
		oldFileIno := prepareInodeForDentryTest(t, mp, FileModeType)
		newFileIno := prepareInodeForDentryTest(t, mp, FileModeType)
		parent := dirIno.Info.Inode
		oldDentry := &Dentry{ParentId: parent, Name: dentryName, Inode: oldFileIno.Info.Inode}
		insertTxRbDentryForTxUpdateTest(t, mp, oldDentry, txUpdateDentryTestTxID)

		req := txUpdateDentryReq(mp, parent, dentryName, oldFileIno.Info.Inode, newFileIno.Info.Inode, txUpdateDentryTestTxID)
		owned, err := mp.txUpdateDentryOwnedByCurrentTx(req)
		require.NoError(t, err)
		require.True(t, owned)

		owned, err = mp.txUpdateDentryOwnedByCurrentTx(txUpdateDentryReq(mp, parent, dentryName,
			oldFileIno.Info.Inode, newFileIno.Info.Inode, "other_tx"))
		require.NoError(t, err)
		require.False(t, owned)
	})
}

func TestTxUpdateDentryIdempotent(t *testing.T) {
	testTxUpdateDentryIdempotent(t, proto.StoreModeMem)
}

func TestTxUpdateDentryIdempotent_Rocksdb(t *testing.T) {
	testTxUpdateDentryIdempotent(t, proto.StoreModeRocksDb)
}
