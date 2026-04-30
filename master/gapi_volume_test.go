// Copyright 2026 The CubeFS Authors.
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

package master

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func gapiVolumeTestContext(userID string, userType proto.UserType) context.Context {
	return context.WithValue(context.Background(), proto.UserInfoKey, &proto.UserInfo{
		UserID:   userID,
		UserType: userType,
		Policy:   proto.NewUserPolicy(),
	})
}

func gapiVolumeTestAdminContext() context.Context {
	return gapiVolumeTestContext("root", proto.UserTypeRoot)
}

func gapiVolumeTestUserContext(userID string) context.Context {
	return gapiVolumeTestContext(userID, proto.UserTypeNormal)
}

func newGAPIVolumeTestCluster(vols ...*Vol) *Cluster {
	cluster := &Cluster{
		ClusterVolSubItem: ClusterVolSubItem{
			vols: make(map[string]*Vol),
		},
	}
	for _, vol := range vols {
		cluster.vols[vol.Name] = vol
	}
	return cluster
}

func newGAPIVolumeTestUser(users ...*proto.UserInfo) *User {
	user := newUser(nil, nil)
	for _, info := range users {
		user.userStore.Store(info.UserID, info)
	}
	return user
}

func newGAPIVolumeTestService(vols ...*Vol) *VolumeService {
	return &VolumeService{
		user:    newGAPIVolumeTestUser(),
		cluster: newGAPIVolumeTestCluster(vols...),
	}
}

func newGAPIVolumeTestVol(name, owner string, status uint8) *Vol {
	vol := newVol(volValue{
		ID:                uint64(len(name) + len(owner) + int(status)),
		Name:              name,
		Owner:             owner,
		ZoneName:          "zone-a",
		DataPartitionSize: 1024,
		Capacity:          100,
		DpReplicaNum:      3,
		ReplicaNum:        3,
		FollowerRead:      true,
		Authenticate:      true,
		CrossZone:         false,
		DefaultPriority:   true,
		CreateTime:        time.Unix(1700000000, 0).Unix(),
		Description:       "test volume " + name,
		VolType:           proto.VolumeTypeHot,
		Status:            status,
	})
	vol.dataPartitions.readableAndWritableCnt = 2
	return vol
}

func addGAPIVolumeTestDataPartition(vol *Vol, partitionID uint64, used uint64) {
	vol.dataPartitions.put(&DataPartition{
		PartitionID: partitionID,
		ReplicaNum:  vol.dpReplicaNum,
		Status:      proto.ReadWrite,
		used:        used,
	})
}

func newGAPIVolumeTestUserInfo(userID string, userType proto.UserType) *proto.UserInfo {
	return &proto.UserInfo{
		UserID:   userID,
		UserType: userType,
		Policy:   proto.NewUserPolicy(),
	}
}

func newGAPIVolumeTestAuthorizedUser(userID, volName string, access ...string) *proto.UserInfo {
	info := newGAPIVolumeTestUserInfo(userID, proto.UserTypeNormal)
	info.Policy.AuthorizedVols[volName] = append([]string(nil), access...)
	return info
}

func gapiVolumeNames(vols []*Vol) []string {
	names := make([]string, 0, len(vols))
	for _, vol := range vols {
		names = append(names, vol.Name)
	}
	sort.Strings(names)
	return names
}

func requireGAPIVolumeErrorContains(t *testing.T, err error, want string) {
	t.Helper()

	require.Error(t, err)
	require.Contains(t, err.Error(), want)
}

func requireGAPIVolumeNoResult(t *testing.T, vol *Vol, err error, want string) {
	t.Helper()

	require.Nil(t, vol)
	requireGAPIVolumeErrorContains(t, err, want)
}

func TestGAPIVolumeGetVolumePermissionCases(t *testing.T) {
	ownerVol := newGAPIVolumeTestVol("owner-vol", "owner", proto.VolStatusNormal)
	otherVol := newGAPIVolumeTestVol("other-vol", "other", proto.VolStatusNormal)
	service := newGAPIVolumeTestService(ownerVol, otherVol)

	tests := []struct {
		name       string
		ctx        context.Context
		volName    string
		wantVol    *Vol
		wantErrMsg string
	}{
		{
			name:    "admin can read owner volume",
			ctx:     gapiVolumeTestAdminContext(),
			volName: ownerVol.Name,
			wantVol: ownerVol,
		},
		{
			name:    "admin can read other owner volume",
			ctx:     gapiVolumeTestAdminContext(),
			volName: otherVol.Name,
			wantVol: otherVol,
		},
		{
			name:    "normal user can read owned volume",
			ctx:     gapiVolumeTestUserContext("owner"),
			volName: ownerVol.Name,
			wantVol: ownerVol,
		},
		{
			name:       "normal user cannot read someone else's volume",
			ctx:        gapiVolumeTestUserContext("owner"),
			volName:    otherVol.Name,
			wantErrMsg: "is not volume",
		},
		{
			name:       "missing volume is returned from cluster lookup",
			ctx:        gapiVolumeTestAdminContext(),
			volName:    "missing",
			wantErrMsg: proto.ErrVolNotExists.Error(),
		},
		{
			name:       "normal user gets missing volume before owner check",
			ctx:        gapiVolumeTestUserContext("owner"),
			volName:    "missing",
			wantErrMsg: proto.ErrVolNotExists.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := service.getVolume(tt.ctx, struct {
				Name string
			}{Name: tt.volName})
			if tt.wantErrMsg != "" {
				requireGAPIVolumeNoResult(t, got, err, tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
			require.Same(t, tt.wantVol, got)
		})
	}
}

func TestGAPIVolumeListVolumeAdminFilters(t *testing.T) {
	normalA := newGAPIVolumeTestVol("alpha", "owner-a", proto.VolStatusNormal)
	normalB := newGAPIVolumeTestVol("beta", "owner-b", proto.VolStatusNormal)
	markDeleteForbiddenFuture := newGAPIVolumeTestVol("delete-later", "owner-a", proto.VolStatusMarkDelete)
	markDeleteForbiddenFuture.Forbidden = true
	markDeleteForbiddenFuture.DeleteExecTime = time.Now().Add(time.Hour)
	markDeleteNow := newGAPIVolumeTestVol("delete-now", "owner-a", proto.VolStatusMarkDelete)
	markDeleteForbiddenExpired := newGAPIVolumeTestVol("delete-expired", "owner-a", proto.VolStatusMarkDelete)
	markDeleteForbiddenExpired.Forbidden = true
	markDeleteForbiddenExpired.DeleteExecTime = time.Now().Add(-time.Minute)
	initializing := newGAPIVolumeTestVol("initializing", "owner-a", proto.VolStatusInitializing)
	initFailed := newGAPIVolumeTestVol("init-failed", "owner-a", proto.VolStatusInitFailed)
	service := newGAPIVolumeTestService(
		normalA,
		normalB,
		markDeleteForbiddenFuture,
		markDeleteNow,
		markDeleteForbiddenExpired,
		initializing,
		initFailed,
	)

	tests := []struct {
		name string
		args struct {
			UserID  *string
			Keyword *string
		}
		wantNames []string
	}{
		{
			name:      "admin sees normal volumes and future forbidden deletes",
			wantNames: []string{"alpha", "beta", "delete-later"},
		},
		{
			name: "admin filters by owner",
			args: struct {
				UserID  *string
				Keyword *string
			}{
				UserID: func() *string { v := "owner-a"; return &v }(),
			},
			wantNames: []string{"alpha", "delete-later"},
		},
		{
			name: "keyword branch skips names containing the keyword",
			args: struct {
				UserID  *string
				Keyword *string
			}{
				Keyword: func() *string { v := "alpha"; return &v }(),
			},
			wantNames: []string{"beta", "delete-later"},
		},
		{
			name: "empty keyword does not filter",
			args: struct {
				UserID  *string
				Keyword *string
			}{
				Keyword: func() *string { v := ""; return &v }(),
			},
			wantNames: []string{"alpha", "beta", "delete-later"},
		},
		{
			name: "unknown owner has empty result",
			args: struct {
				UserID  *string
				Keyword *string
			}{
				UserID: func() *string { v := "nobody"; return &v }(),
			},
			wantNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := service.listVolume(gapiVolumeTestAdminContext(), tt.args)
			require.NoError(t, err)
			require.Equal(t, tt.wantNames, gapiVolumeNames(got))
		})
	}
}

func TestGAPIVolumeListVolumeUserOverridesUserID(t *testing.T) {
	ownerAOne := newGAPIVolumeTestVol("owner-a-1", "owner-a", proto.VolStatusNormal)
	ownerATwo := newGAPIVolumeTestVol("owner-a-2", "owner-a", proto.VolStatusNormal)
	ownerB := newGAPIVolumeTestVol("owner-b-1", "owner-b", proto.VolStatusNormal)
	deleted := newGAPIVolumeTestVol("owner-a-deleted", "owner-a", proto.VolStatusMarkDelete)
	service := newGAPIVolumeTestService(ownerAOne, ownerATwo, ownerB, deleted)
	otherUser := "owner-b"

	got, err := service.listVolume(gapiVolumeTestUserContext("owner-a"), struct {
		UserID  *string
		Keyword *string
	}{
		UserID: &otherUser,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"owner-a-1", "owner-a-2"}, gapiVolumeNames(got))

	keyword := "owner-a-1"
	got, err = service.listVolume(gapiVolumeTestUserContext("owner-a"), struct {
		UserID  *string
		Keyword *string
	}{
		UserID:  &otherUser,
		Keyword: &keyword,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"owner-a-2"}, gapiVolumeNames(got))
}

func TestGAPIVolumeListVolumeRequiresContextUser(t *testing.T) {
	service := newGAPIVolumeTestService(newGAPIVolumeTestVol("vol", "owner", proto.VolStatusNormal))

	require.Panics(t, func() {
		_, _ = service.listVolume(context.Background(), struct {
			UserID  *string
			Keyword *string
		}{})
	})
}

func TestGAPIVolumeVolPermissionAdminCases(t *testing.T) {
	vol := newGAPIVolumeTestVol("vol-main", "owner", proto.VolStatusNormal)
	service := newGAPIVolumeTestService(vol)
	reader := newGAPIVolumeTestAuthorizedUser("reader", vol.Name, "read")
	editor := newGAPIVolumeTestAuthorizedUser("editor", vol.Name, "read", "write")
	noPolicy := newGAPIVolumeTestUserInfo("no-policy", proto.UserTypeNormal)
	service.user = newGAPIVolumeTestUser(reader, editor, noPolicy)
	service.user.volUser.Store(vol.Name, &proto.VolUser{
		Vol:     vol.Name,
		UserIDs: []string{"reader", "missing", "editor", "reader", "no-policy"},
	})

	got, err := service.volPermission(gapiVolumeTestAdminContext(), struct {
		VolName string
		UserID  *string
	}{
		VolName: vol.Name,
	})
	require.NoError(t, err)
	require.Len(t, got, 2)

	byUser := make(map[string]*UserPermission)
	for _, item := range got {
		byUser[item.UserID] = item
	}
	require.ElementsMatch(t, []string{"reader", "editor"}, []string{got[0].UserID, got[1].UserID})
	require.Equal(t, []string{"read"}, byUser["reader"].Access)
	require.Equal(t, []string{"read", "write"}, byUser["editor"].Access)
	require.False(t, byUser["reader"].Edit)
	require.False(t, byUser["editor"].Edit)
}

func TestGAPIVolumeVolPermissionUserCases(t *testing.T) {
	vol := newGAPIVolumeTestVol("shared-vol", "owner", proto.VolStatusNormal)
	ownerGate := newGAPIVolumeTestVol("owner-gate", "owner", proto.VolStatusNormal)
	reader := newGAPIVolumeTestAuthorizedUser("reader", vol.Name, "read")
	service := newGAPIVolumeTestService(vol, ownerGate)
	service.user = newGAPIVolumeTestUser(reader)
	service.user.volUser.Store(vol.Name, &proto.VolUser{
		Vol:     vol.Name,
		UserIDs: []string{"reader"},
	})

	t.Run("normal user must pass user id argument", func(t *testing.T) {
		got, err := service.volPermission(gapiVolumeTestUserContext("owner"), struct {
			VolName string
			UserID  *string
		}{
			VolName: vol.Name,
		})
		require.Nil(t, got)
		requireGAPIVolumeErrorContains(t, err, "need set userID")
	})

	t.Run("normal user gate volume must exist", func(t *testing.T) {
		userID := "missing-gate"
		got, err := service.volPermission(gapiVolumeTestUserContext("owner"), struct {
			VolName string
			UserID  *string
		}{
			VolName: vol.Name,
			UserID:  &userID,
		})
		require.Nil(t, got)
		require.ErrorIs(t, err, proto.ErrVolNotExists)
	})

	t.Run("normal user gate volume must be owned by requester", func(t *testing.T) {
		otherGate := newGAPIVolumeTestVol("other-gate", "other", proto.VolStatusNormal)
		service.cluster.vols[otherGate.Name] = otherGate
		userID := otherGate.Name

		got, err := service.volPermission(gapiVolumeTestUserContext("owner"), struct {
			VolName string
			UserID  *string
		}{
			VolName: vol.Name,
			UserID:  &userID,
		})
		require.Nil(t, got)
		requireGAPIVolumeErrorContains(t, err, "is not volume")
	})

	t.Run("normal owner marks returned permissions editable", func(t *testing.T) {
		userID := ownerGate.Name

		got, err := service.volPermission(gapiVolumeTestUserContext("owner"), struct {
			VolName string
			UserID  *string
		}{
			VolName: vol.Name,
			UserID:  &userID,
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, "reader", got[0].UserID)
		require.Equal(t, []string{"read"}, got[0].Access)
		require.True(t, got[0].Edit)
	})
}

func TestGAPIVolumeVolPermissionErrorCases(t *testing.T) {
	vol := newGAPIVolumeTestVol("vol-main", "owner", proto.VolStatusNormal)
	service := newGAPIVolumeTestService(vol)

	tests := []struct {
		name       string
		setup      func()
		volName    string
		wantErrMsg string
	}{
		{
			name:       "missing volume",
			volName:    "missing-vol",
			wantErrMsg: proto.ErrVolNotExists.Error(),
		},
		{
			name:    "missing vol user",
			volName: vol.Name,
			setup: func() {
				service.user.volUser.Delete(vol.Name)
			},
			wantErrMsg: "not found vol user",
		},
		{
			name:    "vol user exists but all entries are skipped",
			volName: vol.Name,
			setup: func() {
				service.user = newGAPIVolumeTestUser(newGAPIVolumeTestUserInfo("known", proto.UserTypeNormal))
				service.user.volUser.Store(vol.Name, &proto.VolUser{
					Vol:     vol.Name,
					UserIDs: []string{"known", "missing"},
				})
			},
			wantErrMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service.user = newGAPIVolumeTestUser()
			if tt.setup != nil {
				tt.setup()
			}

			got, err := service.volPermission(gapiVolumeTestAdminContext(), struct {
				VolName string
				UserID  *string
			}{
				VolName: tt.volName,
			})
			if tt.wantErrMsg != "" {
				require.Nil(t, got)
				requireGAPIVolumeErrorContains(t, err, tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
			require.Empty(t, got)
		})
	}
}

func TestGAPIVolumeCreateVolumeValidation(t *testing.T) {
	service := newGAPIVolumeTestService()

	tests := []struct {
		name string
		ctx  context.Context
		args struct {
			Name, Owner, ZoneName, Description                          string
			Capacity, DataPartitionSize, MpCount, DpCount, DpReplicaNum uint64
			FollowerRead, Authenticate, CrossZone, DefaultPriority      bool
			iopsRLimit, iopsWLimit, flowRlimit, flowWlimit              uint64
		}
		wantErrMsg string
	}{
		{
			name: "invalid replica number is rejected before cluster create",
			ctx:  gapiVolumeTestAdminContext(),
			args: struct {
				Name, Owner, ZoneName, Description                          string
				Capacity, DataPartitionSize, MpCount, DpCount, DpReplicaNum uint64
				FollowerRead, Authenticate, CrossZone, DefaultPriority      bool
				iopsRLimit, iopsWLimit, flowRlimit, flowWlimit              uint64
			}{
				Name:         "bad-replica",
				Owner:        "owner",
				DpReplicaNum: 1,
			},
			wantErrMsg: "replicaNum can only be 2 and 3",
		},
		{
			name: "normal user cannot create volume for another owner",
			ctx:  gapiVolumeTestUserContext("owner-a"),
			args: struct {
				Name, Owner, ZoneName, Description                          string
				Capacity, DataPartitionSize, MpCount, DpCount, DpReplicaNum uint64
				FollowerRead, Authenticate, CrossZone, DefaultPriority      bool
				iopsRLimit, iopsWLimit, flowRlimit, flowWlimit              uint64
			}{
				Name:         "owner-mismatch",
				Owner:        "owner-b",
				DpReplicaNum: 3,
			},
			wantErrMsg: "not has permission",
		},
		{
			name: "dp count limit is checked before cluster create",
			ctx:  gapiVolumeTestUserContext("owner-a"),
			args: struct {
				Name, Owner, ZoneName, Description                          string
				Capacity, DataPartitionSize, MpCount, DpCount, DpReplicaNum uint64
				FollowerRead, Authenticate, CrossZone, DefaultPriority      bool
				iopsRLimit, iopsWLimit, flowRlimit, flowWlimit              uint64
			}{
				Name:         "too-many-dps",
				Owner:        "owner-a",
				DpReplicaNum: 3,
				DpCount:      maxInitDataPartitionCnt + 1,
			},
			wantErrMsg: "exceeds maximum limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := service.createVolume(tt.ctx, tt.args)
			require.Nil(t, got)
			requireGAPIVolumeErrorContains(t, err, tt.wantErrMsg)
		})
	}
}

func TestGAPIVolumeUpdateVolumeValidation(t *testing.T) {
	owned := newGAPIVolumeTestVol("owned", "owner", proto.VolStatusNormal)
	other := newGAPIVolumeTestVol("other", "other", proto.VolStatusNormal)
	service := newGAPIVolumeTestService(owned, other)

	t.Run("normal user cannot update another owner's volume", func(t *testing.T) {
		got, err := service.updateVolume(gapiVolumeTestUserContext("owner"), struct {
			Name, AuthKey              string
			ZoneName, Description      *string
			Capacity, ReplicaNum       *uint64
			FollowerRead, Authenticate *bool
		}{
			Name: other.Name,
		})
		require.Nil(t, got)
		requireGAPIVolumeErrorContains(t, err, "is not volume")
	})

	t.Run("invalid replica number is rejected", func(t *testing.T) {
		replicaNum := uint64(4)

		got, err := service.updateVolume(gapiVolumeTestAdminContext(), struct {
			Name, AuthKey              string
			ZoneName, Description      *string
			Capacity, ReplicaNum       *uint64
			FollowerRead, Authenticate *bool
		}{
			Name:       owned.Name,
			ReplicaNum: &replicaNum,
		})
		require.Nil(t, got)
		requireGAPIVolumeErrorContains(t, err, "replicaNum can only be 2 and 3")
	})

	t.Run("missing volume is returned for admin update", func(t *testing.T) {
		got, err := service.updateVolume(gapiVolumeTestAdminContext(), struct {
			Name, AuthKey              string
			ZoneName, Description      *string
			Capacity, ReplicaNum       *uint64
			FollowerRead, Authenticate *bool
		}{
			Name: "missing",
		})
		require.Nil(t, got)
		require.ErrorIs(t, err, proto.ErrVolNotExists)
	})

	t.Run("missing volume is returned before user ownership check", func(t *testing.T) {
		got, err := service.updateVolume(gapiVolumeTestUserContext("owner"), struct {
			Name, AuthKey              string
			ZoneName, Description      *string
			Capacity, ReplicaNum       *uint64
			FollowerRead, Authenticate *bool
		}{
			Name: "missing",
		})
		require.Nil(t, got)
		require.ErrorIs(t, err, proto.ErrVolNotExists)
	})
}

func TestGAPIVolumeDataPartitionHelpersSupportObjectFields(t *testing.T) {
	vol := newGAPIVolumeTestVol("usage-vol", "owner", proto.VolStatusNormal)
	addGAPIVolumeTestDataPartition(vol, 11, 10)
	addGAPIVolumeTestDataPartition(vol, 12, 25)
	addGAPIVolumeTestDataPartition(vol, 13, 40)

	var used int64
	for _, partition := range vol.cloneDataPartitionMap() {
		used += int64(partition.used)
	}

	require.Equal(t, int64(75), used)
	require.Equal(t, 3, len(vol.dataPartitions.partitionMap))
	require.Equal(t, 3, len(vol.dataPartitions.partitions))
}

func TestGAPIVolumeSimpleViewFieldsMirrorRegisteredObject(t *testing.T) {
	vol := newGAPIVolumeTestVol("simple-vol", "owner", proto.VolStatusNormal)
	vol.ID = 99
	vol.zoneName = "zone-b"
	vol.dpReplicaNum = 2
	vol.mpReplicaNum = 3
	vol.NeedToLowerReplica = true
	vol.crossZone = true
	vol.createTime = time.Unix(1700001111, 0).Unix()
	vol.description = "description"
	vol.MetaPartitions[1] = &MetaPartition{}
	vol.MetaPartitions[2] = &MetaPartition{}
	addGAPIVolumeTestDataPartition(vol, 1, 1)
	addGAPIVolumeTestDataPartition(vol, 2, 2)
	vol.dataPartitions.readableAndWritableCnt = 7

	view := &proto.SimpleVolView{
		ID:                 vol.ID,
		Name:               vol.Name,
		Owner:              vol.Owner,
		ZoneName:           vol.zoneName,
		DpReplicaNum:       vol.dpReplicaNum,
		MpReplicaNum:       vol.mpReplicaNum,
		Status:             vol.Status,
		Capacity:           vol.Capacity,
		FollowerRead:       vol.FollowerRead,
		NeedToLowerReplica: vol.NeedToLowerReplica,
		Authenticate:       vol.authenticate,
		CrossZone:          vol.crossZone,
		RwDpCnt:            vol.dataPartitions.readableAndWritableCnt,
		MpCnt:              len(vol.MetaPartitions),
		DpCnt:              len(vol.dataPartitions.partitionMap),
		CreateTime:         time.Unix(vol.createTime, 0).Format(proto.TimeFormat),
		Description:        vol.description,
	}

	require.Equal(t, uint64(99), view.ID)
	require.Equal(t, "simple-vol", view.Name)
	require.Equal(t, "owner", view.Owner)
	require.Equal(t, "zone-b", view.ZoneName)
	require.Equal(t, uint8(2), view.DpReplicaNum)
	require.Equal(t, uint8(3), view.MpReplicaNum)
	require.Equal(t, proto.VolStatusNormal, view.Status)
	require.Equal(t, uint64(100), view.Capacity)
	require.True(t, view.FollowerRead)
	require.True(t, view.NeedToLowerReplica)
	require.True(t, view.Authenticate)
	require.True(t, view.CrossZone)
	require.Equal(t, 7, view.RwDpCnt)
	require.Equal(t, 2, view.MpCnt)
	require.Equal(t, 2, view.DpCnt)
	require.Equal(t, time.Unix(1700001111, 0).Format(proto.TimeFormat), view.CreateTime)
	require.Equal(t, "description", view.Description)
}

func TestGAPIVolumePermissionContextModes(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		mode     permissionMode
		wantID   string
		wantPerm permissionMode
		wantErr  string
	}{
		{
			name:     "root is admin",
			ctx:      gapiVolumeTestContext("root", proto.UserTypeRoot),
			mode:     ADMIN,
			wantID:   "root",
			wantPerm: ADMIN,
		},
		{
			name:     "admin user is admin",
			ctx:      gapiVolumeTestContext("admin", proto.UserTypeAdmin),
			mode:     ADMIN | USER,
			wantID:   "admin",
			wantPerm: ADMIN,
		},
		{
			name:     "normal user is user",
			ctx:      gapiVolumeTestContext("user", proto.UserTypeNormal),
			mode:     USER,
			wantID:   "user",
			wantPerm: USER,
		},
		{
			name:     "normal user rejected for admin mode",
			ctx:      gapiVolumeTestContext("user", proto.UserTypeNormal),
			mode:     ADMIN,
			wantID:   "user",
			wantPerm: USER,
			wantErr:  "permissions has err",
		},
		{
			name:     "admin rejected for user only mode",
			ctx:      gapiVolumeTestContext("admin", proto.UserTypeAdmin),
			mode:     USER,
			wantID:   "admin",
			wantPerm: ADMIN,
			wantErr:  "permissions has err",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			userID, perm, err := permissions(tt.ctx, tt.mode)
			require.Equal(t, tt.wantID, userID)
			require.Equal(t, tt.wantPerm, perm)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			requireGAPIVolumeErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestGAPIVolumeListVolumeCombinations(t *testing.T) {
	owners := []string{"owner-a", "owner-b", "owner-c"}
	statuses := []uint8{proto.VolStatusNormal, proto.VolStatusInitializing, proto.VolStatusInitFailed, proto.VolStatusMarkDelete}
	vols := make([]*Vol, 0, len(owners)*len(statuses))
	for ownerIndex, owner := range owners {
		for statusIndex, status := range statuses {
			vol := newGAPIVolumeTestVol(fmt.Sprintf("%s-status-%d", owner, statusIndex), owner, status)
			if status == proto.VolStatusMarkDelete && ownerIndex == 0 {
				vol.Forbidden = true
				vol.DeleteExecTime = time.Now().Add(time.Hour)
			}
			vols = append(vols, vol)
		}
	}
	service := newGAPIVolumeTestService(vols...)

	got, err := service.listVolume(gapiVolumeTestAdminContext(), struct {
		UserID  *string
		Keyword *string
	}{})
	require.NoError(t, err)
	require.Equal(t, []string{
		"owner-a-status-0",
		"owner-a-status-3",
		"owner-b-status-0",
		"owner-c-status-0",
	}, gapiVolumeNames(got))

	for _, owner := range owners {
		t.Run("owner filter "+owner, func(t *testing.T) {
			got, err := service.listVolume(gapiVolumeTestAdminContext(), struct {
				UserID  *string
				Keyword *string
			}{
				UserID: &owner,
			})
			require.NoError(t, err)
			for _, vol := range got {
				require.Equal(t, owner, vol.Owner)
				require.False(t, vol.isInitializingOrInitFailed())
			}
		})
	}
}

func TestGAPIVolumeErrorsStayDescriptive(t *testing.T) {
	service := newGAPIVolumeTestService(newGAPIVolumeTestVol("vol", "owner", proto.VolStatusNormal))
	replicaNum := uint64(9)

	_, createErr := service.createVolume(gapiVolumeTestUserContext("owner"), struct {
		Name, Owner, ZoneName, Description                          string
		Capacity, DataPartitionSize, MpCount, DpCount, DpReplicaNum uint64
		FollowerRead, Authenticate, CrossZone, DefaultPriority      bool
		iopsRLimit, iopsWLimit, flowRlimit, flowWlimit              uint64
	}{
		Name:         "vol",
		Owner:        "owner",
		DpReplicaNum: replicaNum,
	})
	require.Error(t, createErr)
	require.True(t, strings.Contains(createErr.Error(), "replicaNum"))
	require.True(t, strings.Contains(createErr.Error(), "9"))

	_, updateErr := service.updateVolume(gapiVolumeTestAdminContext(), struct {
		Name, AuthKey              string
		ZoneName, Description      *string
		Capacity, ReplicaNum       *uint64
		FollowerRead, Authenticate *bool
	}{
		Name:       "vol",
		ReplicaNum: &replicaNum,
	})
	require.Error(t, updateErr)
	require.True(t, strings.Contains(updateErr.Error(), "replicaNum"))
}
