package master

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
)

type VolumeService struct {
	user    *User
	cluster *Cluster
}

func (s *VolumeService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()

	s.registerObject(schema)
	s.registerQuery(schema)
	s.registerMutation(schema)

	return schema.MustBuild()
}

func (s *VolumeService) registerObject(schema *schemabuilder.Schema) {
	object := schema.Object("Vol", Vol{})

	object.FieldFunc("dpReplicaNum", func(ctx context.Context, v *Vol) (int32, error) {
		if _, _, err := permissions(ctx, USER|ADMIN); err != nil {
			return 0, err
		}
		return int32(v.dpReplicaNum), nil
	})

	object.FieldFunc("occupied", func(ctx context.Context, v *Vol) (int64, error) {
		if _, _, err := permissions(ctx, USER|ADMIN); err != nil {
			return 0, err
		}
		var used int64
		for _, p := range v.cloneDataPartitionMap() {
			used += int64(p.used)
		}
		return used, nil
	})

	object.FieldFunc("toSimpleVolView", func(ctx context.Context, vol *Vol) (*proto.SimpleVolView, error) {
		if _, _, err := permissions(ctx, USER|ADMIN); err != nil {
			return nil, err
		}
		return &proto.SimpleVolView{
			ID:                   vol.ID,
			Name:                 vol.Name,
			Owner:                vol.Owner,
			ZoneName:             vol.zoneName,
			DpReplicaNum:         vol.dpReplicaNum,
			MpReplicaNum:         vol.mpReplicaNum,
			Status:               vol.Status,
			Capacity:             vol.Capacity,
			FollowerRead:         vol.FollowerRead,
			ForceROW:             vol.ForceROW,
			EnableWriteCache:     vol.enableWriteCache,
			CrossRegionHAType:    vol.CrossRegionHAType,
			NeedToLowerReplica:   vol.NeedToLowerReplica,
			Authenticate:         vol.authenticate,
			EnableToken:          vol.enableToken,
			CrossZone:            vol.crossZone,
			AutoRepair:           vol.autoRepair,
			Tokens:               vol.tokens,
			RwDpCnt:              vol.dataPartitions.readableAndWritableCnt,
			MpCnt:                vol.getMpCnt(),
			DpCnt:                vol.getDpCnt(),
			CreateTime:           time.Unix(vol.createTime, 0).Format(proto.TimeFormat),
			Description:          vol.description,
			Quorum:               vol.getDataPartitionQuorum(),
			ExtentCacheExpireSec: vol.ExtentCacheExpireSec,
			RwMpCnt:              int(vol.getWritableMpCount()),
			MinWritableMPNum:     vol.MinWritableMPNum,
			MinWritableDPNum:     vol.MinWritableDPNum,
		}, nil
	})

	object.FieldFunc("tokens", func(ctx context.Context, vol *Vol) ([]*proto.Token, error) {
		if _, _, err := permissions(ctx, USER|ADMIN); err != nil {
			return nil, err
		}

		list := make([]*proto.Token, 0)
		for _, t := range vol.tokens {
			list = append(list, t)
		}

		return list, nil
	})

	object.FieldFunc("createTime", func(ctx context.Context, v *Vol) (int64, error) {
		if _, _, err := permissions(ctx, USER|ADMIN); err != nil {
			return 0, err
		}
		return v.createTime, nil
	})

	object.FieldFunc("inodeCount", func(ctx context.Context, v *Vol) (int64, error) {
		if _, _, err := permissions(ctx, USER|ADMIN); err != nil {
			return 0, err
		}
		var count uint64 = 0
		v.mpsLock.RLock()
		defer v.mpsLock.RUnlock()
		for _, p := range v.MetaPartitions {
			count += p.InodeCount
		}
		return int64(count), nil
	})

}

func (s *VolumeService) registerQuery(schema *schemabuilder.Schema) {
	query := schema.Query()

	query.FieldFunc("getVolume", s.getVolume)
	query.FieldFunc("listVolume", s.listVolume)
	query.FieldFunc("volPermission", s.volPermission)
}

func (s *VolumeService) registerMutation(schema *schemabuilder.Schema) {
	mutation := schema.Mutation()

	mutation.FieldFunc("createVolume", s.createVolume)
	mutation.FieldFunc("deleteVolume", s.markDeleteVol)
	mutation.FieldFunc("updateVolume", s.updateVolume)

}

type UserPermission struct {
	UserID string
	Access []string
	Edit   bool
}

func (s *VolumeService) volPermission(ctx context.Context, args struct {
	VolName string
	UserID  *string
}) ([]*UserPermission, error) {
	uid, perm, err := permissions(ctx, ADMIN|USER)
	if err != nil {
		return nil, err
	}

	if perm == USER {
		if args.UserID == nil {
			return nil, fmt.Errorf("user:[%s] need set userID", uid)
		}
		if v, e := s.cluster.getVol(*args.UserID); e != nil {
			return nil, e
		} else {
			if v.Owner != uid {
				return nil, fmt.Errorf("user:[%s] is not volume:[%d] onwer", uid, args.UserID)
			}
		}
	}

	vol, err := s.cluster.getVol(args.VolName)
	if err != nil {
		return nil, err
	}

	var volUser *proto.VolUser
	if value, exist := s.user.volUser.Load(args.VolName); exist {
		volUser = value.(*proto.VolUser)
	} else {
		return nil, fmt.Errorf("not found vol user in cluster")
	}

	userPList := make([]*UserPermission, 0, len(volUser.UserIDs))

	userMap := make(map[string]bool)

	for _, u := range volUser.UserIDs {
		v, e := s.user.getUserInfo(u)
		if e != nil {
			log.LogWarnf("get user info by vol has err:[%s]", e.Error())
			continue
		}
		if arr, exist := v.Policy.AuthorizedVols[args.VolName]; exist {
			if userMap[u] {
				continue
			}
			userMap[u] = true
			userPList = append(userPList, &UserPermission{
				UserID: u,
				Access: arr,
				Edit:   uid == vol.Owner,
			})
		} else {
			log.LogWarnf("get vol:[%s] author:[%s] by user policy has err ", args.VolName, u)
		}
	}

	sort.Slice(userPList, func(i, j int) bool {
		return userPList[i].Edit
	})

	return userPList, nil
}

func (s *VolumeService) createVolume(ctx context.Context, args struct {
	Name, Owner, ZoneName, Description                                                   string
	Capacity, DataPartitionSize, MpCount, DpReplicaNum, storeMode, mpPercent, repPercent uint64
	FollowerRead, Authenticate, CrossZone, EnableToken                                   bool
	batchDelInodeCnt, delInodeInterVal                                                   uint32
}) (*Vol, error) {
	uid, per, err := permissions(ctx, ADMIN|USER)
	if err != nil {
		return nil, err
	}

	if !(args.DpReplicaNum == 2 || args.DpReplicaNum == 3) {
		return nil, fmt.Errorf("replicaNum can only be 2 and 3,received replicaNum is[%v]", args.DpReplicaNum)
	}

	if args.storeMode == 0 {
		args.storeMode = uint64(proto.StoreModeMem)
	}
	if !(args.storeMode == uint64(proto.StoreModeMem) || args.storeMode == uint64(proto.StoreModeRocksDb)) {
		return nil, fmt.Errorf("storeMode can only be %d and %d,received storeMode is[%v]", proto.StoreModeMem, proto.StoreModeRocksDb, args.storeMode)
	}

	if args.mpPercent > 100 || args.repPercent > 100 {
		return nil, fmt.Errorf("mpPercent repPercent can only be [0-100],received is[%v - %v]", args.mpPercent, args.repPercent)
	}

	if per == USER && args.Owner != uid {
		return nil, fmt.Errorf("[%s] not has permission to create volume for [%s]", uid, args.Owner)
	}

	vol, err := s.cluster.createVol(args.Name, args.Owner, args.ZoneName, args.Description, int(args.MpCount),
		int(args.DpReplicaNum), defaultReplicaNum, int(args.DataPartitionSize), int(args.Capacity), 0, defaultEcDataNum, defaultEcParityNum, defaultEcEnable,
		args.FollowerRead, args.Authenticate, args.EnableToken, false, false, false, false, false, 0, 0,
		defaultChildFileMaxCount, proto.StoreMode(args.storeMode), proto.MetaPartitionLayout{uint32(args.mpPercent), uint32(args.repPercent)}, nil, proto.CompactDefault,
		proto.DpFollowerReadDelayConfig{false, 0}, args.batchDelInodeCnt, args.delInodeInterVal, false)
	if err != nil {
		return nil, err
	}

	userInfo, err := s.user.getUserInfo(args.Owner)

	if err != nil {
		if err != proto.ErrUserNotExists {
			return nil, err
		}

		var param = proto.UserCreateParam{
			ID:       args.Owner,
			Password: DefaultUserPassword,
			Type:     proto.UserTypeNormal,
		}
		if userInfo, err = s.user.createKey(&param); err != nil {
			return nil, err
		}
	}

	if _, err = s.user.addOwnVol(userInfo.UserID, args.Name); err != nil {
		return nil, err
	}

	return vol, nil
}

func (s *VolumeService) markDeleteVol(ctx context.Context, args struct {
	Name, AuthKey string
}) (*proto.GeneralResp, error) {
	uid, perm, err := permissions(ctx, ADMIN|USER)
	if err != nil {
		return nil, err
	}

	if perm == USER {
		if v, e := s.cluster.getVol(args.Name); e != nil {
			return nil, e
		} else {
			if v.Owner != uid {
				return nil, fmt.Errorf("user:[%s] is not volume:[%s] onwer", uid, args.Name)
			}
		}
	}

	if err = s.user.deleteVolPolicy(args.Name); err != nil {
		return nil, err
	}

	if err = s.cluster.markDeleteVol(args.Name, args.AuthKey); err != nil {
		return nil, err
	}

	log.LogWarnf("delete vol[%s] successfully,from[%s]", args.Name, uid)

	return proto.Success("success"), nil
}

func (s *VolumeService) updateVolume(ctx context.Context, args struct {
	Name, AuthKey                                          string
	ZoneName, Description                                  *string
	Capacity, ReplicaNum, storeMode, mpPercent, repPercent *uint64
	EnableToken, ForceROW, EnableWriteCache                *bool
	FollowerRead, Authenticate, AutoRepair                 *bool
	TrashInterVal                                          *uint64
	batchDelInodeCnt, delInodeInterval                     *uint32
}) (*Vol, error) {
	uid, perm, err := permissions(ctx, ADMIN|USER)
	if err != nil {
		return nil, err
	}

	if perm == USER {
		if v, e := s.cluster.getVol(args.Name); e != nil {
			return nil, e
		} else {
			if v.Owner != uid {
				return nil, fmt.Errorf("user:[%s] is not volume:[%s] onwer", uid, args.Name)
			}
		}
	}

	if args.ReplicaNum != nil && !(*args.ReplicaNum == 2 || *args.ReplicaNum == 3) {
		return nil, fmt.Errorf("replicaNum can only be 2 and 3,received replicaNum is[%v]", args.ReplicaNum)
	}

	vol, err := s.cluster.getVol(args.Name)
	if err != nil {
		return nil, err
	}

	if args.FollowerRead == nil {
		args.FollowerRead = &vol.FollowerRead
	}

	if args.ForceROW == nil {
		args.ForceROW = &vol.ForceROW
	}

	if args.EnableWriteCache == nil {
		args.EnableWriteCache = &vol.enableWriteCache
	}

	if args.Authenticate == nil {
		args.Authenticate = &vol.authenticate
	}

	if args.ZoneName == nil {
		args.ZoneName = &vol.zoneName
	}

	if args.Capacity == nil {
		args.Capacity = &vol.Capacity
	}

	if args.ReplicaNum == nil {
		v := uint64(vol.dpReplicaNum)
		args.ReplicaNum = &v
	}

	if args.EnableToken == nil {
		args.EnableToken = &vol.enableToken
	}

	if args.AutoRepair == nil {
		args.AutoRepair = &vol.autoRepair
	}

	if args.Description == nil {
		args.Description = &vol.description
	}

	if args.storeMode == nil {
		tmp := uint64(vol.DefaultStoreMode)
		args.storeMode = &tmp
	}
	if args.mpPercent == nil {
		tmp := uint64(vol.MpLayout.PercentOfMP)
		args.mpPercent = &tmp
	}
	if args.repPercent == nil {
		tmp := uint64(vol.MpLayout.PercentOfReplica)
		args.repPercent = &tmp
	}

	if !(*args.storeMode == uint64(proto.StoreModeMem) || *args.storeMode == uint64(proto.StoreModeRocksDb)) {
		return nil, fmt.Errorf("storeMode can only be %d and %d,received storeMode is[%v]", proto.StoreModeMem, proto.StoreModeRocksDb, *args.storeMode)
	}

	if *args.mpPercent > 100 || *args.repPercent > 100 {
		return nil, fmt.Errorf("mpPercent repPercent can only be [0-100],received is[%v - %v]", *args.mpPercent, *args.repPercent)
	}

	if args.TrashInterVal == nil {
		args.TrashInterVal = &vol.TrashCleanInterval
	}

	if args.batchDelInodeCnt == nil {
		args.batchDelInodeCnt = &vol.BatchDelInodeCnt
	}

	if args.delInodeInterval == nil {
		args.delInodeInterval = &vol.DelInodeInterval
	}

	if err = s.cluster.updateVol(args.Name, args.AuthKey, *args.ZoneName, *args.Description, *args.Capacity,
		uint8(*args.ReplicaNum), vol.mpReplicaNum, *args.FollowerRead, vol.NearRead, *args.Authenticate, *args.EnableToken, *args.AutoRepair, *args.ForceROW,
		vol.volWriteMutexEnable, false, *args.EnableWriteCache, vol.dpSelectorName, vol.dpSelectorParm, vol.OSSBucketPolicy, vol.CrossRegionHAType,
		vol.dpWriteableThreshold, vol.trashRemainingDays, proto.StoreMode(*args.storeMode), proto.MetaPartitionLayout{uint32(*args.mpPercent), uint32(*args.repPercent)},
		vol.ExtentCacheExpireSec, vol.smartRules, vol.compactTag, vol.FollowerReadDelayCfg, vol.FollReadHostWeight, *args.TrashInterVal,
		*args.batchDelInodeCnt, *args.delInodeInterval, vol.UmpCollectWay, vol.TrashCleanMaxCountEachTime, vol.CleanTrashDurationEachTime, false,
		vol.RemoteCacheBoostPath, vol.RemoteCacheBoostEnable, vol.RemoteCacheAutoPrepare, vol.RemoteCacheTTL); err != nil {
		return nil, err
	}

	log.LogInfof("update vol[%v] successfully\n", args.Name)

	vol, err = s.cluster.getVol(args.Name)
	if err != nil {
		return nil, err
	}

	return vol, nil
}

func (s *VolumeService) listVolume(ctx context.Context, args struct {
	UserID  *string
	Keyword *string
}) ([]*Vol, error) {
	uid, perm, err := permissions(ctx, ADMIN|USER)
	if err != nil {
		return nil, err
	}

	if perm == USER {
		args.UserID = &uid
	}

	var list []*Vol
	for _, vol := range s.cluster.vols {
		if args.UserID != nil && (vol.Owner != *args.UserID && perm != ADMIN) {
			continue
		}

		if args.Keyword != nil && *args.Keyword != "" && strings.Contains(vol.Name, *args.Keyword) {
			continue
		}

		if vol.Status == proto.VolStMarkDelete {
			continue
		}

		list = append(list, vol)
	}
	return list, nil
}

func (s *VolumeService) getVolume(ctx context.Context, args struct {
	Name string
}) (*Vol, error) {

	uid, perm, err := permissions(ctx, ADMIN|USER)
	if err != nil {
		return nil, err
	}

	if perm == USER {
		if v, e := s.cluster.getVol(args.Name); e != nil {
			return nil, e
		} else {
			if v.Owner != uid {
				return nil, fmt.Errorf("user:[%s] is not volume:[%s] onwer", uid, args.Name)
			}
		}
	}

	vol, err := s.cluster.getVol(args.Name)
	if err != nil {
		return nil, err
	}
	return vol, nil
}
