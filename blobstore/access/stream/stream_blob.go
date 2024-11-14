package stream

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/sharding"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

func (h *Handler) GetBlob(ctx context.Context, args *acapi.GetBlobArgs) (*proto.Location, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("get blob args:%+v", *args)

	var blob shardnode.GetBlobRet
	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getShardOpHeader(ctx, &acapi.GetShardCommonArgs{
			ClusterID: args.ClusterID,
			BlobName:  args.BlobName,
			Mode:      args.Mode,
			ShardKeys: args.ShardKeys,
		})
		if err != nil {
			return true, err // not retry
		}

		host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
		if err != nil {
			return true, err
		}

		blob, err = h.shardnodeClient.GetBlob(ctx, host, shardnode.GetBlobArgs{
			Header: header,
			Name:   args.BlobName,
		})
		if err != nil {
			interrupt := h.punishAndUpdate(ctx, &punishArgs{
				ShardOpHeader: header,
				clusterID:     args.ClusterID,
				host:          host,
				err:           err,
			})
			return interrupt, err
		}

		return true, nil
	})

	if rerr != nil {
		span.Errorf("get blob failed, args:%+v, err:%+v", *args, rerr)
	}
	return &blob.Blob.Location, rerr
}

func (h *Handler) CreateBlob(ctx context.Context, args *acapi.CreateBlobArgs) (*proto.Location, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("create blob args:%+v", *args)

	err := h.fixCreateBlobArgs(ctx, args)
	if err != nil {
		return nil, err
	}

	var blob shardnode.CreateBlobRet
	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getShardOpHeader(ctx, &acapi.GetShardCommonArgs{
			ClusterID: args.ClusterID,
			BlobName:  args.BlobName,
			Mode:      acapi.GetShardModeLeader,
			ShardKeys: args.ShardKeys,
		})
		if err != nil {
			return true, err
		}

		host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
		if err != nil {
			return true, err
		}

		blob, err = h.shardnodeClient.CreateBlob(ctx, host, shardnode.CreateBlobArgs{
			Header:    header,
			Name:      args.BlobName,
			CodeMode:  args.CodeMode,
			Size_:     args.Size,
			SliceSize: args.SliceSize,
		})
		if err != nil {
			interrupt := h.punishAndUpdate(ctx, &punishArgs{
				ShardOpHeader: header,
				clusterID:     args.ClusterID,
				host:          host,
				err:           err,
			})
			return interrupt, err
		}
		return true, nil
	})

	if rerr != nil {
		span.Errorf("create blob failed, args:%+v, err:%+v", *args, rerr)
	}
	return &blob.Blob.Location, rerr
}

func (h *Handler) DeleteBlob(ctx context.Context, args *acapi.DelBlobArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("delete blob args:%+v", *args)

	var blob shardnode.GetBlobRet
	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getShardOpHeader(ctx, &acapi.GetShardCommonArgs{
			ClusterID: args.ClusterID,
			BlobName:  args.BlobName,
			Mode:      acapi.GetShardModeLeader,
			ShardKeys: args.ShardKeys,
		})
		if err != nil {
			return true, err
		}

		host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
		if err != nil {
			return true, err
		}

		blob, err = h.shardnodeClient.GetBlob(ctx, host, shardnode.GetBlobArgs{
			Header: header,
			Name:   args.BlobName,
		})

		punishFunc := func() bool {
			return h.punishAndUpdate(ctx, &punishArgs{
				ShardOpHeader: header,
				clusterID:     args.ClusterID,
				host:          host,
				err:           err,
			})
		}
		if err != nil {
			interrupt := punishFunc()
			return interrupt, err
		}

		err = h.shardnodeClient.DeleteBlob(ctx, host, shardnode.DeleteBlobArgs{
			Header: header,
			Name:   args.BlobName,
		})
		if err != nil {
			interrupt := punishFunc()
			return interrupt, err
		}

		return true, nil
	})
	if rerr != nil {
		return rerr
	}

	if rerr != nil {
		span.Errorf("delete blob failed, args:%+v, err:%+v", *args, rerr)
	}
	return h.Delete(ctx, &blob.Blob.Location)
}

func (h *Handler) SealBlob(ctx context.Context, args *acapi.SealBlobArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("seal blob args:%+v", *args)

	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getShardOpHeader(ctx, &acapi.GetShardCommonArgs{
			ClusterID: args.ClusterID,
			BlobName:  args.BlobName,
			Mode:      acapi.GetShardModeLeader,
			ShardKeys: args.ShardKeys,
		})
		if err != nil {
			return true, err
		}

		host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
		if err != nil {
			return true, err
		}

		err = h.shardnodeClient.SealBlob(ctx, host, shardnode.SealBlobArgs{
			Header: header,
			Name:   args.BlobName,
			Slices: args.Slices,
		})
		if err != nil {
			interrupt := h.punishAndUpdate(ctx, &punishArgs{
				ShardOpHeader: header,
				clusterID:     args.ClusterID,
				host:          host,
				err:           err,
			})
			return interrupt, err
		}
		return true, nil
	})

	if rerr != nil {
		span.Errorf("seal blob failed, args:%+v, err:%+v", *args, rerr)
	}
	return rerr
}

func (h *Handler) ListBlob(ctx context.Context, args *acapi.ListBlobArgs) (ret shardnode.ListBlobRet, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("list blob args:%+v", *args)
	defer func() {
		if err != nil {
			span.Errorf("list blob failed, args:%+v, err:%+v", *args, err)
		}
	}()

	if args.ShardID != 0 {
		return h.listSpecificShard(ctx, args)
	}

	return h.listManyShards(ctx, args)
}

func (h *Handler) AllocSlice(ctx context.Context, args *acapi.AllocSliceArgs) (shardnode.AllocSliceRet, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("alloc blob args:%+v", *args)

	var slices shardnode.AllocSliceRet
	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getShardOpHeader(ctx, &acapi.GetShardCommonArgs{
			ClusterID: args.ClusterID,
			BlobName:  args.BlobName,
			Mode:      acapi.GetShardModeLeader,
			ShardKeys: args.ShardKeys,
		})
		if err != nil {
			return true, err
		}

		host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
		if err != nil {
			return true, err
		}

		slices, err = h.shardnodeClient.AllocSlice(ctx, host, shardnode.AllocSliceArgs{
			Header:      header,
			Name:        args.BlobName,
			CodeMode:    args.CodeMode,
			Size_:       args.Size,
			FailedSlice: args.FailSlice,
		})
		if err != nil {
			interrupt := h.punishAndUpdate(ctx, &punishArgs{
				ShardOpHeader: header,
				clusterID:     args.ClusterID,
				host:          host,
				err:           err,
			})
			return interrupt, err
		}
		return true, nil
	})

	if rerr != nil {
		span.Errorf("alloc slice failed, args:%+v, err:%+v", *args, rerr)
	}
	return slices, rerr
}

func (h *Handler) listSpecificShard(ctx context.Context, args *acapi.ListBlobArgs) (shardnode.ListBlobRet, error) {
	span := trace.SpanFromContextSafe(ctx)
	var ret shardnode.ListBlobRet
	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getOpHeaderByID(ctx, args.ClusterID, args.ShardID, args.Mode)
		if err != nil {
			return true, err
		}

		interrupt := false
		ret, interrupt, err = h.listSingleShardEnough(ctx, args, header)
		span.Debugf("list blob, shardID=%d, interrupt:%t, length:%d, err:%+v", args.ShardID, interrupt, len(ret.Blobs), err)

		if err != nil {
			return interrupt, err
		}
		return true, nil
	})
	return ret, rerr
}

func (h *Handler) listManyShards(ctx context.Context, args *acapi.ListBlobArgs) (shardnode.ListBlobRet, error) {
	span := trace.SpanFromContextSafe(ctx)
	shardMgr, err := h.clusterController.GetShardController(args.ClusterID)
	if err != nil {
		return shardnode.ListBlobRet{}, err
	}

	var (
		shard   controller.Shard
		allBlob shardnode.ListBlobRet
	)
	if len(args.Marker) == 0 {
		shard, err = shardMgr.GetFisrtShard(ctx)
		if err != nil {
			return shardnode.ListBlobRet{}, err
		}
	} else {
		unionMarker := acapi.ListBlobEncodeMarker{}
		if err = unionMarker.Unmarshal(args.Marker); err != nil {
			return shardnode.ListBlobRet{}, err
		}
		allBlob.NextMarker = unionMarker.Marker
		shard, err = shardMgr.GetShardByRange(ctx, unionMarker.Range)
		if err != nil {
			return shardnode.ListBlobRet{}, err
		}
	}

	lastRange := shard.GetRange()
	count := args.Count
	for count > 0 {
		var ret shardnode.ListBlobRet
		interrupt := false
		rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
			header, err := h.getOpHeaderByShard(ctx, shardMgr, shard, args.Mode, nil)
			if err != nil {
				return interrupt, err
			}
			args.Marker = allBlob.NextMarker
			ret, interrupt, err = h.listSingleShardEnough(ctx, args, header)
			span.Debugf("list blob, shardID=%d, interrupt:%t, length:%d, err:%+v", args.ShardID, interrupt, len(ret.Blobs), err)

			if err != nil {
				return interrupt, err
			}
			return true, nil
		})
		if rerr != nil {
			return shardnode.ListBlobRet{}, rerr
		}

		allBlob.Blobs = append(allBlob.Blobs, ret.Blobs...)
		allBlob.NextMarker = ret.NextMarker
		count -= uint64(len(ret.Blobs))
		if ret.NextMarker == nil {
			shard, err = shardMgr.GetNextShard(ctx, lastRange)
			if err != nil {
				return shardnode.ListBlobRet{}, err
			}
			// err == nil && shard == nil, means last shard, reach end
			if shard == nil {
				lastRange = sharding.Range{}
				break // reach end
			}
			lastRange = shard.GetRange()
		}
	}

	// reach end, don't need marshal
	if len(allBlob.NextMarker) == 0 && lastRange.Type == 0 {
		return allBlob, nil
	}

	markers := acapi.ListBlobEncodeMarker{
		Range:  lastRange,          // empty, means reach the end; else, means next expect shard
		Marker: allBlob.NextMarker, // empty, means current shard list end; else, means expect begin blob name
	}
	unionMarker, err := markers.Marshal()
	allBlob.NextMarker = unionMarker
	return allBlob, err
}

func (h *Handler) listSingleShardEnough(ctx context.Context, args *acapi.ListBlobArgs, header shardnode.ShardOpHeader) (shardnode.ListBlobRet, bool, error) {
	host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
	if err != nil {
		return shardnode.ListBlobRet{}, true, err
	}

	ret, err := h.shardnodeClient.ListBlob(ctx, host, shardnode.ListBlobArgs{
		Header: header,
		Prefix: args.Prefix,
		Marker: args.Marker,
		Count:  args.Count,
	})
	if err != nil {
		interrupt := h.punishAndUpdate(ctx, &punishArgs{
			ShardOpHeader: header,
			clusterID:     args.ClusterID,
			host:          host,
			err:           err,
		})
		return shardnode.ListBlobRet{}, interrupt, err
	}

	return ret, true, nil
}

func (h *Handler) getShardOpHeader(ctx context.Context, args *acapi.GetShardCommonArgs) (shardnode.ShardOpHeader, error) {
	shardMgr, err := h.clusterController.GetShardController(args.ClusterID)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	if args.ShardKeys == nil {
		args.ShardKeys = [][]byte{args.BlobName}
	}
	shard, err := shardMgr.GetShard(ctx, args.ShardKeys)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	oh, err := h.getOpHeaderByShard(ctx, shardMgr, shard, args.Mode, args.ShardKeys)
	return oh, err
}

func (h *Handler) getOpHeaderByID(ctx context.Context, clusterID proto.ClusterID, shardID proto.ShardID, mode acapi.GetShardMode) (shardnode.ShardOpHeader, error) {
	shardMgr, err := h.clusterController.GetShardController(clusterID)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	shard, err := shardMgr.GetShardByID(ctx, shardID)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	return h.getOpHeaderByShard(ctx, shardMgr, shard, mode, nil)
}

func (h *Handler) getOpHeaderByShard(ctx context.Context, shardMgr controller.IShardController, shard controller.Shard,
	mode acapi.GetShardMode, shardKeys [][]byte,
) (shardnode.ShardOpHeader, error) {
	span := trace.SpanFromContextSafe(ctx)

	spaceID := shardMgr.GetSpaceID()
	info := shard.GetMember(mode, 0)

	oh := shardnode.ShardOpHeader{
		SpaceID:      spaceID,
		DiskID:       info.DiskID,
		Suid:         info.Suid,
		RouteVersion: info.RouteVersion,
		ShardKeys:    shardKeys, // don't need shardKeys when list blob, other required
	}

	span.Debugf("shard op header: %+v", oh)
	return oh, nil
}

func (h *Handler) getShardHost(ctx context.Context, clusterID proto.ClusterID, diskID proto.DiskID) (string, error) {
	span := trace.SpanFromContextSafe(ctx)
	s, err := h.clusterController.GetServiceController(clusterID)
	if err != nil {
		return "", err
	}

	hostInfo, err := s.GetShardnodeHost(ctx, diskID)
	if err != nil {
		return "", err
	}

	span.Debugf("get shard host:%+v", *hostInfo)
	return hostInfo.Host, nil
}

type punishArgs struct {
	shardnode.ShardOpHeader
	clusterID proto.ClusterID
	host      string
	err       error
}

func (h *Handler) punishAndUpdate(ctx context.Context, args *punishArgs) bool {
	span := trace.SpanFromContextSafe(ctx)
	code := rpc.DetectStatusCode(args.err)

	switch code {
	case errcode.CodeDiskBroken: // punish and select new master
		h.punishShardnodeDisk(ctx, args.clusterID, args.DiskID, args.host, "Broken")
		if err1 := h.updateShard(ctx, args); err1 != nil {
			span.Warnf("need update shard node, cluster:%d, err:%+v", args.clusterID, err1)
		}
		return true

	case errcode.CodeShardNodeDiskNotFound: // update and punish
		h.punishShardnodeDisk(ctx, args.clusterID, args.DiskID, args.host, "NotFound")
		if err1 := h.updateShardRoute(ctx, args.clusterID); err1 != nil {
			span.Warnf("need update shard node, cluster:%d, err:%+v", args.clusterID, err1)
		}
		return false

	case errcode.CodeShardDoesNotExist, errcode.CodeShardRouteVersionNeedUpdate: // update
		if err1 := h.updateShardRoute(ctx, args.clusterID); err1 != nil {
			span.Warnf("need update shard node, cluster:%d, err:%+v", args.clusterID, err1)
		}
		return false

	case errcode.CodeShardNodeNotLeader: // select master
		if err1 := h.updateShard(ctx, args); err1 != nil {
			span.Warnf("need update shard node, cluster:%d, err:%+v", args.clusterID, err1)
		}
		return false

	default:
	}

	// err:dial tcp 127.0.0.1:9100: connect: connection refused  ； code:500
	if errorConnectionRefused(args.err) {
		span.Warnf("shardnode connection refused, args:%+v, err:%+v", *args, args.err)
		h.groupRun.Do("shardnode-leader-"+args.DiskID.ToString(), func() (interface{}, error) {
			// must wait have master leader, block wait
			if err1 := h.waitShardnodeNextLeader(ctx, args.clusterID, args.Suid, args.DiskID); err1 != nil {
				span.Warnf("fail to change other shard node, err:%+v", err1)
			}
			h.punishShardnodeDisk(ctx, args.clusterID, args.DiskID, args.host, "Refused")
			return nil, nil
		})
		return false
	}

	return true
}

func (h *Handler) updateShardRoute(ctx context.Context, clusterID proto.ClusterID) error {
	shardMgr, err := h.clusterController.GetShardController(clusterID)
	if err != nil {
		return err
	}

	return shardMgr.UpdateRoute(ctx)
}

func (h *Handler) updateShard(ctx context.Context, args *punishArgs) error {
	shardMgr, err := h.clusterController.GetShardController(args.clusterID)
	if err != nil {
		return err
	}

	shardStat, err := h.getLeaderShardInfo(ctx, args.clusterID, args.host, args.DiskID, args.Suid)
	if err != nil {
		return err
	}

	return shardMgr.UpdateShard(ctx, shardStat)
}

func (h *Handler) waitShardnodeNextLeader(ctx context.Context, clusterID proto.ClusterID, suid proto.Suid, diskID proto.DiskID) error {
	shardMgr, err := h.clusterController.GetShardController(clusterID)
	if err != nil {
		return err
	}
	shard, err := shardMgr.GetShardByID(ctx, suid.ShardID())
	if err != nil {
		return err
	}

	// we get new disk, exclude bad diskID
	newDisk := shard.GetMember(acapi.GetShardModeRandom, diskID)
	host, err := h.getShardHost(ctx, clusterID, newDisk.DiskID)
	if err != nil {
		return err
	}
	// span := trace.SpanFromContextSafe(ctx)
	// span.Debugf("get newDisk:%+v, old host:%s, old disk:%d", newDisk, args.host, args.DiskID)

	shardStat, err := h.getLeaderShardInfo(ctx, clusterID, host, diskID, suid)
	if err != nil {
		return err
	}

	return shardMgr.UpdateShard(ctx, shardStat)
}

func (h *Handler) getLeaderShardInfo(ctx context.Context, clusterID proto.ClusterID, host string, diskID proto.DiskID, suid proto.Suid) (shardnode.ShardStats, error) {
	for i := 0; i < h.ShardnodeRetryTimes; i++ {
		// 1. get leader info
		leader, err := h.shardnodeClient.GetShardStats(ctx, host, shardnode.GetShardArgs{
			DiskID: diskID,
			Suid:   suid,
		})
		if err != nil {
			return shardnode.ShardStats{}, err
		}

		if leader.LeaderDiskID == 0 || leader.LeaderDiskID == diskID {
			time.Sleep(time.Millisecond * time.Duration(h.ShardnodeRetryIntervalMS))
			continue
		}

		// 2. get leader ShardNode host
		leaderHost, err := h.getShardHost(ctx, clusterID, leader.LeaderDiskID)
		if err != nil {
			return shardnode.ShardStats{}, err
		}

		// 3. get leader shard stat, with leader host
		ret, err := h.shardnodeClient.GetShardStats(ctx, leaderHost, shardnode.GetShardArgs{
			DiskID: leader.LeaderDiskID,
			Suid:   leader.LeaderSuid,
		})
		if err != nil {
			return shardnode.ShardStats{}, err
		}
		return ret, nil
	}

	return shardnode.ShardStats{}, errcode.ErrShardNodeNotLeader
}

func (h *Handler) fixCreateBlobArgs(ctx context.Context, args *acapi.CreateBlobArgs) error {
	span := trace.SpanFromContextSafe(ctx)
	if int64(args.Size) > h.maxObjectSize {
		span.Info("exceed max object size", h.maxObjectSize)
		return errcode.ErrAccessExceedSize
	}

	if args.SliceSize == 0 {
		args.SliceSize = atomic.LoadUint32(&h.MaxBlobSize)
		span.Debugf("fill slice size:%d", args.SliceSize)
	}

	if args.CodeMode == 0 {
		args.CodeMode = h.allCodeModes.SelectCodeMode(int64(args.Size))
		span.Debugf("select codemode:%d", args.CodeMode)
	}

	if !args.CodeMode.IsValid() {
		span.Infof("invalid codemode:%d", args.CodeMode)
		return errcode.ErrIllegalArguments
	}

	if args.ClusterID == 0 {
		cluster, err := h.clusterController.ChooseOne()
		if err != nil {
			return err
		}
		args.ClusterID = cluster.ClusterID
		span.Debugf("choose cluster[%+v]", cluster)
	}

	return nil
}
