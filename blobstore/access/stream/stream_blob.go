package stream

import (
	"context"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
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

	return rerr
}

func (h *Handler) ListBlob(ctx context.Context, args *acapi.ListBlobArgs) (*shardnode.ListBlobRet, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("list blob args:%+v", *args)

	if args.ShardID != 0 {
		return h.listSpecificShard(ctx, args)
	}

	return h.listManyShards(ctx, args)
}

func (h *Handler) AllocSlice(ctx context.Context, args *acapi.AllocSliceArgs) (*shardnode.AllocSliceRet, error) {
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

	return &slices, rerr
}

func (h *Handler) listSpecificShard(ctx context.Context, args *acapi.ListBlobArgs) (*shardnode.ListBlobRet, error) {
	var ret *shardnode.ListBlobRet
	rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
		header, err := h.getShardHeaderByID(ctx, args.ClusterID, args.ShardID)
		if err != nil {
			return true, err
		}

		interrupt := false
		ret, interrupt, err = h.listSingleShardEnough(ctx, args, header)
		if err != nil {
			return interrupt, err
		}
		return true, nil
	})
	return ret, rerr
}

func (h *Handler) listManyShards(ctx context.Context, args *acapi.ListBlobArgs) (*shardnode.ListBlobRet, error) {
	shardMgr, err := h.clusterController.GetShardController(args.ClusterID)
	if err != nil {
		return nil, err
	}

	var (
		shard   controller.Shard
		allBlob shardnode.ListBlobRet
	)
	if len(args.Marker) == 0 {
		shard, err = shardMgr.GetFisrtShard(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		unionMarker := shardnode.ListBlobEncodeMarker{}
		if err = unionMarker.Unmarshal(args.Marker); err != nil {
			return nil, err
		}
		allBlob.NextMarker = unionMarker.Marker
		shard, err = shardMgr.GetShardByRange(ctx, unionMarker.Range)
		if err != nil {
			return nil, err
		}
	}

	lastRange := shard.GetRange()
	count := args.Count
	for count > 0 {
		var ret *shardnode.ListBlobRet
		interrupt := false
		rerr := retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
			header := h.getOpHeader(shardMgr, shard)
			args.Marker = allBlob.NextMarker
			ret, interrupt, err = h.listSingleShardEnough(ctx, args, header)
			if err != nil {
				return interrupt, err
			}
			return true, nil
		})
		if rerr != nil {
			return nil, rerr
		}

		allBlob.Blobs = append(allBlob.Blobs, ret.Blobs...)
		allBlob.NextMarker = ret.NextMarker
		count -= uint64(len(ret.Blobs))
		if ret.NextMarker == nil {
			shard, err = shardMgr.GetNextShard(ctx, lastRange)
			if err != nil {
				return nil, err
			}
			lastRange = shard.GetRange()
		}
	}

	markers := shardnode.ListBlobEncodeMarker{
		Range:  lastRange,
		Marker: allBlob.NextMarker,
	}
	unionMarker, err := markers.Marshal()
	allBlob.NextMarker = unionMarker
	return &allBlob, err
}

func (h *Handler) listSingleShardEnough(ctx context.Context, args *acapi.ListBlobArgs, header shardnode.ShardOpHeader) (*shardnode.ListBlobRet, bool, error) {
	host, err := h.getShardHost(ctx, args.ClusterID, header.DiskID)
	if err != nil {
		return nil, true, err
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
		return nil, interrupt, err
	}

	return &ret, true, nil
}

func (h *Handler) getShardOpHeader(ctx context.Context, args *acapi.GetShardCommonArgs) (shardnode.ShardOpHeader, error) {
	shardMgr, err := h.clusterController.GetShardController(args.ClusterID)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	shardKeys := args.ShardKeys
	if shardKeys == nil {
		shardKeys = [][]byte{args.BlobName}
	}
	shardInfo, err := shardMgr.GetShard(ctx, shardKeys)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	var info controller.ShardOpInfo
	switch args.Mode {
	case acapi.GetShardModeLeader:
		info = shardInfo.GetShardLeader()
	case acapi.GetShardModeRandom:
		info = shardInfo.GetShardRandom()
	default:
		return shardnode.ShardOpHeader{}, errcode.ErrIllegalArguments
	}

	spaceID := shardMgr.GetSpaceID()
	return shardnode.ShardOpHeader{
		SpaceID:      spaceID,
		DiskID:       info.DiskID,
		Suid:         info.Suid,
		RouteVersion: info.RouteVersion,
		ShardKeys:    shardKeys,
	}, nil
}

func (h *Handler) getShardHeaderByID(ctx context.Context, clusterID proto.ClusterID, shardID proto.ShardID) (shardnode.ShardOpHeader, error) {
	shardMgr, err := h.clusterController.GetShardController(clusterID)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	shard, err := shardMgr.GetShardByID(ctx, shardID)
	if err != nil {
		return shardnode.ShardOpHeader{}, err
	}

	info := shard.GetShardRandom()

	spaceID := shardMgr.GetSpaceID()
	return shardnode.ShardOpHeader{
		SpaceID:      spaceID,
		DiskID:       info.DiskID,
		Suid:         info.Suid,
		RouteVersion: info.RouteVersion,
	}, nil
}

func (h *Handler) getOpHeader(shardMgr controller.IShardController, shard controller.Shard) shardnode.ShardOpHeader {
	info := shard.GetShardRandom()

	spaceID := shardMgr.GetSpaceID()
	return shardnode.ShardOpHeader{
		SpaceID:      spaceID,
		DiskID:       info.DiskID,
		Suid:         info.Suid,
		RouteVersion: info.RouteVersion,
	}
}

func (h *Handler) getShardHost(ctx context.Context, clusterID proto.ClusterID, diskID proto.DiskID) (string, error) {
	s, err := h.clusterController.GetServiceController(clusterID)
	if err != nil {
		return "", err
	}

	hostInfo, err := s.GetShardnodeHost(ctx, diskID)
	if err != nil {
		return "", err
	}

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
	case errcode.CodeDiskBroken: // punish
		h.punishShardnodeDisk(ctx, args.clusterID, args.DiskID, args.host, "Broken")
		return true

	case errcode.CodeShardNodeDiskNotFound: // update and punish
		if err1 := h.updateShardRoute(ctx, args.clusterID); err1 != nil {
			span.Warnf("need update shard node, cluster:%d, err:%+v", args.clusterID, err1)
		}
		h.punishShardnodeDisk(ctx, args.clusterID, args.DiskID, args.host, "NotFound")
		return false

	case errcode.CodeShardDoesNotExist, errcode.CodeShardRouteVersionNeedUpdate: // update
		if err1 := h.updateShardRoute(ctx, args.clusterID); err1 != nil {
			span.Warnf("need update shard node, cluster:%d, err:%+v", args.clusterID, err1)
		}
		return false

	case errcode.CodeShardNodeNotLeader:
		if err1 := h.updateShard(ctx, args); err1 != nil {
			span.Warnf("need update shard node, cluster:%d, err:%+v", args.clusterID, err1)
		}
		return false

	default:
	}
	return true
}

func (h *Handler) punishShardnodeDisk(ctx context.Context, clusterID proto.ClusterID, diskID proto.DiskID, host, reason string) {
	reportUnhealth(clusterID, "punish", "shardnode", host, reason)
	if serviceController, err := h.clusterController.GetServiceController(clusterID); err == nil {
		serviceController.PunishShardnode(ctx, diskID, h.DiskPunishIntervalS)
	}
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

	ret, err := h.shardnodeClient.GetShardStats(ctx, args.host, shardnode.GetShardArgs{
		DiskID: args.DiskID,
		Suid:   args.Suid,
	})
	if err != nil {
		return err
	}

	return shardMgr.UpdateShard(ctx, ret)
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
		span.Debugf("choose cluster[%v]", cluster)
	}

	return nil
}
