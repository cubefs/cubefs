package clustermgr

import (
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (s *Service) VolumeRoutesGet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.GetVolumeRoutesArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept VolumeRoutesGet request, args: %v", args)

	// linear read
	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("get volume routes read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	ret, err := s.VolumeMgr.GetVolumeRoutes(ctx, args)
	if err != nil {
		span.Errorf("get volume routes err => %v", errors.Detail(err))
		c.RespondError(err)
		return
	}
	c.RespondJSON(ret)
}
