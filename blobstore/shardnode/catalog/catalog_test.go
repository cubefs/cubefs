package catalog

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var (
	A = gomock.Any()
	C = gomock.NewController

	_, ctx = trace.StartSpanFromContext(context.Background(), "Testing")
)

func mockTransport(tb testing.TB) *base.MockTransport {
	return base.NewMockTransport(C(tb))
}

func TestServerCatalog_New(t *testing.T) {
	cfg := &Config{}
	tp := mockTransport(t)
	cfg.Transport = tp

	tp.EXPECT().GetAllSpaces(A).Return(nil, errors.New("")).Times(1)
	require.NotPanics(t, func() { NewCatalog(ctx, cfg) })

	tp.EXPECT().GetAllSpaces(A).Return(
		[]clustermgr.Space{
			{SpaceID: 1, Name: "space1"},
		}, nil,
	)
	require.NotPanics(t, func() { NewCatalog(ctx, cfg) })
}

func TestServerCatalog_Space(t *testing.T) {
	cfg := &Config{}
	tp := mockTransport(t)
	cfg.Transport = tp

	tp.EXPECT().GetAllSpaces(A).Return(nil, nil).Times(1)
	c := NewCatalog(ctx, cfg)

	sid := proto.SpaceID(1)
	tp.EXPECT().GetSpace(A, A).Return(&clustermgr.Space{
		SpaceID: sid,
		Name:    "space1",
	}, nil)

	space, err := c.GetSpace(ctx, sid)
	require.Nil(t, err)
	require.Equal(t, sid, space.sid)
}

func TestServerCatalog_InitRoute(t *testing.T) {}
