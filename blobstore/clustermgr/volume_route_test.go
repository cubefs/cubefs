package clustermgr

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestVolume_Route(t *testing.T) {
	testService, clean := initServiceWithData()
	defer clean()
	cmClient := initTestClusterClient(testService)
	ctx := newCtx()

	// get all routes
	ret, err := cmClient.GetVolumeRoutes(ctx, &clustermgr.GetVolumeRoutesArgs{})
	require.NoError(t, err)
	require.Equal(t, 10, len(ret.Items))
	require.Equal(t, proto.RouteVersion(1), ret.RouteVersion)

	// get route without specified routeVersion
	ret, err = cmClient.GetVolumeRoutes(ctx, &clustermgr.GetVolumeRoutesArgs{RouteVersion: 1})
	require.NoError(t, err)
	require.Equal(t, 0, len(ret.Items))
	require.Equal(t, proto.RouteVersion(0), ret.RouteVersion)

	// get route with routeVersion which is bigger than CM's
	ret, err = cmClient.GetVolumeRoutes(ctx, &clustermgr.GetVolumeRoutesArgs{RouteVersion: 100})
	require.NoError(t, err)
	require.Equal(t, 0, len(ret.Items))
	require.Equal(t, proto.RouteVersion(0), ret.RouteVersion)
}
