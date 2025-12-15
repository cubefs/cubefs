package volumemgr

import (
	"context"

	"github.com/gogo/protobuf/types"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func (v *VolumeMgr) GetVolumeRoutes(ctx context.Context, args *clustermgr.GetVolumeRoutesArgs) (ret *clustermgr.GetVolumeRoutesRet, err error) {
	span := trace.SpanFromContextSafe(ctx)

	var (
		items    []*base.RouteItem
		isLatest bool
	)
	if args.RouteVersion > 0 {
		items, isLatest = v.routeMgr.GetRouteItems(ctx, args.RouteVersion)
	}
	ret = new(clustermgr.GetVolumeRoutesRet)
	if items == nil && !isLatest {
		// get all routes，must getRouteVersion first
		ret.RouteVersion = proto.RouteVersion(v.routeMgr.GetRouteVersion())
		vols := v.all.list()
		items = make([]*base.RouteItem, 0, len(vols))
		for _, vol := range vols {
			items = append(items, &base.RouteItem{
				Type:       proto.RouteItemTypeAddVolume,
				ItemDetail: &routeItemVolumeAdd{Vid: vol.vid},
			})
		}
	}

	for i := range items {
		ret.Items = append(ret.Items, clustermgr.VolumeRouteItem{
			RouteVersion: items[i].RouteVersion,
			Type:         items[i].Type.(proto.VolumeRouteItemType),
		})
		switch items[i].Type {
		case proto.RouteItemTypeAddVolume:
			vid := items[i].ItemDetail.(*routeItemVolumeAdd).Vid
			vol := v.all.getVol(vid)
			addVolumeItem := &clustermgr.RouteItemAddVolume{
				Vid: vid,
			}
			vol.withRLocked(func() error {
				if items[i].RouteVersion == proto.InvalidRouteVersion {
					ret.Items[i].RouteVersion = vol.volInfoBase.RouteVersion
				}
				for _, unit := range vol.vUnits {
					addVolumeItem.Units = append(addVolumeItem.Units, clustermgr.VolumeUnitInfoBase(*unit.vuInfo))
				}
				addVolumeItem.VolumeInfoBase = clustermgr.VolumeInfoBasePB(vol.volInfoBase)
				return nil
			})
			ret.Items[i].Item, err = types.MarshalAny(addVolumeItem)
			span.Debugf("addVolumeItem: %+v", addVolumeItem)
		case proto.RouteItemTypeUpdateVolume:
			vuidPrefix := items[i].ItemDetail.(*routeItemVolumeUpdate).VuidPrefix
			vol := v.all.getVol(vuidPrefix.Vid())
			updateVolumeItem := &clustermgr.RouteItemUpdateVolume{
				Vid: vuidPrefix.Vid(),
			}
			vol.withRLocked(func() error {
				unit := vol.vUnits[vuidPrefix.Index()]
				updateVolumeItem.Unit = clustermgr.VolumeUnitInfoBase(*unit.vuInfo)
				updateVolumeItem.VolumeInfoBase = clustermgr.VolumeInfoBasePB(vol.volInfoBase)
				return nil
			})
			ret.Items[i].Item, err = types.MarshalAny(updateVolumeItem)
			span.Debugf("updateVolumeItem: %+v", updateVolumeItem)
		default:
		}

		if err != nil {
			return nil, err
		}
	}

	if ret.RouteVersion == 0 && len(ret.Items) > 0 {
		ret.RouteVersion = ret.Items[len(ret.Items)-1].RouteVersion
	}
	return ret, nil
}
