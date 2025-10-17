package base

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestRouteItemRing(t *testing.T) {
	ring := newRouteItemRing(3)
	items, isLatest := ring.getFrom(3)
	assert.Equal(t, 0, len(items))
	assert.Equal(t, true, isLatest)

	for i := 1; i <= 3; i++ {
		item := &RouteItem{
			RouteVersion: proto.RouteVersion(i),
		}
		ring.put(item)
	}
	assert.Equal(t, proto.RouteVersion(1), ring.getMinVer())
	assert.Equal(t, proto.RouteVersion(3), ring.getMaxVer())

	items, isLatest = ring.getFrom(1)
	assert.Equal(t, 2, len(items))
	assert.Equal(t, false, isLatest)

	items, isLatest = ring.getFrom(3)
	assert.Equal(t, 0, len(items))
	assert.Equal(t, true, isLatest)

	item4 := &RouteItem{
		RouteVersion: proto.RouteVersion(4),
	}
	ring.put(item4)
	assert.Equal(t, ring.getMinVer(), proto.RouteVersion(2))
	assert.Equal(t, ring.getMaxVer(), proto.RouteVersion(4))
}
