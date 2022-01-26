package cmd

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
)

type clientHandler interface {
	excuteHttp() (err error)
}

type volumeClient struct {
	name     string
	capacity uint64
	opCode   MasterOp
	client   *master.MasterClient
}

func NewVolumeClient(opCode MasterOp, client *master.MasterClient) (vol *volumeClient) {
	vol = new(volumeClient)
	vol.opCode = opCode
	vol.client = client
	return
}

func (vol *volumeClient) excuteHttp() (err error) {
	switch vol.opCode {
	case OpExpandVol:
		var vv *proto.SimpleVolView
		if vv, err = vol.client.AdminAPI().GetVolumeSimpleInfo(vol.name); err != nil {
			return
		}
		if err = vol.client.AdminAPI().VolExpand(vol.name, vol.capacity, calcAuthKey(vv.Owner)); err != nil {
			return
		}
	case OpShrinkVol:
		var vv *proto.SimpleVolView
		if vv, err = vol.client.AdminAPI().GetVolumeSimpleInfo(vol.name); err != nil {
			return
		}
		if err = vol.client.AdminAPI().VolShrink(vol.name, vol.capacity, calcAuthKey(vv.Owner)); err != nil {
			return
		}
	case OpDeleteVol:
	default:

	}

	return
}
