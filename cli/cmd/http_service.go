package cmd

import (
	"github.com/chubaofs/chubaofs/sdk/master"
)

type clientHandler interface {
	excuteHttp(owner string) (err error)
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

func (vol *volumeClient) excuteHttp(owner string) (err error) {
	switch vol.opCode {
	case OpExpandVol:
		if err = vol.client.AdminAPI().VolExpand(vol.name, vol.capacity, calcAuthKey(owner)); err != nil {
			return
		}
	case OpShrinkVol:
		if err = vol.client.AdminAPI().VolShrink(vol.name, vol.capacity, calcAuthKey(owner)); err != nil {
			return
		}
	case OpDeleteVol:
	default:
	}
	return
}
