// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cmd

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
)

type clientHandler interface {
	excuteHttp() (err error)
}

type volumeClient struct {
	name     string
	capacity uint64
	opCode   MasterOp
	client   master.IMasterClient
}

func NewVolumeClient(opCode MasterOp, client master.IMasterClient) (vol *volumeClient) {
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
		if vol.capacity <= vv.Capacity {
			return errors.New(fmt.Sprintf("Expand capacity must larger than %v!\n", vv.Capacity))
		}
		if err = vol.client.AdminAPI().VolExpand(vol.name, vol.capacity, util.CalcAuthKey(vv.Owner)); err != nil {
			return
		}
	case OpShrinkVol:
		var vv *proto.SimpleVolView
		if vv, err = vol.client.AdminAPI().GetVolumeSimpleInfo(vol.name); err != nil {
			return
		}
		if vol.capacity >= vv.Capacity {
			return errors.New(fmt.Sprintf("Expand capacity must less than %v!\n", vv.Capacity))
		}
		if err = vol.client.AdminAPI().VolShrink(vol.name, vol.capacity, util.CalcAuthKey(vv.Owner)); err != nil {
			return
		}
	case OpDeleteVol:
	default:

	}

	return
}
