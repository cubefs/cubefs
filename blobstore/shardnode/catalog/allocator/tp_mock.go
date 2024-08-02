// Copyright 2024 The CubeFS Authors.
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

package allocator

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	cm "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/shardnode/base"
	"github.com/golang/mock/gomock"
)

var A = gomock.Any()

func NewMockAllocTransport(t testing.TB) base.Transport {
	_c := gomock.NewController
	tp := base.NewMockTransport(_c(t))
	tp.EXPECT().AllocBid(A, A).Return(proto.BlobID(1), nil).AnyTimes()
	tp.EXPECT().GetConfig(A, A).DoAndReturn(func(ctx context.Context, key string) (ret string, err error) {
		switch key {
		case proto.CodeModeConfigKey:
			policy := []codemode.Policy{
				{ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 0, SizeRatio: 0.3, Enable: true},
				{ModeName: codemode.EC15P12.Name(), MinSize: 0, MaxSize: 0, SizeRatio: 0.7, Enable: true},
			}
			data, err := json.Marshal(policy)
			return string(data), err
		case proto.VolumeReserveSizeKey:
			return "1024", nil
		case proto.VolumeChunkSizeKey:
			return "17179869184", nil
		default:
			return
		}
	}).AnyTimes()
	tp.EXPECT().AllocVolume(A, A, A, A).DoAndReturn(func(ctx context.Context, isInit bool, mode codemode.CodeMode, count int) (ret cm.AllocatedVolumeInfos, err error) {
		if !mode.IsValid() {
			return cm.AllocatedVolumeInfos{}, errors.New("alloc error")
		}
		now := time.Now().UnixNano()
		rets := cm.AllocatedVolumeInfos{}

		volInfos := make([]cm.AllocVolumeInfo, 0)
		for i := 50; i < 50+count; i++ {
			volInfo := cm.AllocVolumeInfo{
				VolumeInfo: cm.VolumeInfo{
					VolumeInfoBase: cm.VolumeInfoBase{
						CodeMode: mode,
						Vid:      proto.Vid(i),
						Free:     16 * 1024 * 1024 * 1024,
					},
				},
				ExpireTime: 800*int64(math.Pow(10, 9)) + now,
			}
			volInfos = append(volInfos, volInfo)
		}
		rets.AllocVolumeInfos = volInfos

		return rets, nil
	}).AnyTimes()
	tp.EXPECT().RetainVolume(A, A).DoAndReturn(func(ctx context.Context, tokens []string) (ret cm.RetainVolumes, err error) {
		now := int64(1598000000)
		ret = cm.RetainVolumes{}
		vol := make([]cm.RetainVolume, 0)
		for i, token := range tokens {
			if i < 8 {
				continue
			}
			retainInfo := cm.RetainVolume{Token: token, ExpireTime: now + 500}
			vol = append(vol, retainInfo)
		}
		ret.RetainVolTokens = vol
		return ret, nil
	}).AnyTimes()

	return tp
}
