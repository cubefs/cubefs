// Copyright 2022 The CubeFS Authors.
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

package mock

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/golang/mock/gomock"

	cm "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func ProxyMockClusterMgrCli(c *gomock.Controller) cm.APIProxy {
	cmcli := mocks.NewMockClientAPI(c)
	cmcli.EXPECT().RegisterService(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	cmcli.EXPECT().AllocBid(gomock.Any(), gomock.Any()).Return(&cm.BidScopeRet{StartBid: proto.BlobID(1), EndBid: proto.BlobID(10000)}, nil).AnyTimes()
	cmcli.EXPECT().GetConfig(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, key string) (ret string, err error) {
		if key == proto.CodeModeConfigKey {
			policy := []codemode.Policy{
				{
					ModeName: codemode.EC6P6.Name(), MinSize: 0, MaxSize: 0, SizeRatio: 0.3, Enable: true,
				}, {
					ModeName: codemode.EC15P12.Name(), MinSize: 0, MaxSize: 0, SizeRatio: 0.7, Enable: true,
				},
			}
			data, err := json.Marshal(policy)
			return string(data), err
		} else if key == proto.VolumeReserveSizeKey {
			return "1024", nil
		} else if key == proto.VolumeChunkSizeKey {
			return "17179869184", nil
		}
		return
	}).AnyTimes()
	cmcli.EXPECT().AllocVolume(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, args *cm.AllocVolumeArgs) (ret cm.AllocatedVolumeInfos, err error) {
		if !args.CodeMode.IsValid() {
			return cm.AllocatedVolumeInfos{}, errors.New("alloc error")
		}
		now := time.Now().UnixNano()
		rets := cm.AllocatedVolumeInfos{}

		volInfos := make([]cm.AllocVolumeInfo, 0)
		for i := 50; i < 50+args.Count; i++ {
			volInfo := cm.AllocVolumeInfo{
				VolumeInfo: cm.VolumeInfo{
					VolumeInfoBase: cm.VolumeInfoBase{
						CodeMode: args.CodeMode,
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
	cmcli.EXPECT().RetainVolume(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, args *cm.RetainVolumeArgs) (ret cm.RetainVolumes, err error) {
		now := int64(1598000000)
		ret = cm.RetainVolumes{}
		vol := make([]cm.RetainVolume, 0)
		for i, token := range args.Tokens {
			if i < 8 {
				continue
			}
			retainInfo := cm.RetainVolume{Token: token, ExpireTime: now + 500}
			vol = append(vol, retainInfo)
		}
		ret.RetainVolTokens = vol
		return ret, nil
	}).AnyTimes()

	return cmcli
}
