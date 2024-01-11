package allocator

import (
	"context"
	"sync"
	"time"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type AllocRet struct {
	BidStart proto.BlobID `json:"bid_start"`
	BidEnd   proto.BlobID `json:"bid_end"`
	Vid      proto.Vid    `json:"vid"`
}

type AllocArgs struct {
	BidCount uint64 `json:"bid_count"`
	cmapi.AllocVolumeV2Args
}

type VolumeMgr interface {
	Alloc(ctx context.Context, args *AllocArgs) ([]AllocRet, error)
	Release(ctx context.Context, args *cmapi.ReleaseVolumes) error
}

type VolumeMgrConf struct {
	RetainIntervalSec int `json:"retain_interval_sec"`
}

type volumeMgrImpl struct {
	conf      VolumeMgrConf
	volumeLck sync.Mutex
	volumes   map[proto.Vid]*cmapi.AllocVolumeInfo
	cmClient  cmapi.APIAccess
	stopCh    <-chan struct{}
}

func NewVolumeMgrImpl(conf VolumeMgrConf, cmClient cmapi.APIAccess, stopCh <-chan struct{}) (VolumeMgr, error) {
	v := &volumeMgrImpl{
		conf:     conf,
		cmClient: cmClient,
		stopCh:   stopCh,
		volumes:  make(map[proto.Vid]*cmapi.AllocVolumeInfo),
	}
	go v.retainAll()
	return v, nil
}

func (v *volumeMgrImpl) Alloc(ctx context.Context, args *AllocArgs) ([]AllocRet, error) {
	// If the bid application succeeds but the volume application fails, this scenario is too low. Don't cache the bid for now
	bids, err := v.cmClient.AllocBid(ctx, &cmapi.BidScopeArgs{Count: args.BidCount})
	if err != nil {
		return nil, err
	}

	ret := make([]AllocRet, 0)
	v.volumeLck.Lock()
	for vid, vol := range v.volumes {
		if vol.CodeMode == args.CodeMode && vol.Free >= args.NeedSize {
			vol.Free -= args.NeedSize
			ret = append(ret, AllocRet{
				BidStart: bids.StartBid,
				BidEnd:   bids.EndBid,
				Vid:      vid,
			})
		}
	}
	v.volumeLck.Unlock()
	if len(ret) != 0 {
		return ret, nil
	}

	vInfo, err := v.cmClient.AllocVolumeV2(ctx, &args.AllocVolumeV2Args)
	if err != nil {
		return nil, err
	}

	for idx, vol := range vInfo.AllocVolumeInfos {
		if vol.CodeMode == args.CodeMode && vol.Free >= args.NeedSize {
			vInfo.AllocVolumeInfos[idx].Free -= args.NeedSize
			ret = append(ret, AllocRet{
				BidStart: bids.StartBid,
				BidEnd:   bids.EndBid,
				Vid:      vol.Vid,
			})
		}
	}

	v.volumeLck.Lock()
	for _, vol := range vInfo.AllocVolumeInfos {
		v.volumes[vol.Vid] = &vol
	}
	v.volumeLck.Unlock()

	return ret, nil
}

func (v *volumeMgrImpl) Release(ctx context.Context, args *cmapi.ReleaseVolumes) error {
	v.volumeLck.Lock()
	deleteKeys(v.volumes, args.NormalVids...)
	deleteKeys(v.volumes, args.SealedVids...)
	v.volumeLck.Unlock()
	return v.cmClient.ReleaseVolume(ctx, args)
}

func (v *volumeMgrImpl) retainAll() {
	tk := time.NewTimer(time.Second * time.Duration(v.conf.RetainIntervalSec))
	defer tk.Stop()
	ctx := context.Background()

	for {
		select {
		case <-tk.C:
			v.retainVolume(ctx)
			tk.Reset(time.Second * time.Duration(v.conf.RetainIntervalSec))

		case <-v.stopCh:
			return
		}
	}
}

func (v *volumeMgrImpl) retainVolume(ctx context.Context) {
	span, ctx := trace.StartSpanFromContext(ctx, "")

	v.volumeLck.Lock()
	tokens := make([]string, 0, len(v.volumes))
	for _, vol := range v.volumes {
		if vol.Token != "" {
			tokens = append(tokens, vol.Token)
		}
	}
	v.volumeLck.Unlock()

	span.Debugf("retain volume, tokens:%v", tokens)
	_, err := v.cmClient.RetainVolume(ctx, &cmapi.RetainVolumeArgs{Tokens: tokens})
	if err != nil {
		span.Errorf("Failed to retain volume, tokens:%v, err:%+v", tokens, err)
	}
}

// deleteKeys removes the keys from the map (if they exist).
func deleteKeys(m map[proto.Vid]*cmapi.AllocVolumeInfo, keys ...proto.Vid) {
	for _, k := range keys {
		delete(m, k)
	}
}
