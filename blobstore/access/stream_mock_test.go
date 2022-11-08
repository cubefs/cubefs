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

package access

// github.com/cubefs/cubefs/blobstore/access/... module access interfaces
//go:generate mockgen -destination=./controller_mock_test.go -package=access -mock_names ClusterController=MockClusterController,ServiceController=MockServiceController,VolumeGetter=MockVolumeGetter github.com/cubefs/cubefs/blobstore/access/controller ClusterController,ServiceController,VolumeGetter
//go:generate mockgen -destination=./access_mock_test.go -package=access -mock_names StreamHandler=MockStreamHandler,Limiter=MockLimiter github.com/cubefs/cubefs/blobstore/access StreamHandler,Limiter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/golang/mock/gomock"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/ec"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

var (
	errNotFound     = errors.New("not found")
	errAllocTimeout = errors.New("alloc timeout")

	allocTimeoutSize uint64 = 1 << 40
	punishServiceS          = 1
	minReadShardsX          = 5

	idc        = "test-idc"
	idcOther   = "test-idc-other"
	allID      = []int{1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012}
	idcID      = []int{1001, 1002, 1003, 1007, 1008, 1009}
	idcOtherID = []int{1004, 1005, 1006, 1010, 1011, 1012}

	clusterID = proto.ClusterID(1)
	volumeID  = proto.Vid(1)
	blobSize  = 1 << 22

	streamer *Handler

	memPool     *resourcepool.MemPool
	encoder     map[codemode.CodeMode]ec.Encoder
	proxyClient proxy.Client

	allCodeModes CodeModePairs

	cmcli             clustermgr.APIAccess
	volumeGetter      controller.VolumeGetter
	serviceController controller.ServiceController
	cc                controller.ClusterController

	clusterInfo *clustermgr.ClusterInfo
	dataVolume  *proxy.VersionVolume
	dataAllocs  []proxy.AllocRet
	dataNodes   map[string]clustermgr.ServiceInfo
	dataDisks   map[proto.DiskID]blobnode.DiskInfo
	dataShards  *shardsData

	vuidController *vuidControl

	putErrors = []errcode.Error{
		errcode.ErrDiskBroken, errcode.ErrReadonlyVUID,
		errcode.ErrChunkNoSpace,
		errcode.ErrNoSuchDisk, errcode.ErrNoSuchVuid,
	}
	getErrors = []errcode.Error{
		errcode.ErrOverload,
		errcode.ErrDiskBroken, errcode.ErrReadonlyVUID,
		errcode.ErrNoSuchDisk, errcode.ErrNoSuchVuid,
	}
)

type shardKey struct {
	Vuid proto.Vuid
	Bid  proto.BlobID
}

type shardsData struct {
	mutex sync.RWMutex
	data  map[shardKey][]byte
}

func (d *shardsData) clean() {
	d.mutex.Lock()
	for key := range d.data {
		d.data[key] = d.data[key][:0]
	}
	d.mutex.Unlock()
}

func (d *shardsData) get(vuid proto.Vuid, bid proto.BlobID) []byte {
	key := shardKey{Vuid: vuid, Bid: bid}
	d.mutex.RLock()
	data := d.data[key]
	buff := make([]byte, len(data))
	copy(buff, data)
	d.mutex.RUnlock()
	return buff
}

func (d *shardsData) set(vuid proto.Vuid, bid proto.BlobID, b []byte) {
	key := shardKey{Vuid: vuid, Bid: bid}
	d.mutex.Lock()
	old := d.data[key]
	if cap(old) <= len(b) {
		d.data[key] = make([]byte, len(b))
	} else {
		d.data[key] = old[:len(b)]
	}
	copy(d.data[key], b)
	d.mutex.Unlock()
}

type vuidControl struct {
	mutex    sync.Mutex
	broken   map[proto.Vuid]bool
	blocked  map[proto.Vuid]bool
	block    func()
	duration time.Duration

	isBNRealError bool // is return blobnode real error
}

func (c *vuidControl) Break(id proto.Vuid) {
	c.mutex.Lock()
	c.broken[id] = true
	c.mutex.Unlock()
}

func (c *vuidControl) Unbreak(id proto.Vuid) {
	c.mutex.Lock()
	delete(c.broken, id)
	c.mutex.Unlock()
}

func (c *vuidControl) Isbroken(id proto.Vuid) bool {
	c.mutex.Lock()
	v, ok := c.broken[id]
	c.mutex.Unlock()
	return ok && v
}

func (c *vuidControl) Block(id proto.Vuid) {
	c.mutex.Lock()
	c.blocked[id] = true
	c.mutex.Unlock()
}

func (c *vuidControl) Unblock(id proto.Vuid) {
	c.mutex.Lock()
	delete(c.blocked, id)
	c.mutex.Unlock()
}

func (c *vuidControl) Isblocked(id proto.Vuid) bool {
	c.mutex.Lock()
	v, ok := c.blocked[id]
	c.mutex.Unlock()
	return ok && v
}

func (c *vuidControl) SetBNRealError(b bool) {
	c.mutex.Lock()
	c.isBNRealError = b
	c.mutex.Unlock()
}

func (c *vuidControl) IsBNRealError() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.isBNRealError
}

func randBlobnodeRealError(errors []errcode.Error) error {
	n := rand.Intn(1024) % len(errors)
	return errors[n]
}

var storageAPIRangeGetShard = func(ctx context.Context, host string, args *blobnode.RangeGetShardArgs) (
	body io.ReadCloser, shardCrc uint32, err error) {
	if vuidController.Isbroken(args.Vuid) {
		err = errors.New("get shard fake error")
		if vuidController.IsBNRealError() {
			err = randBlobnodeRealError(getErrors)
		}
		return
	}
	if vuidController.Isblocked(args.Vuid) {
		vuidController.block()
		if rand.Intn(2) == 0 {
			err = errors.New("get shard timeout")
		} else {
			err = errors.New("get shard Timeout")
		}
		return
	}

	buff := dataShards.get(args.Vuid, args.Bid)
	if len(buff) == 0 {
		return nil, 0, errNotFound
	}
	if len(buff) < int(args.Offset+args.Size) {
		err = errors.New("get shard concurrently")
		return
	}

	buff = buff[int(args.Offset):int(args.Offset+args.Size)]
	shardCrc = crc32.ChecksumIEEE(buff)
	body = ioutil.NopCloser(bytes.NewReader(buff))
	return
}

var storageAPIPutShard = func(ctx context.Context, host string, args *blobnode.PutShardArgs) (
	crc uint32, err error) {
	if vuidController.Isbroken(args.Vuid) {
		err = errors.New("put shard fake error")
		if vuidController.IsBNRealError() {
			err = randBlobnodeRealError(putErrors)
		}
		return
	}
	if vuidController.Isblocked(args.Vuid) {
		vuidController.block()
		err = errors.New("put shard timeout")
		return
	}

	buffer, _ := memPool.Alloc(int(args.Size))
	defer memPool.Put(buffer)

	buffer = buffer[:int(args.Size)]
	_, err = io.ReadFull(args.Body, buffer)
	if err != nil {
		return
	}

	crc = crc32.ChecksumIEEE(buffer)
	dataShards.set(args.Vuid, args.Bid, buffer)
	return
}

func initMockData() {
	dataAllocs = make([]proxy.AllocRet, 2)
	dataAllocs[0] = proxy.AllocRet{
		BidStart: 10000,
		BidEnd:   10000,
		Vid:      volumeID,
	}
	dataAllocs[1] = proxy.AllocRet{
		BidStart: 20000,
		BidEnd:   50000,
		Vid:      volumeID,
	}

	dataVolume = &proxy.VersionVolume{VolumeInfo: clustermgr.VolumeInfo{
		VolumeInfoBase: clustermgr.VolumeInfoBase{
			Vid:      volumeID,
			CodeMode: codemode.EC6P6,
		},
		Units: func() (units []clustermgr.Unit) {
			for _, id := range allID {
				units = append(units, clustermgr.Unit{
					Vuid:   proto.Vuid(id),
					DiskID: proto.DiskID(id),
					Host:   strconv.Itoa(id),
				})
			}
			return
		}(),
	}}

	proxyNodes := make([]clustermgr.ServiceNode, 32)
	for idx := range proxyNodes {
		proxyNodes[idx] = clustermgr.ServiceNode{
			ClusterID: 1,
			Name:      serviceProxy,
			Host:      fmt.Sprintf("proxy-%d", idx),
			Idc:       idc,
		}
	}

	dataNodes = make(map[string]clustermgr.ServiceInfo)
	dataNodes[serviceProxy] = clustermgr.ServiceInfo{
		Nodes: proxyNodes,
	}

	dataDisks = make(map[proto.DiskID]blobnode.DiskInfo)
	for _, id := range idcID {
		dataDisks[proto.DiskID(id)] = blobnode.DiskInfo{
			ClusterID: clusterID, Idc: idc, Host: strconv.Itoa(id),
			DiskHeartBeatInfo: blobnode.DiskHeartBeatInfo{DiskID: proto.DiskID(id)},
		}
	}
	for _, id := range idcOtherID {
		dataDisks[proto.DiskID(id)] = blobnode.DiskInfo{
			ClusterID: clusterID, Idc: idcOther, Host: strconv.Itoa(id),
			DiskHeartBeatInfo: blobnode.DiskHeartBeatInfo{DiskID: proto.DiskID(id)},
		}
	}

	dataShards = &shardsData{
		data: make(map[shardKey][]byte, len(allID)),
	}
	dataShards.clean()

	ctr := gomock.NewController(&testing.T{})
	cli := mocks.NewMockClientAPI(ctr)
	cli.EXPECT().GetService(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, args clustermgr.GetServiceArgs) (clustermgr.ServiceInfo, error) {
			if val, ok := dataNodes[args.Name]; ok {
				return val, nil
			}
			return clustermgr.ServiceInfo{}, errNotFound
		})
	cmcli = cli

	clusterInfo = &clustermgr.ClusterInfo{
		Region:    "test-region",
		ClusterID: clusterID,
		Nodes:     []string{"node-1", "node-2", "node-3"},
	}

	ctr = gomock.NewController(&testing.T{})
	proxycli := mocks.NewMockProxyClient(ctr)
	proxycli.EXPECT().GetCacheVolume(gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().Return(dataVolume, nil)
	proxycli.EXPECT().GetCacheDisk(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, _ string, args *proxy.CacheDiskArgs) (*blobnode.DiskInfo, error) {
			if val, ok := dataDisks[args.DiskID]; ok {
				return &val, nil
			}
			return nil, errNotFound
		})

	serviceController, _ = controller.NewServiceController(
		controller.ServiceConfig{
			ClusterID: clusterID,
			IDC:       idc,
			ReloadSec: 1000,
		}, cmcli, proxycli, nil)
	volumeGetter, _ = controller.NewVolumeGetter(clusterID, serviceController, proxycli, 0)

	ctr = gomock.NewController(&testing.T{})
	c := NewMockClusterController(ctr)
	c.EXPECT().Region().AnyTimes().Return("test-region")
	c.EXPECT().ChooseOne().AnyTimes().Return(clusterInfo, nil)
	c.EXPECT().GetServiceController(gomock.Any()).AnyTimes().Return(serviceController, nil)
	c.EXPECT().GetVolumeGetter(gomock.Any()).AnyTimes().Return(volumeGetter, nil)
	c.EXPECT().ChangeChooseAlg(gomock.Any()).AnyTimes().DoAndReturn(
		func(alg controller.AlgChoose) error {
			if alg < 10 {
				return nil
			}
			return controller.ErrInvalidChooseAlg
		})
	cc = c

	ctr = gomock.NewController(&testing.T{})
	allocCli := mocks.NewMockProxyClient(ctr)
	allocCli.EXPECT().SendDeleteMsg(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	allocCli.EXPECT().SendShardRepairMsg(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	allocCli.EXPECT().VolumeAlloc(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, host string, args *proxy.AllocVolsArgs) ([]proxy.AllocRet, error) {
			if args.Fsize > allocTimeoutSize {
				return nil, errAllocTimeout
			}
			return dataAllocs, nil
		})
	proxyClient = allocCli
}

func initPool() {
	memPool = resourcepool.NewMemPool(getDefaultMempoolSize())
}

func initEncoder() {
	coderEC6P6, _ := ec.NewEncoder(ec.Config{
		CodeMode:     codemode.EC6P6.Tactic(),
		EnableVerify: true,
	})
	coderEC6P10L2, _ := ec.NewEncoder(ec.Config{
		CodeMode:     codemode.EC6P10L2.Tactic(),
		EnableVerify: true,
	})
	coderEC15P12, _ := ec.NewEncoder(ec.Config{
		CodeMode:     codemode.EC15P12.Tactic(),
		EnableVerify: true,
	})
	coderEC16P20L2, _ := ec.NewEncoder(ec.Config{
		CodeMode:     codemode.EC16P20L2.Tactic(),
		EnableVerify: true,
	})
	encoder = map[codemode.CodeMode]ec.Encoder{
		codemode.EC6P6:     coderEC6P6,
		codemode.EC6P10L2:  coderEC6P10L2,
		codemode.EC15P12:   coderEC15P12,
		codemode.EC16P20L2: coderEC16P20L2,
	}
}

func initEC() {
	allCodeModes = CodeModePairs{
		codemode.EC6P6: CodeModePair{
			Policy: codemode.Policy{
				ModeName: codemode.EC6P6.Name(),
				MaxSize:  math.MaxInt64,
				Enable:   true,
			},
			Tactic: codemode.EC6P6.Tactic(),
		},
		codemode.EC6P10L2: CodeModePair{
			Policy: codemode.Policy{
				ModeName: codemode.EC6P10L2.Name(),
				MaxSize:  -1,
			},
			Tactic: codemode.EC6P10L2.Tactic(),
		},
		codemode.EC15P12: CodeModePair{
			Policy: codemode.Policy{
				ModeName: codemode.EC15P12.Name(),
				MaxSize:  -1,
			},
			Tactic: codemode.EC15P12.Tactic(),
		},
		codemode.EC16P20L2: CodeModePair{
			Policy: codemode.Policy{
				ModeName: codemode.EC16P20L2.Name(),
				MaxSize:  -1,
			},
			Tactic: codemode.EC16P20L2.Tactic(),
		},
	}
}

func initController() {
	vuidController = &vuidControl{
		broken:  make(map[proto.Vuid]bool),
		blocked: make(map[proto.Vuid]bool),
		block: func() {
			time.Sleep(200 * time.Millisecond)
		},
		duration:      200 * time.Millisecond,
		isBNRealError: false,
	}
	// initialized broken 1005
	vuidController.Break(1005)
}

func newMockStorageAPI() blobnode.StorageAPI {
	ctr := gomock.NewController(&testing.T{})
	api := mocks.NewMockStorageAPI(ctr)
	api.EXPECT().RangeGetShard(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(storageAPIRangeGetShard)
	api.EXPECT().PutShard(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(storageAPIPutShard)
	return api
}

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))

	hystrix.ConfigureCommand(rwCommand, hystrix.CommandConfig{
		Timeout:               9000,
		MaxConcurrentRequests: 9000,
		ErrorPercentThreshold: 90,
	})

	initPool()
	initEncoder()
	initEC()
	initMockData()
	initController()

	streamer = &Handler{
		memPool:           memPool,
		encoder:           encoder,
		clusterController: cc,

		blobnodeClient: newMockStorageAPI(),
		proxyClient:    proxyClient,

		allCodeModes:  allCodeModes,
		maxObjectSize: defaultMaxObjectSize,
		StreamConfig: StreamConfig{
			IDC:                    idc,
			MaxBlobSize:            uint32(blobSize), // 4M
			DiskPunishIntervalS:    punishServiceS,
			ServicePunishIntervalS: punishServiceS,
			AllocRetryTimes:        3,
			AllocRetryIntervalMS:   3000,
			MinReadShardsX:         minReadShardsX,
		},
		discardVidChan: make(chan discardVid, 8),
		stopCh:         make(chan struct{}),
	}
	streamer.loopDiscardVids()
}

func ctxWithName(funcName string) func() context.Context {
	return func() context.Context {
		_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), funcName, funcName)
		return ctx
	}
}

func getBufSizes(size int) ec.BufferSizes {
	sizes, _ := ec.GetBufferSizes(size, codemode.EC6P6.Tactic())
	return sizes
}

func dataEqual(exp, act []byte) bool {
	return crc32.ChecksumIEEE(exp) == crc32.ChecksumIEEE(act)
}
