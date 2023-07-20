package node

import (
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"

	"github.com/cubefs/cubefs/console/backend/handler/blobstore/common"
	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/enums"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/helper/pool"
	"github.com/cubefs/cubefs/console/backend/model"
	bn "github.com/cubefs/cubefs/console/backend/service/blobstore/blobnode"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr/disk"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr/service"
)

type ListInput struct {
	Idc        string           `json:"idc" form:"idc"`
	Rack       string           `json:"rack" form:"rack"`
	Host       string           `json:"host" form:"host"`
	Status     uint8            `json:"status" form:"status"`
	Marker     int              `json:"marker" form:"marker"`
	Count      int              `json:"count" form:"count"`
	Page       int              `json:"page" form:"page"`
	GetHistory bool             `json:"get_history" form:"get_history"`
	Readonly   enums.ReadOnlyOp `json:"readonly" form:"readonly"`
	NodeData   bool             `json:"node_data" form:"node_data"`
	DiskId     uint32           `json:"disk_id" form:"disk_id"`
}

type ListOutput struct {
	Total int             `json:"total"`
	Nodes map[string]Info `json:"nodes"`
}

type Info struct {
	Disks         []common.DiskInfo `json:"disks"`
	Host          string            `json:"host"`
	NormalDiskLen int               `json:"normal_disk_len"`
}

func List(c *gin.Context) {
	input := &ListInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	in := &common.DiskListInput{}
	if err = copier.Copy(in, input); err != nil {
		log.Errorf("copy common.DiskListInput failed.err:%+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	in.NodeData = true
	in.IsAll = true
	out, err := common.GetDiskList(c, consulAddr, in)
	if err != nil {
		log.Errorf("common.GetDiskList failed.args:%+v,consulAddr:%s,err:%+v", in, consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	if len(out.Disks) == 0 {
		log.Info("out.Disks data is nil")
		ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), gin.H{})
		return
	}
	disks := make([]common.DiskInfo, 0)
	_ = copier.Copy(&disks, out.Disks)
	disks = common.HandlerRepair(c, consulAddr, disks)
	_, diskMap, err := common.GetWritingVolumes(c, consulAddr, &common.WritingVolumeInput{IsAll: true})
	if err != nil {
		log.Errorf("common.GetWritingVolumes failed.err:%+v", err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	nodes := map[string]Info{}
	for _, v := range disks {
		if input.Idc != "" && v.Idc != input.Idc {
			continue
		}
		if input.Status != 0 && uint8(v.Status) != input.Status {
			continue
		}
		if input.Host != "" && v.Host != input.Host {
			continue
		}
		v.DiskLoad = diskMap[v.DiskID].DiskLoad
		v.VolumeIds = diskMap[v.DiskID].VolumeId

		info, ok := nodes[v.Host]
		if !ok {
			info = Info{Disks: []common.DiskInfo{v}}
		} else {
			info.Disks = append(info.Disks, v)
		}
		if v.Status == proto.DiskStatusNormal {
			info.NormalDiskLen++
		}
		nodes[v.Host] = info
	}
	if input.Page == 0 || input.Count == 0 {
		ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), ListOutput{Total: len(nodes), Nodes: nodes})
		return
	}
	output := ListOutput{Total: len(nodes), Nodes: map[string]Info{}}
	hosts := make([]string, 0)
	for k := range nodes {
		hosts = append(hosts, k)
	}
	sort.Strings(hosts)
	hosts = common.PagingMachineStr(hosts, input.Page, input.Count)
	for _, v := range hosts {
		if val, ok := nodes[v]; ok {
			output.Nodes[v] = val
		}
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

type AccessInput struct {
	Host     string `json:"host" binding:"required"`
	Readonly bool   `json:"readonly"`
}

func Access(c *gin.Context) {
	input := &AccessInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	in := &common.DiskListInput{Host: input.Host}
	in.NodeData = true
	in.IsAll = true
	out, err := common.GetDiskList(c, consulAddr, in)
	if err != nil {
		log.Errorf("common.GetDiskList failed.args:%+v,consulAddr:%s,err:%+v", in, consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	if len(out.Disks) == 0 {
		log.Info("out.Disks data is nil")
		ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), gin.H{})
		return
	}
	droppingMap, err := GetDroppingMap(c, consulAddr)
	if err != nil {
		log.Errorf("GetDroppingMap failed.err:%+v", err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	for _, v := range out.Disks {
		if v.Readonly == input.Readonly || v.Host != input.Host || v.Status >= proto.DiskStatusBroken {
			continue
		}
		if _, ok := droppingMap[v.DiskID]; ok {
			continue
		}
		err = disk.Access(c, consulAddr, &disk.AccessInput{DiskId: uint32(v.DiskID), Readonly: input.Readonly})
		if err != nil {
			log.Errorf("disk.Access failed.consulAddr:%s,disk_id:%d,err:%+v", consulAddr, uint32(v.DiskID), err)
			ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
			return
		}
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func GetDroppingMap(c *gin.Context, consulAddr string) (map[proto.DiskID]*blobnode.DiskInfo, error) {
	disks, err := disk.DroppingList(c, consulAddr)
	if err != nil {
		return nil, err
	}
	out := map[proto.DiskID]*blobnode.DiskInfo{}
	for _, v := range disks.Disks {
		out[v.DiskID] = v
	}
	return out, nil
}

type DropInput struct {
	Host string `json:"host" binding:"required"`
}

func Drop(c *gin.Context) {
	input := &DropInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	in := &common.DiskListInput{Host: input.Host}
	in.NodeData = true
	in.IsAll = true
	out, err := common.GetDiskList(c, consulAddr, in)
	if err != nil {
		log.Errorf("common.GetDiskList failed.args:%+v,consulAddr:%s,err:%+v", in, consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	if len(out.Disks) == 0 {
		log.Info("out.Disks data is nil")
		ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), gin.H{})
		return
	}
	for _, v := range out.Disks {
		err = disk.Drop(c, consulAddr, uint32(v.DiskID))
		if err != nil {
			log.Errorf("disk.Drop failed.consulAddr:%s,disk_id:%d,err:%+v", consulAddr, uint32(v.DiskID), err)
			ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
			return
		}
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

type OfflineInput struct {
	Host string `json:"host" binding:"required"`
}

func Offline(c *gin.Context) {
	input := &OfflineInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	nodes, err := service.List(c, consulAddr)
	if err != nil {
		log.Errorf("service.List failed.consulAddr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	exists := false
	for _, n := range nodes {
		if n.Host == input.Host {
			exists = true
			err = service.Unregister(c, consulAddr, &service.UnregisterInput{Name: n.Name, Host: n.Host})
			if err != nil {
				log.Errorf("service.Unregister failed.consulAddr:%s,host:%s,name:%s,err:%+v", consulAddr, n.Host, n.Name, err)
				ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
				return
			}
		}
	}
	if !exists {
		ginutils.Send(c, codes.NotFound.Code(), "node not found", nil)
		return
	}

	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

type ConfigReloadInput struct {
	Nodes   []string `json:"nodes" binding:"required"`
	Key     string   `json:"key" binding:"required"`
	Value   string   `json:"value" binding:"required"`
	cluster string
}

func ConfigReload(c *gin.Context) {
	input := &ConfigReloadInput{}
	if !ginutils.Check(c, input) {
		return
	}
	input.cluster = c.Param(ginutils.Cluster)
	poolSize := 30
	if len(input.Nodes) < poolSize {
		poolSize = len(input.Nodes)
	}
	tp := pool.New(poolSize, poolSize)
	errChan := make(chan configReloadErr, poolSize)
	go configReload(c, input, tp, errChan)
	err := map[string]string{}
	for e := range errChan {
		err[e.Node] = e.Err
	}
	if len(err) != 0 {
		log.Errorf("reload error. args:%+v, err:%+v", input, err)
		ginutils.Send(c, codes.OK.Code(), codes.ThirdPartyError.Msg(), err)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

type configReloadErr struct {
	Node string `json:"node"`
	Err  string `json:"err"`
}

func configReload(c *gin.Context, input *ConfigReloadInput, tp pool.TaskPool, errChan chan configReloadErr) {
	wg := sync.WaitGroup{}
	in := &bn.ConfigReloadInput{Key: input.Key, Value: input.Value}
	for i := range input.Nodes {
		node := input.Nodes[i]
		wg.Add(1)
		tp.Run(func() {
			defer wg.Done()
			if err := bn.ConfigReload(c, node, in); err != nil {
				recordConfigFailure(node, input, err)
				errChan <- configReloadErr{
					Node: node,
					Err:  err.Error(),
				}
			} else {
				recordConfigSuccess(node, input)
			}
		})
	}
	wg.Wait()
	close(errChan)
	tp.Close()
}

func recordConfigFailure(node string, input *ConfigReloadInput, err error) {
	failure := &model.NodeConfigFailure{
		Node:         node,
		Cluster:      input.cluster,
		Key:          input.Key,
		Value:        input.Value,
		FailedReason: err.Error(),
		CreatedAt:    time.Now(),
	}
	if err = failure.Insert(); err != nil {
		log.Errorf("record failure failed. err:%v, node[%s], cluster[%s]", err, node, input.cluster)
	}
}

func recordConfigSuccess(node string, input *ConfigReloadInput) {
	if err := new(model.NodeConfig).Upsert(node, input.cluster, input.Key, input.Value); err != nil {
		log.Errorf("record success failed. err:%v, node[%s], cluster[%s]", err, node, input.cluster)
	}
}

type ConfigInfoInput struct {
	Node string `form:"node" binding:"required"`
}

func ConfigInfo(c *gin.Context) {
	input := &ConfigInfoInput{}
	if !ginutils.Check(c, input) {
		return
	}
	info := &model.NodeConfig{Configuration: map[string]string{}}
	if err := info.One(input.Node, c.Param(ginutils.Cluster)); err != nil {
		log.Errorf("info.One failed.node:%s, err:%+v", input.Node, err)
		ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), info)
}

func ConfigFailures(c *gin.Context) {
	input := &ConfigInfoInput{}
	if !ginutils.Check(c, input) {
		return
	}
	failure := &model.NodeConfigFailure{}
	if err := failure.One(input.Node, c.Param(ginutils.Cluster)); err != nil {
		log.Errorf("failure.One failed.node:%s, err:%+v", input.Node, err)
		ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), failure)
}
