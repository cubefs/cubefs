package metanode

import (
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/proto"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"

	"github.com/cubefs/cubefs/console/backend/helper"
	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/enums"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/helper/node"
	"github.com/cubefs/cubefs/console/backend/helper/pool"
	"github.com/cubefs/cubefs/console/backend/service/cluster"
	"github.com/cubefs/cubefs/console/backend/service/metanode"
)

type AddInput struct {
	Id       string `json:"id" binding:"required"`
	ZoneName string `json:"zone_name" binding:"required"`
	Addr     string `json:"addr" binding:"required"`
}

func Add(c *gin.Context) {
	input := &AddInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	in := &metanode.AddInput{}
	if err = copier.Copy(in, input); err != nil {
		log.Errorf("copy AddInput failed.args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return
	}
	out, err := metanode.Add(c, addr, in)
	if err != nil {
		log.Errorf("datanode.Add failed.addr:%s,args:%+v,err:%+v", addr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), out)
}

type ListInput struct {
	Node      string `form:"node"`
	NodeSetId uint64 `form:"node_set_id"`
	Sort      string `form:"sort"` // asc, desc
}

type ListOutput struct {
	ID                 uint64    `json:"id"`
	Addr               string    `json:"addr"`
	Status             string    `json:"status"`
	ZoneName           string    `json:"zone_name"`
	Total              string    `json:"total"`
	Used               string    `json:"used"`
	Available          string    `json:"available"`
	UsageRatio         string    `json:"usage_ratio"`
	ReportTime         time.Time `json:"report_time"`
	MetaPartitionCount int       `json:"partition_count"`
	Writable           bool      `json:"writable"`
	WritableStr        string    `json:"writable_str,omitempty"`
	RdOnly             bool      `json:"rd_only"`
	NodeSetId          uint64    `json:"node_set_id"`
}

func List(c *gin.Context) {
	input := &ListInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	nodes, err := cluster.GetNodes(c, addr, input.Node, true)
	if err != nil {
		log.Errorf("cluster.GetNodes failed.addr:%s,args:%+v,err:%+v", addr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	poolSize := pool.GetPoolSize(30, len(nodes))
	nodeInfoChan := make(chan *proto.MetaNodeInfo, poolSize)
	output := make([]ListOutput, 0)
	go getNodesInfo(c, addr, nodes, poolSize, nodeInfoChan)
	for n := range nodeInfoChan {
		if input.NodeSetId != 0 && n.NodeSetID != input.NodeSetId {
			continue
		}
		item := ListOutput{
			ID:                 n.ID,
			Addr:               n.Addr,
			Status:             node.FormatNodeStatus(n.IsActive),
			ZoneName:           n.ZoneName,
			Total:              helper.ByteConversion(n.Total),
			Used:               helper.ByteConversion(n.Used),
			Available:          helper.ByteConversion(node.FormatAvailable(n.MaxMemAvailWeight, n.Used, n.Total)),
			UsageRatio:         helper.Percentage(n.Used, n.Total),
			ReportTime:         n.ReportTime,
			MetaPartitionCount: n.MetaPartitionCount,
			Writable:           n.IsWriteAble,
			WritableStr:        node.FormatWritableStr(n.IsWriteAble),
			RdOnly:             n.RdOnly,
			NodeSetId:          n.NodeSetID,
		}
		output = append(output, item)
	}
	if len(output) > 0 {
		sort.Slice(output, func(i, j int) bool {
			if input.Sort == enums.DESC {
				return output[i].NodeSetId >= output[j].NodeSetId
			}
			return output[i].NodeSetId < output[j].NodeSetId
		})
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

func getNodesInfo(c *gin.Context, clusterAddr string, nodes []string, poolSize int, result chan *proto.MetaNodeInfo) {
	start := time.Now()
	var wg sync.WaitGroup
	tp := pool.New(poolSize, poolSize)
	for i := range nodes {
		n := nodes[i]
		wg.Add(1)
		tp.Run(func() {
			defer wg.Done()
			nodeInfo, err := metanode.Get(c, clusterAddr, n)
			if err != nil {
				log.Errorf("metanode.Get failed.addr:%s,node:%s,err:%+v", clusterAddr, n, err)
				return
			}
			result <- nodeInfo
		})
	}
	wg.Wait()
	close(result)
	tp.Close()
	log.Infof("get meta nodes cost time:%+v", time.Since(start))
}

type PartitionInput struct {
	NodeAddr string `form:"addr" binding:"required"`
	Id       uint64 `form:"id"`
}

type PartitionOutput struct {
	PartitionId uint64      `json:"partition_id"`
	VolName     string      `json:"vol_name"`
	Start       uint64      `json:"start"`
	End         string      `json:"end"`
	Peers       interface{} `json:"peers"`
}

func Partitions(c *gin.Context) {
	input := &PartitionInput{}
	if !ginutils.Check(c, input) {
		return
	}
	addr := helper.GetIp(input.NodeAddr)
	partitions, err := metanode.GetPartitions(c, addr)
	if err != nil {
		log.Errorf("datanode.GetPartitions failed. args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	output := make([]PartitionOutput, 0)
	if partitions == nil {
		ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
		return
	}
	for key, p := range *partitions {
		item := PartitionOutput{
			PartitionId: p.PartitionId,
			VolName:     p.VolName,
			Start:       p.Start,
			End:         node.FormatUint64(p.End),
			Peers:       p.Peers,
		}
		if input.Id == 0 || strconv.FormatUint(input.Id, 10) == key {
			output = append(output, item)
		}
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

type DecommissionInput struct {
	Addrs []string `json:"addrs" binding:"required,gte=1"`
}

func Decommission(c *gin.Context) {

	input := &DecommissionInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	var output interface{}
	for i := range input.Addrs {
		output, err = metanode.Decommission(c, addr, input.Addrs[i])
		if err != nil {
			log.Errorf("metanode.Decommission failed.cluster_name:%s,cluster_addr:%s,node:%s,err:%+v", c.Param(ginutils.Cluster), addr, input.Addrs[i], err)
			ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
			return
		}
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

type MigrateInput struct {
	SrcAddr    string `json:"src_addr" binding:"required"`
	TargetAddr string `json:"target_addr" binding:"required"`
}

func Migrate(c *gin.Context) {

	input := &MigrateInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	err = metanode.Migrate(c, addr, input.SrcAddr, input.TargetAddr)
	if err != nil {
		log.Errorf("metanode.Migrate failed. args:%+v,addr:%s,err:%+v", input, addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}
