package datanode

import (
	"sort"
	"strings"
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
	"github.com/cubefs/cubefs/console/backend/service/datanode"
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
	in := &datanode.AddInput{}
	if err = copier.Copy(in, input); err != nil {
		log.Errorf("copy AddInput failed.args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return
	}
	out, err := datanode.Add(c, addr, in)
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
	DataPartitionCount uint32    `json:"partition_count"`
	Writable           bool      `json:"writable"`
	WritableStr        string    `json:"writable_str,omitempty"`
	BadDisks           []string  `json:"bad_disks,omitempty"`
	RdOnly             bool      `json:"rd_only"`
	NodeSetId          uint64    `json:"node_set_id"`
}

func List(c *gin.Context) {
	input := &ListInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	nodes, err := cluster.GetNodes(c, addr, input.Node, false)
	if err != nil {
		log.Errorf("cluster.GetNodes failed.addr:%s,args:%+v,err:%+v", addr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	poolSize := pool.GetPoolSize(30, len(nodes))
	tp := pool.New(poolSize, poolSize)
	nodeInfoChan := make(chan *proto.DataNodeInfo, poolSize)
	output := make([]ListOutput, 0)
	go getNodesInfo(c, addr, nodes, tp, nodeInfoChan)
	for n := range nodeInfoChan {
		if input.NodeSetId != 0 && input.NodeSetId != n.NodeSetID {
			continue
		}
		item := ListOutput{
			ID:                 n.ID,
			Addr:               n.Addr,
			Status:             node.FormatNodeStatus(n.IsActive, n.BadDisks...),
			ZoneName:           n.ZoneName,
			Total:              helper.ByteConversion(n.Total),
			Used:               helper.ByteConversion(n.Used),
			Available:          helper.ByteConversion(node.FormatAvailable(n.AvailableSpace, n.Used, n.Total)),
			UsageRatio:         helper.Percentage(n.Used, n.Total),
			ReportTime:         n.ReportTime,
			DataPartitionCount: n.DataPartitionCount,
			Writable:           n.IsWriteAble,
			WritableStr:        node.FormatWritableStr(n.IsWriteAble),
			BadDisks:           n.BadDisks,
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

func getNodesInfo(c *gin.Context, clusterAddr string, nodes []string, tp pool.TaskPool, result chan *proto.DataNodeInfo) {
	start := time.Now()
	var wg sync.WaitGroup
	for i := range nodes {
		n := nodes[i]
		wg.Add(1)
		tp.Run(func() {
			defer wg.Done()
			nodeInfo, err := datanode.Get(c, clusterAddr, n)
			if err != nil {
				log.Errorf("datanode.Get failed.addr:%s,node:%s,err:%+v", clusterAddr, n, err)
				return
			}
			result <- nodeInfo
		})
	}
	wg.Wait()
	close(result)
	tp.Close()
	log.Infof("get data nodes cost time:%+v", time.Since(start))
}

type PartitionInput struct {
	NodeAddr string `form:"addr" binding:"required"`
	Id       uint64 `form:"id"`
	DiskPath string `form:"disk_path"`
}

type PartitionOutput struct {
	ID       uint64   `json:"id"`
	Size     string   `json:"size"`
	Used     string   `json:"used"`
	Status   string   `json:"status"`
	Path     string   `json:"path"`
	Replicas []string `json:"replicas"`
}

func Partitions(c *gin.Context) {
	input := &PartitionInput{}
	if !ginutils.Check(c, input) {
		return
	}
	addr := helper.GetIp(input.NodeAddr)
	partitions, err := datanode.GetPartitions(c, addr)
	if err != nil {
		log.Errorf("datanode.GetPartitions failed. args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	output := make([]PartitionOutput, 0)
	for _, p := range partitions {
		item := PartitionOutput{
			ID:       p.ID,
			Size:     helper.ByteConversion(p.Size),
			Used:     helper.ByteConversion(p.Used),
			Status:   node.FormatDiskAndPartitionStatus(p.Status),
			Path:     p.Path,
			Replicas: p.Replicas,
		}
		if isAppendPartition(input, item) {
			output = append(output, item)
		}
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

func isAppendPartition(input *PartitionInput, output PartitionOutput) bool {
	isAppend := input.Id == 0 && input.DiskPath == ""
	if input.Id != 0 && input.Id == output.ID {
		isAppend = true
	}
	if input.DiskPath != "" {
		if output.Path == input.DiskPath {
			isAppend = true
		} else if input.DiskPath[len(input.DiskPath)-1] != '/' {
			isAppend = strings.Contains(output.Path, input.DiskPath+"/")
		} else {
			isAppend = strings.Contains(output.Path, input.DiskPath)
		}
	}
	return isAppend
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
		output, err = datanode.Decommission(c, addr, input.Addrs[i])
		if err != nil {
			log.Errorf("datanode.Decommission failed.cluster_name:%s,cluster_addr:%s,node:%s,err:%+v", c.Param(ginutils.Cluster), addr, input.Addrs[i], err)
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
	err = datanode.Migrate(c, addr, input.SrcAddr, input.TargetAddr)
	if err != nil {
		log.Errorf("datanode.Migrate failed. args:%+v,addr:%s,err:%+v", input, addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}
