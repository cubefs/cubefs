package disk

import (
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper"
	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/helper/node"
	"github.com/cubefs/cubefs/console/backend/service/datanode"
	"github.com/cubefs/cubefs/console/backend/service/disk"
)

type ListInput struct {
	DataNodeAddr string `form:"addr" binding:"required"`
}

type ListOutput struct {
	Path       string `json:"path"`
	Total      string `json:"total"`
	Used       string `json:"used"`
	Allocated  string `json:"allocated"`
	Status     string `json:"status"`
	Partitions int    `json:"partitions"`
	UsageRatio string `json:"usage_ratio"`
}

func List(c *gin.Context) {
	input := &ListInput{}
	if !ginutils.Check(c, input) {
		return
	}
	addr := helper.GetIp(input.DataNodeAddr)
	diskData, err := datanode.GetDisks(c, addr)
	if err != nil {
		log.Errorf("datanode.GetDisks failed.args:%+v,addr:%s,err:%+v", input, addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	output := make([]ListOutput, 0)
	for _, d := range diskData.Disks {
		item := ListOutput{
			Path:       d.Path,
			Total:      helper.ByteConversion(d.Total),
			Used:       helper.ByteConversion(d.Used),
			Allocated:  helper.ByteConversion(d.Allocated),
			Status:     node.FormatDiskAndPartitionStatus(d.Status),
			Partitions: d.Partitions,
			UsageRatio: helper.Percentage(d.Used, d.Total),
		}
		output = append(output, item)
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

type DecommissionInput struct {
	DataNodeAddr string   `json:"addr" binding:"required"`
	Disks        []string `json:"disks" binding:"required,gte=1"`
}

func Decommission(c *gin.Context) {
	input := &DecommissionInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	for i := range input.Disks {
		item := input.Disks[i]
		err = disk.Decommission(c, addr, input.DataNodeAddr, item)
		if err != nil {
			log.Errorf("disk.Decommission failed.args:%+v,clusterAddr:%s,disk:%+v,err:%+v", input, addr, item, err)
			ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
			return
		}
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}
