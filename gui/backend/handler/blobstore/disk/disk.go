package disk

import (
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"

	"github.com/cubefs/cubefs/console/backend/handler/blobstore/common"
	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/enums"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/blobnode"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr/disk"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/scheduler"
)

type ListOutput struct {
	Disks  []common.DiskInfo `json:"disks"`
	Marker uint32            `json:"marker"`
	Total  int               `json:"total"`
}

func List(c *gin.Context) {
	input := &common.DiskListInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	var disks *clustermgr.ListDiskRet
	if input.Status == enums.DiskStatusDropping {
		disks, err = GetDroppingList(c, consulAddr, input)
	} else {
		input.IsAll = true
		disks, err = common.GetDiskList(c, consulAddr, input)
	}
	if err != nil {
		log.Errorf("get disks failed. input:%+v,consulAddr:%s,err:%+v", input, consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	output := ListOutput{
		Disks:  make([]common.DiskInfo, 0),
		Marker: uint32(disks.Marker),
		Total:  len(disks.Disks),
	}
	err = copier.Copy(&output.Disks, disks.Disks)
	if err != nil {
		log.Errorf("copy disks failed.err:%+v", err)
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return
	}
	output.Disks = common.HandlerRepair(c, consulAddr, output.Disks)
	_, diskLoad, err := common.GetWritingVolumes(c, consulAddr, &common.WritingVolumeInput{IsAll: true})
	if err != nil {
		log.Errorf("common.GetWritingVolumes failed.consulAddr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	output.Disks = common.HandleDiskLoad(output.Disks, diskLoad)
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

func GetDroppingList(c *gin.Context, consulAddr string, input *common.DiskListInput) (*clustermgr.ListDiskRet, error) {
	disks, err := disk.DroppingList(c, consulAddr)
	if err != nil {
		log.Errorf("disk.DroppingList failed.consulAddr:%s,err:%+v", consulAddr, err)
		return nil, err
	}
	if input.DiskId != 0 {
		disks.Disks = common.FilterDiskId(disks.Disks, input.DiskId)
	}
	if input.Idc != "" {
		disks.Disks = common.FilterIdc(disks.Disks, input.Idc)
	}
	if input.Readonly != 0 {
		disks.Disks = common.FilterReadonly(disks.Disks, input.Readonly)
	}
	if input.Page != 0 && input.Count != 0 {
		disks.Disks = common.PagingMachine(disks.Disks, input.Page, input.Count)
	}
	return disks, nil
}

type InfoInput struct {
	DiskId uint32 `form:"disk_id" binding:"required"`
}

func Info(c *gin.Context) {
	input := &InfoInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	output, err := disk.Info(c, consulAddr, input.DiskId)
	if err != nil {
		log.Errorf("disk.Info failed.disk_id:%d,consulAddr:%s,err:%+v", input.DiskId, consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

func DroppingList(c *gin.Context) {
	consulAddr, err := ginutils.GetConsulAddr(c)
	if err != nil {
		return
	}
	output, err := disk.DroppingList(c, consulAddr)
	if err != nil {
		log.Errorf("disk.DroppingList failed.consulAddr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

type AccessInput struct {
	DiskId   uint32 `json:"disk_id" binding:"required"`
	ReadOnly bool   `json:"readonly"`
}

func Access(c *gin.Context) {
	input := &AccessInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	diskInfo, err := disk.Info(c, consulAddr, input.DiskId)
	if err != nil {
		log.Errorf("disk.Info failed.consulAddr:%s,disk_id:%d,err:%+v", consulAddr, input.DiskId, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	if diskInfo.Status != proto.DiskStatusNormal {
		log.Errorf("disk status is not normal,disk_id:%d,status:%+v", input.DiskId, diskInfo.Status)
		ginutils.Send(c, codes.InvalidArgs.Code(), "invalid disk status", nil)
		return
	}
	in := &disk.AccessInput{DiskId: input.DiskId, Readonly: input.ReadOnly}
	if err = disk.Access(c, consulAddr, in); err != nil {
		log.Errorf("disk.Access failed.consulAddr:%s,args:%+v,err:%+v", consulAddr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

type SetBrokenInput struct {
	DiskId uint32 `json:"disk_id" binding:"required"`
}

func SetBroken(c *gin.Context) {
	input := &SetBrokenInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	in := &disk.SetInput{DiskId: input.DiskId, Status: proto.DiskStatusBroken}
	if err = disk.Set(c, consulAddr, in); err != nil {
		log.Errorf("disk.Set failed.consulAddr:%s,args:%+v,err:%+v", consulAddr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

type DropInput struct {
	DiskId uint32 `json:"disk_id" binding:"required"`
}

func Drop(c *gin.Context) {
	input := &DropInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	if err = disk.Drop(c, consulAddr, input.DiskId); err != nil {
		log.Errorf("disk.Drop failed.consulAddr:%s,args:%+v,err:%+v", consulAddr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

type ProbeInput struct {
	DiskId uint32 `json:"disk_id" binding:"required"`
	Path   string `json:"path" binding:"required"`
	Host   string `json:"host" binding:"required"`
}

func Probe(c *gin.Context) {
	input := &ProbeInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	diskInfo, err := disk.Info(c, consulAddr, input.DiskId)
	if err != nil {
		log.Errorf("disk.Info failed.consulAddr:%s,disk_id:%d,err:%+v", consulAddr, input.DiskId, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	if diskInfo.Status != proto.DiskStatusRepaired && diskInfo.Status != proto.DiskStatusDropped {
		log.Errorf("disk status is not normal,disk_id:%d,status:%+v", input.DiskId, diskInfo.Status)
		ginutils.Send(c, codes.InvalidArgs.Code(), "invalid disk status", nil)
		return
	}
	if err = blobnode.DiskProbe(c, input.Host, input.Path); err != nil {
		log.Errorf("blobnode.DiskProb failed.consulAddr:%s,args:%+v,err:%+v", consulAddr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

type MigratingStatInput struct {
	DiskId   uint32         `form:"disk_id"`
	TaskType proto.TaskType `json:"task_type"`
}

func StatMigrating(c *gin.Context) {
	input := &MigratingStatInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	stats, err := scheduler.DiskMigratingStats(c, consulAddr, &scheduler.DiskMigratingStatsInput{DiskId: input.DiskId, TaskType: string(input.TaskType)})
	if err != nil {
		log.Errorf("scheduler.DiskMigratingStats failed. args:%+v, err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), stats)
}
