package common

import (
	"sort"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"

	"github.com/cubefs/cubefs/console/backend/helper"
	"github.com/cubefs/cubefs/console/backend/helper/enums"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr/disk"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/scheduler"
)

func HandleDiskLoad(disks []DiskInfo, loadMap map[proto.DiskID]DiskVolumeInfo) []DiskInfo {
	for k, v := range disks {
		if val, ok := loadMap[v.DiskID]; ok {
			disks[k].DiskLoad = val.DiskLoad
			disks[k].VolumeIds = val.VolumeId
		}
	}
	return disks
}

func HandlerRepair(c *gin.Context, consulAddr string, disks []DiskInfo) []DiskInfo {
	for k, v := range disks {
		if v.Status == proto.DiskStatusRepaired {
			disks[k].RepairSchedule = 1
			continue
		}
		if v.Status == proto.DiskStatusRepairing {
			stats, err := scheduler.DiskMigratingStats(c, consulAddr, &scheduler.DiskMigratingStatsInput{DiskId: uint32(v.DiskID), TaskType: string(proto.TaskTypeDiskRepair)})
			if err != nil {
				log.Errorf("scheduler.DiskMigratingStatus error.disk_id:%d,disk_type:%s", uint32(v.DiskID), string(proto.TaskTypeDiskRepair))
				continue
			}
			if stats.TotalTasksCnt != 0 {
				disks[k].RepairSchedule = float64(stats.MigratedTasksCnt) / float64(stats.TotalTasksCnt)
			}
		}
	}
	return disks
}

type RepairScheduler struct {
	DiskId             uint32
	DiskRepairSchedule float64
}

func GetRepairScheduler(c *gin.Context, consulAddr string) (*RepairScheduler, error) {
	stats, err := scheduler.LeaderStats(c, consulAddr)
	if err != nil {
		log.Errorf("scheduler.LeaderStats failed.consulAddr:%s,err:%+v", consulAddr, err)
		return nil, err
	}
	if stats.DiskDrop == nil && stats.DiskRepair == nil {
		return nil, nil
	}
	if stats.DiskRepair != nil && stats.DiskRepair.TotalTasksCnt != 0 && stats.DiskRepair.RepairedTasksCnt != 0 {
		schedule := float64(stats.DiskRepair.RepairedTasksCnt) / float64(stats.DiskRepair.TotalTasksCnt)
		if schedule != 0 {
			return &RepairScheduler{DiskRepairSchedule: helper.FloatRound(schedule, 2)}, nil
		}
	}
	if stats.DiskDrop != nil && stats.DiskDrop.TotalTasksCnt != 0 && stats.DiskDrop.DroppedTasksCnt != 0 {
		schedule := float64(stats.DiskDrop.DroppedTasksCnt) / float64(stats.DiskDrop.TotalTasksCnt)
		if schedule != 0 {
			return &RepairScheduler{DiskRepairSchedule: helper.FloatRound(schedule, 2)}, nil
		}
	}
	return nil, nil
}

type DiskInfo struct {
	*blobnode.DiskInfo
	DiskLoad       int         `json:"disk_load"`
	RepairSchedule float64     `json:"repair_schedule"`
	VolumeIds      []proto.Vid `json:"volume_ids"`
}

type DiskListInput struct {
	Status enums.DiskStatus `json:"status" form:"status"`

	Idc    string `json:"idc" form:"idc"`
	Rack   string `json:"rack" form:"rack"`
	Host   string `json:"host" form:"host"`
	Marker uint32 `json:"marker" form:"marker"`
	Count  int    `json:"count" form:"count"`

	Page       int              `json:"page" form:"page"`
	GetHistory bool             `json:"get_history" form:"get_history"`
	Readonly   enums.ReadOnlyOp `json:"readonly" form:"readonly"`
	NodeData   bool             `json:"node_data" form:"node_data"`
	DiskId     uint32           `json:"disk_id" form:"disk_id"`
	IsAll      bool             `json:"is_all" form:"is_all"`
}

func GetDiskList(c *gin.Context, consulAddr string, input *DiskListInput) (*clustermgr.ListDiskRet, error) {
	disks, err := GetDisks(c, consulAddr, input)
	if err != nil {
		return nil, err
	}
	if !input.IsAll {
		disks.Disks = FilterStatus(disks.Disks, uint8(input.Status))
		return disks, nil
	}
	if input.GetHistory {
		disks.Disks = FilterHistory(disks.Disks)
	} else {
		disks.Disks = FilterStatus(disks.Disks, uint8(input.Status))
	}
	if input.Readonly != 0 {
		disks.Disks = FilterReadonly(disks.Disks, input.Readonly)
	}
	if input.DiskId != 0 {
		disks.Disks = FilterDiskId(disks.Disks, input.DiskId)
	}
	if input.NodeData {
		return disks, nil
	}
	SortByDiskId(disks.Disks)
	if input.Page != 0 && input.Count != 0 {
		disks.Disks = PagingMachine(disks.Disks, input.Page, input.Count)
	}
	return disks, nil
}

func GetDisks(c *gin.Context, consulAddr string, input *DiskListInput) (*clustermgr.ListDiskRet, error) {
	if !input.IsAll {
		return disk.List(c, consulAddr, nil)
	}

	var err error
	var marker uint32
	out := &clustermgr.ListDiskRet{}
	for {
		input.Marker = marker
		var disks *clustermgr.ListDiskRet
		in := &disk.ListInput{}
		if err = copier.Copy(in, input); err != nil {
			return nil, err
		}
		disks, err = disk.List(c, consulAddr, in)
		if err != nil {
			break
		}
		if len(disks.Disks) == 0 {
			break
		}
		out.Disks = append(out.Disks, disks.Disks...)
		marker = uint32(disks.Marker)
	}
	return out, err
}

// FilterHistory *过滤初历史坏盘,过滤规则:host+path有多个的情况根据disk小到大排序,不要disk最大的
func FilterHistory(disks []*blobnode.DiskInfo) []*blobnode.DiskInfo {
	output := make([]*blobnode.DiskInfo, 0)
	diskMap := make(map[string][]*blobnode.DiskInfo)
	for _, v := range disks {
		if _, ok := diskMap[v.Host+v.Path]; ok {
			diskMap[v.Host+v.Path] = append(diskMap[v.Host+v.Path], v)
		} else {
			diskMap[v.Host+v.Path] = []*blobnode.DiskInfo{v}
		}
	}
	for _, v := range diskMap {
		if len(v) > 1 {
			SortByDiskId(v)
			output = append(output, v[:len(v)-1]...)
		}
	}
	return output
}

func FilterStatus(disks []*blobnode.DiskInfo, status uint8) []*blobnode.DiskInfo {
	if status == 0 {
		return disks
	}
	output := make([]*blobnode.DiskInfo, 0)
	diskMap := make(map[string]*blobnode.DiskInfo)
	for _, v := range disks {
		val, ok := diskMap[v.Host+v.Path]
		if !ok || v.DiskID >= val.DiskID {
			diskMap[v.Host+v.Path] = v
		}
	}
	for _, v := range diskMap {
		if uint8(v.Status) == status {
			output = append(output, v)
		}
	}
	return output
}

func FilterReadonly(disks []*blobnode.DiskInfo, readonly enums.ReadOnlyOp) []*blobnode.DiskInfo {
	if readonly == 0 || len(disks) == 0 {
		return disks
	}
	readOnly := readonly == enums.ReadOnly
	out := make([]*blobnode.DiskInfo, 0)
	for _, v := range disks {
		if v.Readonly == readOnly {
			out = append(out, v)
		}
	}
	return out
}

func FilterDiskId(disks []*blobnode.DiskInfo, diskId uint32) []*blobnode.DiskInfo {
	if diskId == 0 || len(disks) == 0 {
		return disks
	}
	out := make([]*blobnode.DiskInfo, 0)
	for _, v := range disks {
		if uint32(v.DiskID) == diskId {
			out = append(out, v)
			break
		}
	}
	return out
}

func FilterIdc(disks []*blobnode.DiskInfo, idc string) []*blobnode.DiskInfo {
	if idc == "" || len(disks) == 0 {
		return disks
	}
	out := make([]*blobnode.DiskInfo, 0)
	for _, v := range disks {
		if v.Idc == idc {
			out = append(out, v)
		}
	}
	return out
}

func SortByDiskId(disks []*blobnode.DiskInfo) {
	sort.Slice(disks, func(i, j int) bool {
		return disks[i].DiskID < disks[j].DiskID
	})
}

func PagingMachine(disks []*blobnode.DiskInfo, page, count int) []*blobnode.DiskInfo {
	if len(disks) == 0 || page == 0 || count == 0 {
		return disks
	}
	start := (page - 1) * count
	end := page * count
	if start <= 0 {
		start = 0
	}
	if end > len(disks) {
		end = len(disks)
	}
	if start > end {
		start = end
	}
	return disks[start:end]
}

func PagingMachineStr(strArr []string, page, count int) []string {
	if len(strArr) == 0 || page == 0 || count == 0 {
		return strArr
	}
	start := (page - 1) * count
	end := page * count
	if start <= 0 {
		start = 0
	}

	if end > len(strArr) {
		end = len(strArr)
	}

	if start > end {
		start = end
	}

	return strArr[start:end]
}
