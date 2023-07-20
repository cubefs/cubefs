package volume

import (
	"math"
	"sort"
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"

	"github.com/cubefs/cubefs/console/backend/handler/blobstore/common"
	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/helper/pool"
	mgr "github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/clustermgr/volume"
)

type ListInput struct {
	Count  int `form:"count"`
	Marker int `form:"marker"`
}

func (input *ListInput) Check() error {
	if input.Count <= 0 {
		input.Count = 15
	}
	if input.Marker <= 0 {
		input.Marker = 0
	}
	return nil
}

type ListOutput struct {
	Volumes []Info `json:"volumes"`
	Total   int    `json:"total"`
}

type Info struct {
	Units []common.VolumeUnit `json:"units"`
	clustermgr.VolumeInfoBase
}

func List(c *gin.Context) {
	input := &ListInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	volumeInfo, err := volume.List(c, consulAddr, &volume.ListInput{Marker: input.Marker, Count: input.Count})
	if err != nil {
		log.Errorf("volume.List failed.consulAddr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	_, diskLoad, err := common.GetWritingVolumes(c, consulAddr, &common.WritingVolumeInput{IsAll: true})
	if err != nil {
		log.Errorf("common.GetWritingVolumes failed.consulAddr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	volumes := make([]Info, 0)
	for _, vol := range volumeInfo.Volumes {
		info := Info{VolumeInfoBase: vol.VolumeInfoBase, Units: make([]common.VolumeUnit, 0)}
		for _, u := range vol.Units {
			info.Units = append(info.Units, common.GetVolumeUnit(u, diskLoad))
		}

		volumes = append(volumes, info)
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), ListOutput{Volumes: volumes, Total: len(volumes)})
}

func WritingList(c *gin.Context) {
	input := &common.WritingVolumeInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	output, _, err := common.GetWritingVolumes(c, consulAddr, input)
	if err != nil {
		log.Errorf("common.GetWritingVolumes failed.consulAddr:%s,input:%+v,err:%+v", consulAddr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

type V2ListInput struct {
	Count  int                `form:"count"`
	Page   int                `form:"page"`
	Status proto.VolumeStatus `form:"status"`
}

func V2List(c *gin.Context) {
	input := &V2ListInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	var volumes *clustermgr.ListVolumes
	if input.Status == proto.VolumeStatusIdle {
		volumes, err = GetIdleVolume(c, consulAddr)
	} else {
		volumes, err = volume.V2List(c, consulAddr, int(input.Status))
	}
	if err != nil {
		log.Errorf("get volumes failed.consulAddr:%s,args:%+v,err:%+v", consulAddr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	output := ListOutput{
		Volumes: make([]Info, 0),
		Total:   len(volumes.Volumes),
	}
	if len(volumes.Volumes) == 0 {
		ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
		return
	}
	if input.Count == 0 || input.Page == 0 {
		err = copier.Copy(&output.Volumes, volumes.Volumes)
		if err != nil {
			log.Errorf("copy volumes failed.err:%+v", err)
			ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
			return
		}
		ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
		return
	}

	sort.Slice(volumes.Volumes, func(i, j int) bool {
		return volumes.Volumes[i].Vid < volumes.Volumes[j].Vid
	})

	volumes.Volumes = common.PagingMachineVolume(volumes.Volumes, input.Page, input.Count)
	_, diskLoad, err := common.GetWritingVolumes(c, consulAddr, &common.WritingVolumeInput{IsAll: true})
	if err != nil {
		log.Errorf("common.GetWritingVolumes failed.consulAddr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}

	for _, v := range volumes.Volumes {
		item := Info{
			Units:          make([]common.VolumeUnit, 0),
			VolumeInfoBase: v.VolumeInfoBase,
		}
		for _, u := range v.Units {
			item.Units = append(item.Units, common.GetVolumeUnit(u, diskLoad))
		}
		output.Volumes = append(output.Volumes, item)
	}

	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

func GetIdleVolume(c *gin.Context, consulAddr string) (*clustermgr.ListVolumes, error) {
	stat, err := mgr.Stat(c, consulAddr, true)
	if err != nil {
		log.Errorf("mgr.Stat failed.consulAddr:%s,err:%+v", consulAddr, err)
		return nil, err
	}
	limit := 500
	totalPage := int(math.Ceil(float64(stat.VolumeStat.TotalVolume) / float64(limit)))
	poolSize := 50
	if totalPage < poolSize {
		poolSize = totalPage
	}
	tp := pool.New(poolSize, poolSize)
	volumesChan := make(chan *clustermgr.ListVolumes, poolSize)
	go GetAllVolume(c, consulAddr, limit, totalPage, tp, volumesChan)
	out := &clustermgr.ListVolumes{
		Volumes: make([]*clustermgr.VolumeInfo, 0),
	}
	for volumes := range volumesChan {
		for _, vol := range volumes.Volumes {
			if vol.Status == proto.VolumeStatusIdle {
				out.Volumes = append(out.Volumes, vol)
			}
		}
	}
	if len(out.Volumes) != stat.VolumeStat.IdleVolume {
		log.Warnf("stat.IdleVolume[%d] != len(out.Volumes)[%d]", stat.VolumeStat.IdleVolume, len(out.Volumes))
	}
	return out, nil
}

func GetAllVolume(c *gin.Context, consulAddr string, count, totalPage int, tp pool.TaskPool, volChan chan *clustermgr.ListVolumes) {
	wg := sync.WaitGroup{}
	for i := 0; i < totalPage; i++ {
		marker := i * count
		wg.Add(1)
		tp.Run(func() {
			defer wg.Done()
			out, err := volume.List(c, consulAddr, &volume.ListInput{Marker: marker, Count: count})
			if err != nil {
				log.Errorf("volume.List failed. consulAddr:%s,err:%+v", consulAddr, err)
			} else {
				if len(out.Volumes) > 0 {
					volChan <- out
				}
			}
		})
	}
	wg.Wait()
	close(volChan)
	tp.Close()
}

type AllocatedListInput struct {
	Host     string `form:"host" binding:"required"`
	CodeMode uint8  `form:"code_mode"`
}

func AllocatedList(c *gin.Context) {
	input := &AllocatedListInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	volumes, err := volume.AllocatedList(c, consulAddr, &volume.AllocatedListInput{Host: input.Host, CodeMode: input.CodeMode})
	if err != nil {
		log.Errorf("volume.AllocatedList failed.consulAddr:%s,args:%+v,err:%+v", consulAddr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), volumes)
}

type GetInput struct {
	Vid uint32 `form:"vid" binding:"required"`
}

func Get(c *gin.Context) {
	input := &GetInput{}
	consulAddr, err := ginutils.CheckAndGetConsul(c, input)
	if err != nil {
		return
	}
	volInfo, err := volume.Get(c, consulAddr, input.Vid)
	if err != nil {
		log.Errorf("volume.Get failed.consulAddr:%s,args:%+v,err:%+v", consulAddr, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	_, diskLoad, err := common.GetWritingVolumes(c, consulAddr, &common.WritingVolumeInput{IsAll: true})
	if err != nil {
		log.Errorf("common.GetWritingVolumes failed.consulAddr:%s,err:%+v", consulAddr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	info := Info{VolumeInfoBase: volInfo.VolumeInfoBase, Units: make([]common.VolumeUnit, 0)}
	for _, u := range volInfo.Units {
		info.Units = append(info.Units, common.GetVolumeUnit(u, diskLoad))
	}

	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), info)
}
