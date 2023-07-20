package common

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/pool"
	"github.com/cubefs/cubefs/console/backend/service/blobstore/proxy"
)

type WritingVolumeInput struct {
	Page  int       `json:"page" form:"page" binding:"required"`
	Count int       `json:"count" form:"count" binding:"required"`
	Vid   proto.Vid `json:"vid" form:"vid"`
	IsAll bool      `json:"is_all" form:"is_all"`
}

type WritingVolumeOutput struct {
	Total   int                 `json:"total"`
	Volumes []WritingVolumeInfo `json:"volumes"`
}

type WritingVolumeInfo struct {
	Units []VolumeUnit `json:"units"`
	clustermgr.VolumeInfoBase
	Token      string `json:"token"`
	ExpireTime int64  `json:"expire_time"`
	Allocator  string `json:"allocator"`
}

func GetWritingVolumes(c *gin.Context, consulAddr string, input *WritingVolumeInput) (*WritingVolumeOutput, map[proto.DiskID]DiskVolumeInfo, error) {
	proxies, err := GetProxyHost(c, consulAddr)
	if err != nil {
		log.Errorf("GetProxyHost failed.consulAddr:%s,err:%+v", consulAddr, err)
		return nil, nil, err
	}
	codeModes, err := GetCodeMode(c, consulAddr)
	if err != nil {
		log.Errorf("GetCodeMode failed.consulAddr:%s,err:%+v", consulAddr, err)
		return nil, nil, err
	}
	hostCodes := make([]HostCode, 0)
	for _, v := range proxies {
		for _, mode := range codeModes {
			hostCodes = append(hostCodes, HostCode{Host: v, Code: mode.CodeMode})
		}
	}
	poolSize := pool.GetPoolSize(30, len(proxies))
	volumeChan := make(chan *VolumeList, poolSize)
	tp := pool.New(poolSize, poolSize)
	go getVolumeList(c, hostCodes, tp, volumeChan)
	volumes := &VolumeList{Volumes: make([]AllocVolumeInfo, 0)}
	for v := range volumeChan {
		if len(v.Volumes) == 0 {
			continue
		}
		volumes.Volumes = append(volumes.Volumes, v.Volumes...)
	}
	diskLoad := GetDiskLoadByWritingVolume(volumes.Volumes)
	output := &WritingVolumeOutput{
		Total:   len(volumes.Volumes),
		Volumes: make([]WritingVolumeInfo, 0),
	}
	// search one volume
	if input.Vid != 0 {
		for _, v := range volumes.Volumes {
			if v.Vid == input.Vid {
				output.Volumes = append(output.Volumes, FormatAllocVolumeInfo(v, diskLoad))
				break
			}
		}
		output.Total = len(output.Volumes)
		if output.Total == 0 {
			err = fmt.Errorf("volume[%d] not found", input.Vid)
			log.Errorf("search one volume failed.input:%+v,err:%+v", input, err)
			return nil, diskLoad, err
		}
		return output, diskLoad, nil
	}
	sort.Slice(volumes.Volumes, func(i, j int) bool {
		return volumes.Volumes[i].Vid < volumes.Volumes[j].Vid
	})
	if !input.IsAll {
		volumes.Volumes = PagingMachineAllocVolume(volumes.Volumes, input.Page, input.Count)
	}
	for _, v := range volumes.Volumes {
		output.Volumes = append(output.Volumes, FormatAllocVolumeInfo(v, diskLoad))
	}
	return output, diskLoad, err
}

func PagingMachineAllocVolume(volumes []AllocVolumeInfo, page, count int) []AllocVolumeInfo {
	if len(volumes) == 0 {
		return volumes
	}
	start := (page - 1) * count
	end := page * count
	if start <= 0 {
		start = 0
	}

	if end > len(volumes) {
		end = len(volumes)
	}

	if start > end {
		start = end
	}

	return volumes[start:end]
}

func PagingMachineVolume(volumes []*clustermgr.VolumeInfo, page, count int) []*clustermgr.VolumeInfo {
	if len(volumes) == 0 {
		return volumes
	}
	start := (page - 1) * count
	end := page * count
	if start <= 0 {
		start = 0
	}

	if end > len(volumes) {
		end = len(volumes)
	}

	if start > end {
		start = end
	}

	return volumes[start:end]
}

func FormatAllocVolumeInfo(vol AllocVolumeInfo, diskMap map[proto.DiskID]DiskVolumeInfo) WritingVolumeInfo {
	units := make([]VolumeUnit, len(vol.Units))
	for i, u := range vol.Units {
		units[i] = GetVolumeUnit(u, diskMap)
	}
	return WritingVolumeInfo{Units: units,
		VolumeInfoBase: vol.VolumeInfoBase,
		Token:          vol.Token,
		ExpireTime:     vol.ExpireTime,
		Allocator:      vol.Allocator,
	}
}

type DiskVolumeInfo struct {
	DiskLoad int         `json:"disk_load"`
	VolumeId []proto.Vid `json:"volume_id"`
}

func GetDiskLoadByWritingVolume(volumes []AllocVolumeInfo) map[proto.DiskID]DiskVolumeInfo {
	diskMap := make(map[proto.DiskID]DiskVolumeInfo)
	for _, v := range volumes {
		for _, disk := range v.Units {
			val, ok := diskMap[disk.DiskID]
			if ok {
				val.DiskLoad += 1
				val.VolumeId = append(val.VolumeId, v.Vid)
			} else {
				val = DiskVolumeInfo{DiskLoad: 1, VolumeId: []proto.Vid{v.Vid}}
			}
			diskMap[disk.DiskID] = val
		}
	}
	return diskMap
}

type HostCode struct {
	Host string            `json:"host"`
	Code codemode.CodeMode `json:"code"`
}

type VolumeList struct {
	Vids    []proto.Vid       `json:"vids"`
	Volumes []AllocVolumeInfo `json:"volumes"`
}

type AllocVolumeInfo struct {
	clustermgr.AllocVolumeInfo
	Allocator string `json:"allocator"`
}

type VolumeUnit struct {
	clustermgr.Unit
	VuidPrefix proto.VuidPrefix `json:"vuid_prefix"`
	Epoch      uint32           `json:"epoch"`
	Index      uint8            `json:"index"`
	DiskLoad   int              `json:"disk_load"`
}

func GetVolumeUnit(u clustermgr.Unit, diskLoad map[proto.DiskID]DiskVolumeInfo) VolumeUnit {
	return VolumeUnit{
		Unit:       u,
		VuidPrefix: u.Vuid.VuidPrefix(),
		Epoch:      u.Vuid.Epoch(),
		Index:      u.Vuid.Index(),
		DiskLoad:   diskLoad[u.DiskID].DiskLoad,
	}
}

func getVolumeList(c *gin.Context, hostCodes []HostCode, tp pool.TaskPool, volumeChan chan *VolumeList) {
	wg := sync.WaitGroup{}
	for i := range hostCodes {
		hostCode := hostCodes[i]
		wg.Add(1)
		tp.Run(func() {
			defer wg.Done()
			out, err := proxy.GetVolumeList(c, hostCode.Host, hostCode.Code)
			if err != nil {
				log.Errorf("proxy.GetVolumeList failed. args:%+v, err:%+v", hostCode, err)
				return
			}
			vol := &VolumeList{
				Vids:    out.Vids,
				Volumes: make([]AllocVolumeInfo, 0),
			}
			for _, v := range out.Volumes {
				item := AllocVolumeInfo{
					AllocVolumeInfo: v,
					Allocator:       hostCode.Host,
				}
				vol.Volumes = append(vol.Volumes, item)
			}
			volumeChan <- vol
		})
	}
	wg.Wait()
	close(volumeChan)
	tp.Close()
}
