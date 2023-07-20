package vol

import (
	"errors"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"

	"github.com/cubefs/cubefs/console/backend/helper"
	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/crypt"
	"github.com/cubefs/cubefs/console/backend/helper/enums"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/model"
	"github.com/cubefs/cubefs/console/backend/service/user"
	"github.com/cubefs/cubefs/console/backend/service/vol"
)

type CreateInput struct {
	Name            string `json:"name" binding:"required"`
	Owner           string `json:"owner" binding:"required"`
	Capacity        uint64 `json:"capacity" binding:"required"`
	CrossZone       bool   `json:"cross_zone"`
	Business        string `json:"business"`
	DefaultPriority bool   `json:"default_priority"`
	VolType         int    `json:"vol_type"`
	ReplicaNumber   int    `json:"replica_number"`
	CacheCap        int    `json:"cache_cap"`
}

func (input *CreateInput) Check() error {
	switch input.VolType {
	case int(enums.VolTypeLowFrequency):
		input.ReplicaNumber = 1
		if input.CacheCap <= 0 {
			return errors.New("cache_cap should great than 0")
		}
	case int(enums.VolTypeStandard):
		if input.ReplicaNumber <= 0 {
			return errors.New("replica_number should great than 0")
		}
	default:
		err := fmt.Errorf("invalid vol_type:%d", input.VolType)
		log.Error(err)
		return err
	}
	return nil
}

func Create(c *gin.Context) {
	input := &CreateInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}

	loginUser, err := ginutils.GetLoginUser(c)
	if err != nil {
		log.Errorf("ginutils.GetLoginUser failed.input:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.Unauthorized.Code(), err.Error(), nil)
		return
	}

	_, err = user.Info(c, addr, input.Owner)
	if err != nil {
		log.Errorf("user.Info failed.user_id:%s,err:%+v", input.Owner, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}

	in := &vol.CreateInput{Description: input.Business}
	if err = copier.Copy(in, input); err != nil {
		log.Errorf("copy CreateInput failed. args:%+v, err:%+v", input, err)
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return
	}
	if input.VolType == int(enums.VolTypeLowFrequency) {
		in.FollowerRead = true
		in.ReplicaNumber = 1
	}
	data, err := vol.Create(c, addr, in)
	if err != nil {
		log.Errorf("vol.Create failed.args:%+v,addr:%s,err:%+v", input, addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	createVol(input, loginUser)
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), data)
}

func createVol(input *CreateInput, loginUser *ginutils.LoginUser) {
	volModel := &model.Vol{}
	if err := copier.Copy(volModel, input); err != nil {
		log.Errorf("copy vol model failed. input:%+v,err:%+v", input, err)
		return
	}
	volModel.CreatorId = loginUser.Id
	if err := volModel.Create(); err != nil {
		log.Errorf("volModel.Create failed.volModel:%+v,err:%+v", volModel, err)
	}
}

type ListInput struct {
	KeyWords string `form:"keywords"`
}

type ListOutput struct {
	Name         string `json:"name"`
	Owner        string `json:"owner"`
	CreateTime   string `json:"create_time"`
	Status       string `json:"status"`
	TotalSize    uint64 `json:"total_size"`
	UsedSize     string `json:"used_size"`
	UsageRatio   string `json:"usage_ratio"`
	DpReplicaNum uint8  `json:"dp_replica_num"`
	InodeCount   uint64 `json:"inode_count"`
	DentryCount  uint64 `json:"dentry_count"`
	MpCnt        int    `json:"mp_cnt"`
	DpCnt        int    `json:"dp_cnt"`
	Business     string `json:"business"`
	VolType      int    `json:"vol_type"`
	FollowerRead bool   `json:"follower_read"`
}

func List(c *gin.Context) {
	input := &ListInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	vols, err := vol.Get(c, addr, input.KeyWords)
	if err != nil {
		log.Errorf("vol.Get failed. args:%+v, err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	out := make([]ListOutput, 0)
	for _, v := range *vols {
		volInfo, err := vol.GetByName(c, addr, v.Name)
		if err != nil {
			log.Errorf("vol.GetByName failed. args:%+v,name:%s,err:%+v", input, v.Name, err)
			ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
			return
		}
		volOut := ListOutput{
			Name:         v.Name,
			Owner:        volInfo.Owner,
			CreateTime:   time.Unix(v.CreateTime, 0).Format(helper.LayoutDefault),
			Status:       formatVolumeStatus(v.Status),
			TotalSize:    v.TotalSize,
			UsedSize:     helper.ByteConversion(v.UsedSize),
			UsageRatio:   helper.Percentage(v.UsedSize, v.TotalSize),
			DpReplicaNum: volInfo.DpReplicaNum,
			InodeCount:   volInfo.InodeCount,
			DentryCount:  volInfo.DentryCount,
			MpCnt:        volInfo.MpCnt,
			DpCnt:        volInfo.DpCnt,
			Business:     volInfo.Description,
			VolType:      volInfo.VolType,
			FollowerRead: volInfo.FollowerRead,
		}
		out = append(out, volOut)
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), out)
}

func formatVolumeStatus(status uint8) string {
	switch status {
	case 0:
		return "Normal"
	case 1:
		return "Marked delete"
	default:
		return "Unknown"
	}
}

type InfoInput struct {
	Name string `form:"name" binding:"required"`
}

func Info(c *gin.Context) {
	input := &InfoInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	out, err := vol.GetByName(c, addr, input.Name)
	if err != nil {
		log.Errorf("vol.GetByName failed. args:%+v, err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), out)
}

type UpdateInput struct {
	Name           string `json:"name"`
	CacheCap       uint64 `json:"CacheCapacity"`
	CacheThreshold int    `json:"CacheThreshold"`
	CacheTTL       int    `json:"CacheTtl"`
}

func Update(c *gin.Context) {
	input := &UpdateInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	volInfo, err := vol.GetByName(c, addr, input.Name)
	if err != nil {
		log.Errorf("vol.GetByName failed. args:%+v, err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	in := &vol.UpdateInput{AuthKey: crypt.Md5Encryption(volInfo.Owner)}
	if err = copier.Copy(in, input); err != nil {
		log.Errorf("copy UpdateInput failed. args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return
	}
	if volInfo.DpReplicaNum == 0 {
		volInfo.DpReplicaNum = 1
		in.ReplicaNumber = &volInfo.DpReplicaNum
	}
	if volInfo.FollowerRead == false {
		volInfo.FollowerRead = true
		in.FollowerRead = &volInfo.FollowerRead
	}
	out, err := vol.Update(c, addr, in)
	if err != nil {
		log.Errorf("vol.Update failed.cluster:%s,addr:%s,args:%+v,err:%+v", c.Param(ginutils.Cluster), addr, in, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), out)
}

type ExpandInput struct {
	Name     string `json:"name" binding:"required"`
	Capacity uint64 `json:"capacity" binding:"required"`
}

func Expand(c *gin.Context) {
	expandOrShrink(c, pathExpand)
}

func Shrink(c *gin.Context) {
	expandOrShrink(c, pathShrink)
}

const (
	pathExpand = "expand"
	pathShrink = "shrink"
)

func expandOrShrink(c *gin.Context, path string) {
	input := &ExpandInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	volInfo, err := vol.GetByName(c, addr, input.Name)
	if err != nil {
		log.Errorf("vol.GetByName failed. args:%+v, err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	in := &vol.ExpandInput{AuthKey: crypt.Md5Encryption(volInfo.Owner)}
	if err = copier.Copy(in, input); err != nil {
		log.Errorf("copy UpdateInput failed. args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return
	}
	switch path {
	case pathExpand:
		err = vol.Expand(c, addr, in)
	case pathShrink:
		err = vol.Shrink(c, addr, in)
	}
	if err != nil {
		log.Errorf("vol.%s failed. args:%+v,err:%+v", path, input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}
