package user

import (
	"time"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/copier"

	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/helper/types"
	"github.com/cubefs/cubefs/console/backend/model"
	"github.com/cubefs/cubefs/console/backend/service/user"
)

type CreateInput struct {
	Id          string `json:"id" binding:"required"`
	Type        uint8  `json:"type" binding:"required"`
	Description string `json:"description"`
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
	in := new(user.CreateInput)
	err = copier.Copy(in, input)
	if err != nil {
		log.Errorf("copy user.CreateInput failed.args:%+v, err:%+v", input, err)
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return
	}
	data, err := user.Create(c, addr, in)
	if err != nil {
		log.Errorf("user.Create failed.args:%+v,addr:%s,err:%+v", input, addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	userModel := &model.User{
		Name:       data.UserID,
		Role:       in.Type,
		AccessKey:  types.EncryptStr(data.AccessKey),
		SecretKey:  types.EncryptStr(data.SecretKey),
		CreatorId:  loginUser.Id,
		CreateTime: time.Now(),
	}
	if err = userModel.Create(); err != nil {
		log.Errorf("userModel.Create failed.userModel:%+v,err:%+v", userModel, err)
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), data)
}

type ListInput struct {
	Keywords string `form:"keywords"`
	VolName  string `form:"vol_name"`
}

type ListOutPut struct {
	Users  []User   `json:"users"`
	Policy []Policy `json:"policy"`
}

type User struct {
	UserID     string `json:"user_id"`
	UserType   string `json:"user_type"`
	AccessKey  string `json:"access_key"`
	SecretKey  string `json:"secret_key"`
	CreateTime string `json:"create_time"`
}

type Policy struct {
	UserID   string   `json:"user_id"`
	Volume   string   `json:"volume"`
	Policy   []string `json:"policy"`
	Business string   `json:"business"`
}

func List(c *gin.Context) {
	input := &ListInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	userInfos, err := user.List(c, addr, input.Keywords)
	if err != nil {
		log.Errorf("user.List failed.args:%+v,addr:%s,err:%+v", input, addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	output := ListOutPut{
		Users:  make([]User, 0),
		Policy: make([]Policy, 0),
	}
	for _, u := range *userInfos {
		output.Users = append(output.Users, User{
			UserID:     u.UserID,
			UserType:   u.UserType.String(),
			AccessKey:  u.AccessKey,
			SecretKey:  u.SecretKey,
			CreateTime: u.CreateTime,
		})
	}
	policies := GetUserPolicy(*userInfos, input.VolName)
	output.Policy = policies
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

func GetUserPolicy(users []user.InfoOutput, volName string) (policy []Policy) {
loop:
	for _, v := range users {
		if volName != "" {
			if a, ok := v.Policy.AuthorizedVols[volName]; ok {
				p := Policy{
					UserID:   v.UserID,
					Volume:   volName,
					Policy:   a,
					Business: v.Description,
				}
				policy = append(policy, p)
				continue loop
			}
		} else {
			for x, z := range v.Policy.AuthorizedVols {
				p := Policy{
					UserID:   v.UserID,
					Volume:   x,
					Policy:   z,
					Business: v.Description,
				}
				policy = append(policy, p)
			}
		}
		for _, p := range v.Policy.OwnVols {
			if volName != "" {
				if volName == p {
					p := Policy{
						UserID:   v.UserID,
						Volume:   p,
						Policy:   []string{"owner"},
						Business: v.Description,
					}
					policy = append(policy, p)
					continue loop
				}
			} else {
				p := Policy{
					UserID:   v.UserID,
					Volume:   p,
					Policy:   []string{"owner"},
					Business: v.Description,
				}
				policy = append(policy, p)
			}
		}
	}
	return
}

func ListNames(c *gin.Context) {
	input := &ListInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	userInfos, err := user.List(c, addr, input.Keywords)
	if err != nil {
		log.Errorf("user.List failed.args:%+v,addr:%s,err:%+v", input, addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	output := make([]string, 0)
	for _, u := range *userInfos {
		output = append(output, u.UserID)
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

type UpdatePolicyInput struct {
	UserId string   `json:"user_id" binding:"required"`
	Volume string   `json:"volume" binding:"required"`
	Policy []string `json:"policy" binding:"required"`
}

func UpdatePolicy(c *gin.Context) {
	input := &UpdatePolicyInput{}
	addr, err := ginutils.CheckAndGetMaster(c, input)
	if err != nil {
		return
	}
	in := new(user.UpdatePolicyInput)
	if err = copier.Copy(in, input); err != nil {
		log.Errorf("copy failed.args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.InvalidArgs.Code(), err.Error(), nil)
		return
	}
	output, err := user.UpdatePolicy(c, addr, in)
	if err != nil {
		log.Errorf("user.UpdatePolicy failed.args:%+v,addr:%s,err:%+v", in, addr, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}

type ListVolsOutput struct {
	Page    int         `json:"page"`
	PerPage int         `json:"per_page"`
	Count   int64       `json:"count"`
	Data    []model.Vol `json:"data"`
}

func ListVols(c *gin.Context) {
	input := &model.FindVolsParam{}
	if !ginutils.Check(c, input) {
		return
	}
	vols, count, err := new(model.Vol).Find(input)
	if err != nil {
		log.Errorf("model.Vol.Find failed.input:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.DatabaseError.Code(), err.Error(), nil)
		return
	}
	output := ListVolsOutput{
		Page:    input.Page,
		PerPage: input.PerPage,
		Count:   count,
		Data:    vols,
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), output)
}
