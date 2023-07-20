package user

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/cubefs/cubefs/proto"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/httputils"
)

type CreateInput struct {
	Id          string `json:"id"`
	Type        int    `json:"type"`
	Description string `json:"description"`
	Pwd         string `json:"pwd,omitempty"`
}

func Create(c *gin.Context, clusterAddr string, input *CreateInput) (*InfoOutput, error) {
	reqUrl := "http://" + clusterAddr + proto.UserCreate
	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodPost, bytes.NewReader(b), nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &InfoOutput{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	return output.Data.(*InfoOutput), nil
}

type InfoOutput struct {
	UserID      string            `json:"user_id"`
	AccessKey   string            `json:"access_key"`
	SecretKey   string            `json:"secret_key"`
	Policy      *proto.UserPolicy `json:"policy"`
	UserType    proto.UserType    `json:"user_type"`
	CreateTime  string            `json:"create_time"`
	Description string            `json:"description"`
}

func Info(c *gin.Context, clusterAddr, userName string) (*InfoOutput, error) {
	reqUrl := "http://" + clusterAddr + proto.UserGetInfo + "?user=" + userName
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &InfoOutput{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	return output.Data.(*InfoOutput), nil
}

func List(c *gin.Context, clusterAddr, keywords string) (*[]InfoOutput, error) {
	reqUrl := "http://" + clusterAddr + proto.UserList + "?keywords=" + keywords
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{Data: &[]InfoOutput{}}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	return output.Data.(*[]InfoOutput), nil
}

type UpdatePolicyInput struct {
	UserId string   `json:"user_id"`
	Volume string   `json:"volume"`
	Policy []string `json:"policy"`
}

func UpdatePolicy(c *gin.Context, clusterAddr string, input *UpdatePolicyInput) (interface{}, error) {
	reqUrl := "http://" + clusterAddr + proto.UserUpdatePolicy
	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	resp, err := httputils.DoRequestNoCookie(c, reqUrl, http.MethodPost, bytes.NewReader(b), nil)
	if err != nil {
		return nil, err
	}
	output := httputils.Output{}
	_, err = httputils.HandleResponse(c, resp, err, &output)
	if err != nil {
		return nil, err
	}
	if output.Code != proto.ErrCodeSuccess {
		return nil, errors.New(output.Msg)
	}
	return output.Data, nil
}
