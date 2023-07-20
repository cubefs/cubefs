package s3

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
)

type GetVolCorsInput struct {
	Vol  string `form:"vol" binding:"required"`
	User string `form:"user" binding:"required"`
}

func GetVolCors(c *gin.Context) {
	input := &GetVolCorsInput{}
	if !ginutils.Check(c, input) {
		return
	}
	s3client, ok := GetS3Client(c, input.User, input.Vol)
	if !ok {
		return
	}
	out, err := s3client.GetBucketCors(&s3.GetBucketCorsInput{Bucket: aws.String(input.Vol)})
	if err != nil {
		log.Errorf("s3client.GetBucketCors failed.input:%+v,err:%+v", input, err)
		if strings.Contains(err.Error(), "NoSuchCORSConfiguration") {
			ginutils.Success(c, s3.GetBucketCorsOutput{})
			return
		}
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Success(c, out)
}

type PutVolCorsInput struct {
	Vol   string         `json:"vol" binding:"required"`
	User  string         `json:"user" binding:"required"`
	Rules []*s3.CORSRule `json:"rules" binding:"required,gte=1"`
}

func PutVolCors(c *gin.Context) {
	input := &PutVolCorsInput{}
	if !ginutils.Check(c, input) {
		return
	}
	s3client, ok := GetS3Client(c, input.User, input.Vol)
	if !ok {
		return
	}

	conf := &s3.CORSConfiguration{
		CORSRules: input.Rules,
	}
	in := &s3.PutBucketCorsInput{
		Bucket:            aws.String(input.Vol),
		CORSConfiguration: conf,
	}
	_, err := s3client.PutBucketCors(in)
	if err != nil {
		log.Errorf("s3client.PutBucketCors failed.input:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Success(c, nil)
}

type DeleteVolCorsInput struct {
	Vol  string `form:"vol" binding:"required"`
	User string `form:"user" binding:"required"`
}

func DeleteVolCors(c *gin.Context) {
	input := &DeleteVolCorsInput{}
	if !ginutils.Check(c, input) {
		return
	}
	s3client, ok := GetS3Client(c, input.User, input.Vol)
	if !ok {
		return
	}
	_, err := s3client.DeleteBucketCors(&s3.DeleteBucketCorsInput{Bucket: aws.String(input.Vol)})
	if err != nil {
		log.Errorf("s3client.DeleteBucketCors failed.input:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Success(c, nil)
}
