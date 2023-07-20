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

type MakeDirInput struct {
	Vol     string `json:"vol" binding:"required"`
	User    string `json:"user"`
	DirName string `json:"dir_name" binding:"required"`
	Prefix  string `json:"prefix"`
}

func MakeDir(c *gin.Context) {
	input := &MakeDirInput{}
	if !ginutils.Check(c, input) {
		return
	}
	s3client, ok := GetS3Client(c, input.User, input.Vol)
	if !ok {
		return
	}
	dir := input.Prefix + input.DirName + DELIMITER
	_, err := s3client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(input.Vol),
		Key:    aws.String(dir),
		Body:   aws.ReadSeekCloser(strings.NewReader("")),
	})
	if err != nil {
		log.Errorf("s3client.PutObject failed.args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}
