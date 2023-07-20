package s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cubefs/blobstore/util/log"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
)

type ListFileInput struct {
	Vol       string `form:"vol" binding:"required"`
	User      string `form:"user"`
	Prefix    string `form:"prefix"`
	Delimiter string `form:"delimiter"`
	Marker    string `form:"marker"`
	Limit     int64  `form:"limit"`
}

type ListFileOutput struct {
	Marker    string    `json:"marker,omitempty"`
	Contents  []*FileV2 `json:"contents"`
	Prefixes  []string  `json:"common_prefixes"`
	Directory *FileV2   `json:"directory"`
}

func ListFile(c *gin.Context) {
	input := &ListFileInput{}
	if !ginutils.Check(c, input) {
		return
	}
	s3client, ok := GetS3Client(c, input.User, input.Vol)
	if !ok {
		return
	}

	maxKeys := input.Limit
	if input.Marker == "" {
		maxKeys += 1
	}
	req, output := s3client.ListObjectsV2Request(&s3.ListObjectsV2Input{
		Bucket:            aws.String(input.Vol),
		ContinuationToken: aws.String(input.Marker),
		Delimiter:         aws.String(input.Delimiter),
		MaxKeys:           aws.Int64(maxKeys),
		Prefix:            aws.String(input.Prefix),
	})
	var err error
	if req.Send() != nil {
		err = req.Send()
	}
	if err != nil {
		log.Errorf("s3.ListObjectsV2 failed.args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	prefixes, contents, directories := getContents(input.Prefix, input.Vol, s3client, output)
	out := ListFileOutput{
		Contents:  contents,
		Prefixes:  prefixes,
		Directory: directories,
	}
	if output.NextContinuationToken != nil {
		out.Marker = *output.NextContinuationToken
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), out)
}

func UploadSignedUrl(c *gin.Context) {
	input := &DownloadSignedUrlInput{}
	if !ginutils.Check(c, input) {
		return
	}
	s3client, ok := GetS3Client(c, input.User, input.Vol)
	if !ok {
		return
	}
	req, _ := s3client.PutObjectRequest(&s3.PutObjectInput{
		Key:    aws.String(input.Prefix + input.FileName),
		Bucket: aws.String(input.Vol),
	})
	expire := GetExpire(7200)
	signedUrl, err := req.Presign(expire)
	if err != nil {
		log.Errorf("req.Presign failed.args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), &DownloadSignedUrlOutput{
		SignedUrl: signedUrl,
		Expire:    int64(expire.Seconds()),
	})
}

type MultiUploadSignedUrlInput struct {
	Vol         string `form:"vol" binding:"required"`
	User        string `form:"user"`
	FileName    string `form:"file_name" binding:"required"`
	Prefix      string `form:"prefix"`
	ContentType string `form:"content_type"`
	PartNumber  int64  `form:"part_number"`
}

type MultiUploadPart struct {
	UploadId   string `json:"upload_id"`
	PartNumber int64  `json:"part_number"`
	Url        string `json:"url"`
	Expire     int64  `json:"expire"`
}

func MultiUploadSignedUrl(c *gin.Context) {
	input := &MultiUploadSignedUrlInput{}
	if !ginutils.Check(c, input) {
		return
	}
	s3client, ok := GetS3Client(c, input.User, input.Vol)
	if !ok {
		return
	}
	expire := GetExpire(7200)
	key := input.Prefix + input.FileName
	uploadOut, err := s3client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(input.Vol),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Errorf("s3client.CreateMultipartUpload failed.input:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	signedUrls := make([]*MultiUploadPart, 0)
	var i int64
	for i = 1; i <= input.PartNumber; i++ {
		req, _ := s3client.UploadPartRequest(&s3.UploadPartInput{
			Bucket:     aws.String(input.Vol),
			Key:        aws.String(key),
			PartNumber: aws.Int64(i),
			UploadId:   uploadOut.UploadId,
		})
		signedUrl, err := req.Presign(expire)
		if err != nil {
			log.Errorf("req.Presign failed.index:%d,input:%+v,err:%+v", i, input, err)
			ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
			return
		}
		signedUrls = append(signedUrls, &MultiUploadPart{
			UploadId:   *uploadOut.UploadId,
			PartNumber: i,
			Url:        signedUrl,
			Expire:     int64(expire.Seconds()),
		})
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), signedUrls)
}

type MultiUploadCompleteInput struct {
	Vol      string `json:"vol" binding:"required"`
	User     string `json:"user"`
	FileName string `json:"file_name" binding:"required"`
	Prefix   string `json:"prefix"`
	UploadId string `json:"upload_id"`
}

func MultiUploadComplete(c *gin.Context) {
	input := &MultiUploadCompleteInput{}
	if !ginutils.Check(c, input) {
		return
	}
	s3client, ok := GetS3Client(c, input.User, input.Vol)
	if !ok {
		return
	}
	key := input.Prefix + input.FileName
	partsOut, err := s3client.ListParts(&s3.ListPartsInput{
		Bucket:   aws.String(input.Vol),
		Key:      aws.String(key),
		UploadId: aws.String(input.UploadId),
	})
	if err != nil {
		log.Errorf("s3client.ListParts failed.args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	parts := make([]*s3.CompletedPart, 0)
	for _, part := range partsOut.Parts {
		parts = append(parts, &s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}
	competeOut, err := s3client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(input.Vol),
		Key:             aws.String(key),
		UploadId:        aws.String(input.UploadId),
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		log.Errorf("s3client.CompleteMultipartUpload failed.args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), competeOut)
}

type DownloadSignedUrlInput struct {
	Vol      string `form:"vol" binding:"required"`
	User     string `form:"user"`
	FileName string `form:"file_name" binding:"required"`
	Prefix   string `form:"prefix"`
}

type DownloadSignedUrlOutput struct {
	SignedUrl string `json:"signed_url"`
	Expire    int64  `json:"expire"`
}

func DownloadSignedUrl(c *gin.Context) {
	input := &DownloadSignedUrlInput{}
	if !ginutils.Check(c, input) {
		return
	}
	s3client, ok := GetS3Client(c, input.User, input.Vol)
	if !ok {
		return
	}
	req, _ := s3client.GetObjectRequest(&s3.GetObjectInput{
		Key:    aws.String(input.Prefix + input.FileName),
		Bucket: aws.String(input.Vol),
	})
	expire := GetExpire(7200)
	signedUrl, err := req.Presign(expire)
	if err != nil {
		log.Errorf("req.Presign failed.args:%+v,err:%+v", input, err)
		ginutils.Send(c, codes.ThirdPartyError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), &DownloadSignedUrlOutput{
		SignedUrl: signedUrl,
		Expire:    int64(expire.Seconds()),
	})
}
