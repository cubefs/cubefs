package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

type S3Client struct {
	Client *s3.S3
}

type DownloadJob struct {
	Key       string
	LocalPath string
	Size      uint32
	DownTime  int64
	err error
}

func (job DownloadJob) String() string {
	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("Key(%s), ", job.Key))
	builder.WriteString(fmt.Sprintf("LocalPath(%s), ", job.LocalPath))
	if job.Size != 0 {
		builder.WriteString(fmt.Sprintf("Size(%d), ", job.Size))
	}
	if job.DownTime != 0 {
		builder.WriteString(fmt.Sprintf("DownTime(%d)", job.DownTime))
	}
	if job.err !=nil {
		builder.WriteString(fmt.Sprintf("DownLoadErr(%v)", job.err))
	}

	return builder.String()
}

func NewS3Client(accessKey, secertKey, endPoint string) *S3Client {
	sess := session.Must(session.NewSessionWithOptions( session.Options{
		Config: aws.Config{
				Credentials:      credentials.NewStaticCredentials(accessKey, secertKey, ""),
				S3ForcePathStyle: aws.Bool(true),
				Endpoint:         aws.String(endPoint),
				Region:           aws.String("us-west-2"),
				DisableComputeChecksums: aws.Bool(true),
				S3DisableContentMD5Validation:aws.Bool(true),
				S3Disable100Continue: aws.Bool(true),
			},
	}))

	return &S3Client{s3.New(sess)}
}

func (s *MagicNode) DownloadWorker(workerID int, jobChan <-chan *DownloadJob) {
	allPathDir := make(map[string]int)

	for job := range jobChan {
		job.LocalPath=strings.Replace(job.LocalPath,"\n","",-1)
		job.Key=strings.Replace(job.Key,"\n","",-1)
		pathdir := filepath.Dir(job.LocalPath)
		_, ok := allPathDir[pathdir]
		if !ok {
			os.MkdirAll(pathdir, 0777)
			allPathDir[pathdir] = 1
		}
		log.LogDebugf("Worker (%v) start downloaded file (%v) ", workerID, job)
		start:=time.Now().UnixMicro()
		tpObject := exporter.NewModuleTPUs("DownloadWorker")
		if job.Key!="" {
			s.DownloadFileForS3(job)
		}else {
			s.DownloadFileForFuse(job)
		}

		tpObject.Set(job.err)
		if job.err != nil {
			log.LogWarnf("Worker (%v) downloaded file (%v) err(%v)", workerID, job, job.err)
			continue
		}
		atomic.AddUint64(&s.cacheSize, uint64(job.Size))

		if job.Key != ""{
			s.putToCleanCh(job)
		}
		end:=time.Now().UnixMicro()
		log.LogDebugf("Worker (%v) downloaded file (%v) success,cost(%v)", workerID, job,end-start)
	}
}

func (s *MagicNode) HandleDownloadRequestForCubeS3(body io.Reader) error {
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}

	var arr [][]string
	err = json.Unmarshal(data, &arr)
	if err != nil {
		return err
	}

	for _, item := range arr {
		job:=GetDownLoadJobFromPool()
		job.Key=item[0]
		job.LocalPath=item[1]
		log.LogDebugf("recive job is %v", job)
		s.toDownloadChans[rand.Intn(numChans)] <- job
	}

	return nil
}

func (s *MagicNode) HandleDownloadRequestForCubeFuse(body io.Reader) error {
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}

	var arr []string
	err = json.Unmarshal(data, &arr)
	if err != nil {
		return err
	}
	for _, item := range arr {
		job:=GetDownLoadJobFromPool()
		job.LocalPath=item
		log.LogDebugf("recive job is %v", job)
		s.toDownloadChans[rand.Intn(numChans)] <- job
	}

	return nil
}

const (
	CubeFSKeyPreifx = "cubefs://"
)

func (s *MagicNode)putToCleanCh(job *DownloadJob) {
	select {
		case s.toCleanCh <-job:
			return
	default:
		return
	}
}

func (s *MagicNode)getJobFromCleanCh()(job *DownloadJob) {
	select {
	case job=<-s.toCleanCh:
		return job
	default:
		return
	}
}

func (s *MagicNode) DownloadFileForFuse( job *DownloadJob)  {
	data,err:=ioutil.ReadFile(job.LocalPath)
	if err!=nil {
		job.err=err
		return
	}
	job.Size = uint32(len(data))
	job.DownTime =time.Now().Unix()
	atomic.AddUint64(&s.cacheSize, uint64(job.Size))

	return
}



func (s *MagicNode) DownloadFileForS3(job *DownloadJob)  {
	if !strings.HasPrefix(job.Key, CubeFSKeyPreifx) {
		job.err=fmt.Errorf("unavali key(%v)", job.Key)
		return
	}
	key := job.Key[len(CubeFSKeyPreifx):]
	parts := strings.SplitN(key, "/", 2)
	bucket := parts[0]
	fileName := parts[1]
	resp, err := s.client.Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileName),
	})
	if err != nil {
		job.err=err
		return
	}
	defer resp.Body.Close()
	file, err := os.Create(job.LocalPath + ".tmp")
	if err != nil {
		job.err=err
		return
	}
	defer file.Close()
	var size int64
	if size, err = io.Copy(file, resp.Body); err != nil {
		job.err=err
		return
	}
	err=os.Rename(job.LocalPath+".tmp", job.LocalPath)
	if err!=nil {
		os.RemoveAll(job.LocalPath+".tmp")
		job.err=err
		return
	}
	job.Size = uint32(size)
	job.DownTime =time.Now().Unix()
	atomic.AddUint64(&s.cacheSize, uint64(job.Size))
	s.putToCleanCh(job)


	return
}
