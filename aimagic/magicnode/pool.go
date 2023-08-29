package main

import (
	"math/rand"
	"sync"
	"time"
)

const (
	JobPoolCnt =128
)

var (
	msgPool [JobPoolCnt]*sync.Pool
)

func init() {
	rand.Seed(time.Now().UnixNano())
	for index:=0;index< JobPoolCnt;index++{
		msgPool[index]=&sync.Pool{New: func()(interface{}) {
			return new(DownloadJob)
		}}
	}
}


func GetDownLoadJobFromPool() (job *DownloadJob) {
	index:=rand.Intn(JobPoolCnt)
	job = msgPool[index].Get().(*DownloadJob)
	job.DownTime=0
	job.Key=""
	job.LocalPath=""
	job.Size=0
	job.err=nil
	return job
}

func PutDownLoadJobToPool(job *DownloadJob) {
	index:=rand.Intn(JobPoolCnt)
	msgPool[index].Put(job)
	return
}
