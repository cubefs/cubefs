package scheduler

import (
	"net/http"

	"github.com/cubefs/cubefs/blobstore/api/scheduler"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper"
	"github.com/cubefs/cubefs/console/backend/helper/httputils"
)

func LeaderStats(c *gin.Context, consulAddr string) (*scheduler.TasksStat, error) {
	resp, err := Request(c, http.MethodGet, consulAddr, scheduler.PathStatsLeader, "", nil, nil)
	if err != nil {
		return nil, err
	}
	output := &scheduler.TasksStat{}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func Stats(c *gin.Context, nodeAddr string) (*scheduler.TasksStat, error) {
	reqUrl := nodeAddr + scheduler.PathStats
	resp, err := httputils.DoRequestBlobstore(c, reqUrl, http.MethodGet, nil, nil)
	if err != nil {
		return nil, err
	}
	output := &scheduler.TasksStat{}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}

type DiskMigratingStatsInput struct {
	DiskId   uint32 `json:"disk_id"`
	TaskType string `json:"task_type"`
}

func DiskMigratingStats(c *gin.Context, consulAddr string, input *DiskMigratingStatsInput) (*scheduler.DiskMigratingStats, error) {
	resp, err := Request(c, http.MethodGet, consulAddr, scheduler.PathStatsDiskMigrating, helper.BuildUrlParams(input), nil, nil)
	if err != nil {
		return nil, err
	}
	output := &scheduler.DiskMigratingStats{}
	_, err = httputils.HandleResponse(c, resp, err, output)
	if err != nil {
		return nil, err
	}
	return output, nil
}
