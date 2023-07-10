package rebalance

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
)

func (rw *ReBalanceWorker) handleStart(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	cluster := req.URL.Query().Get(ParamCluster)
	zoneName := req.URL.Query().Get(ParamZoneName)
	highRatioStr := req.URL.Query().Get(ParamHighRatio)
	lowRatioStr := req.URL.Query().Get(ParamLowRatio)
	goalRatioStr := req.URL.Query().Get(ParamGoalRatio)
	maxBatchCountStr := req.URL.Query().Get(ParamClusterMaxBatchCount)
	migrateLimitPerDiskStr := req.URL.Query().Get(ParamMigrateLimitPerDisk)
	highRatio, err := strconv.ParseFloat(highRatioStr, 64)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	lowRatio, err := strconv.ParseFloat(lowRatioStr, 64)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	goalRatio, err := strconv.ParseFloat(goalRatioStr, 64)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	maxBatchCount := -1
	if maxBatchCountStr != "" {
		if tmp, err := strconv.Atoi(maxBatchCountStr); err == nil {
			maxBatchCount = tmp
		}
	}
	migrateLimitPerDisk := -1
	if migrateLimitPerDiskStr != "" {
		if tmp, err := strconv.Atoi(migrateLimitPerDiskStr); err == nil {
			migrateLimitPerDisk = tmp
		}
	}

	err = rw.ReBalanceStart(cluster, zoneName, highRatio, lowRatio, goalRatio, maxBatchCount, migrateLimitPerDisk)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	return http.StatusOK, nil, nil
}

func (rw *ReBalanceWorker) handleStop(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	cluster := req.URL.Query().Get(ParamCluster)
	zoneName := req.URL.Query().Get(ParamZoneName)
	err := rw.ReBalanceStop(cluster, zoneName)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	return http.StatusOK, nil, nil
}

func (rw *ReBalanceWorker) handleStatus(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	cluster := req.URL.Query().Get(ParamCluster)
	zoneName := req.URL.Query().Get(ParamZoneName)
	status, err := rw.ReBalanceStatus(cluster, zoneName)
	if err != nil {
		return http.StatusBadRequest, -1, err
	}
	switch status {
	case StatusStop:
		return http.StatusOK, "Stop", nil
	case StatusRunning:
		return http.StatusOK, "Running", nil
	case StatusTerminating:
		return http.StatusOK, "Terminating", nil
	default:
		return http.StatusInternalServerError, status, fmt.Errorf("wrong Status with status id %v", status)
	}
}

func (rw *ReBalanceWorker) handleReset(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	rw.ResetZoneMap()
	return http.StatusOK, nil, nil
}

type handler func(w http.ResponseWriter, req *http.Request) (status int, data interface{}, err error)

func responseHandler(h handler) http.HandlerFunc {
	type response struct {
		Code    int
		Message string
		Data    interface{}
	}
	return func(w http.ResponseWriter, req *http.Request) {
		status, data, err := h(w, req)
		if err != nil {
			log.LogWarn(err.Error())
			w.Write([]byte(err.Error()))
			w.WriteHeader(status)
			return
		}
		resp := response{
			Code:    2000000,
			Message: "success",
			Data:    data,
		}
		// 响应结果处理
		err = json.NewEncoder(w).Encode(resp)
		if err != nil {
			log.LogWarn(err.Error())
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
