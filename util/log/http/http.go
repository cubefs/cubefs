package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	APIPathSetLogLevel   = "/loglevel/set"
	APIPathSetLogMaxSize = "/logSize/set"
	APIPathGetLogConfig  = "/logConfig/get"
)

var (
	ErrIllegalLogLevel = errors.New("illegal log level")
)

func SetLogLevelHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
	)
	if err = r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	levelStr := r.FormValue("level")
	var level log.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = log.DebugLevel
	case "info", "read", "write":
		level = log.InfoLevel
	case "warn":
		level = log.WarnLevel
	case "error":
		level = log.ErrorLevel
	case "critical":
		level = log.CriticalLevel
	case "fatal":
		level = log.FatalLevel
	default:
		err = fmt.Errorf("level only can be set :debug,info,warn,error,critical,read,write,fatal")
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	log.SetLogLevel(level)
	buildSuccessResp(w, "set log level success")
}

func SetLogSizeHandler(w http.ResponseWriter, r *http.Request) {
	var (
		size int64
		err  error
	)
	if err = r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	sizeStr := r.FormValue("size")
	if size, err = strconv.ParseInt(sizeStr, 10, 64); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if err = log.SetLogMaxSize(size); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	buildSuccessResp(w, fmt.Sprintf("set max log size [%v]MB", size))
}

func GetLogConfigHandler(w http.ResponseWriter, r *http.Request) {
	var msg string
	log.GetLogConfig(func(level log.Level, headRoom, rollingSize, maxUseSize int64) {
		msg = fmt.Sprintf("log config: level[%v], headRoom [%v]MB, rollingSize [%v]MB, maxUse [%v]MB",
			level, headRoom, rollingSize/1024/1024, maxUseSize)
	})
	buildSuccessResp(w, msg)
}

func buildSuccessResp(w http.ResponseWriter, data interface{}) {
	buildJSONResp(w, http.StatusOK, data, "")
}

func buildFailureResp(w http.ResponseWriter, code int, msg string) {
	buildJSONResp(w, code, nil, msg)
}

// Create response for the API request.
func buildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
		Msg  string      `json:"msg"`
	}{
		Code: code,
		Data: data,
		Msg:  msg,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	_, _ = w.Write(jsonBody)
}

func init() {
	http.HandleFunc(APIPathSetLogLevel, SetLogLevelHandler)
	http.HandleFunc(APIPathSetLogMaxSize, SetLogSizeHandler)
	http.HandleFunc(APIPathGetLogConfig, GetLogConfigHandler)
}
