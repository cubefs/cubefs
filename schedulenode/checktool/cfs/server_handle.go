package cfs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func registerChubaoFSHighLoadNodeSolver(s *ChubaoFSHighLoadNodeSolver) {
	if s != nil {
		http.HandleFunc("/checkToolHighLoadNodeSolver", s.highLoadNodeSolverHandle)
		http.HandleFunc("/checkToolHighLoadNodeSolverCritical", s.highLoadNodeSolverCriticalHandle)
	}
}

func registerChubaoFSDPReleaserServer(dpReleaser *ChubaoFSDPReleaser) {
	if dpReleaser != nil {
		http.HandleFunc("/getChubaoFSDPReleaser", dpReleaser.getChubaoFSDPReleaser)
		http.HandleFunc("/setChubaoFSDPReleaser", dpReleaser.setChubaoFSDPReleaser)
	}
}

func parseEnable(r *http.Request) (enable bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue("enable"); value == "" {
		err = fmt.Errorf("ParaEnableNotFound")
		return
	}
	if enable, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func BuildSuccessResp(w http.ResponseWriter, data interface{}) {
	BuildJSONResp(w, http.StatusOK, data, "")
}

func BuildFailureResp(w http.ResponseWriter, code int, msg string) {
	BuildJSONResp(w, code, nil, msg)
}

func BuildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
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
	w.Write(jsonBody)
}

func BuildJSONRespWithDiffCode(w http.ResponseWriter, httpCode, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(httpCode)
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
	w.Write(jsonBody)
}
