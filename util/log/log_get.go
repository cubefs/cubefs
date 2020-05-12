// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package log

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const (
	InvalidLogLevel   = "Invalid log level, only support [error, warn, debug, info, read, update, critical]"
	OpenLogFileFailed = "Failed to open log file"
	GetLogNumFailed   = "Failed to get param num"
	TailLogFileFailed = "Failed to tail log file"
	InvaildLogNum     = ", invalid num param, use default num"
	TooBigNum         = ", param num is too big, use default max num"
	LossNum           = ", can't find num param, use default num"

	GetLogPath = "/log/get"

	buffSize       = int64(4096)
	maxLogLine     = 10000
	defaultLogLine = 100
)

type logView struct {
	logLevel  string
	getLogNum int
	logText   []string
}

// HTTPReply uniform response structure
type HTTPReply struct {
	Code int32       `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

func GetLog(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	levelStr := query.Get("level")
	var fileName string

	switch strings.ToLower(levelStr) {
	case "error":
		fileName = gLog.errorLogger.object.fileName
	case "warn":
		fileName = gLog.warnLogger.object.fileName
	case "debug":
		fileName = gLog.debugLogger.object.fileName
	case "info":
		fileName = gLog.infoLogger.object.fileName
	case "read":
		fileName = gLog.readLogger.object.fileName
	case "update":
		fileName = gLog.updateLogger.object.fileName
	case "critical":
		fileName = gLog.criticalLogger.object.fileName
	default:
		buildFailureResp(w, http.StatusBadRequest, InvalidLogLevel)
		return
	}

	file, err := os.Open(fileName)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("%s, err is [%v]", OpenLogFileFailed, err))
		return
	}
	defer file.Close()

	var msg string
	var num int
	numStr := query.Get("num")
	if numStr == "" {
		num = defaultLogLine
		msg = fmt.Sprintf("%s(%d)", LossNum, defaultLogLine)
	} else {
		num, err = strconv.Atoi(numStr)
		if err != nil {
			buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("%s, err is [%v]", GetLogNumFailed, err))
			return
		}
	}

	if num <= 0 {
		num = defaultLogLine
		msg = fmt.Sprintf("%s(%d)", InvaildLogNum, defaultLogLine)
	} else if num > maxLogLine {
		num = maxLogLine
		msg = fmt.Sprintf("%s(%d)", TooBigNum, maxLogLine)
	}

	data, err := tailn(num, file)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("%s, err is [%v]", TailLogFileFailed, err))
		return
	}

	sendOKReply(w, r, msg, data)

	return
}

func tailn(line int, file *os.File) (data []string, err error) {
	fileLen, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return
	}

	var dataLen int
	var currNum int
	var lastStr string
	data = make([]string, line)
	for {
		currSize := buffSize
		if currSize > fileLen {
			currSize = fileLen
		}

		_, err = file.Seek(-currSize, os.SEEK_CUR)
		if err != nil {
			return
		}

		buff := make([]byte, currSize)
		dataLen, err = file.Read(buff)
		if err != nil {
			return
		}

		last := dataLen
		for i := dataLen - 1; i >= 0; i-- {
			if buff[i] == '\n' {
				if i == dataLen-1 {
					if lastStr != "" {
						data[line-currNum] = lastStr
						lastStr = ""
						currNum++
						if currNum >= line {
							return
						}
					}
					last = i
					continue
				}

				currNum++
				data[line-currNum] = string(buff[i+1:last]) + lastStr
				lastStr = ""
				if currNum >= line {
					return
				}
				last = i
			}
		}
		lastStr = string(buff[:last])

		fileLen, err = file.Seek(-currSize, os.SEEK_CUR)

		if fileLen <= 0 {
			break
		}
	}

	if currNum < line {
		data = data[line-currNum:]
	}

	return
}

func sendOKReply(w http.ResponseWriter, r *http.Request, msg string, data interface{}) {
	reply := &HTTPReply{
		Code: http.StatusOK,
		Msg:  "Success" + msg,
		Data: data,
	}

	httpReply, err := json.Marshal(reply)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("%s, err is [%v]", "", err))
		return
	}

	send(w, r, httpReply)

	return
}

func send(w http.ResponseWriter, r *http.Request, reply []byte) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		return
	}
	return
}
