package log

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/util/errors"
)

const (
	APIPathSetLogLevel   = "/loglevel/set"
	APIPathSetLogMaxSize = "/logSize/set"
	APIPathGetLogConfig  = "/logConfig/get"
	APIPathGetLog        = "/log/get"
	APIPathGetLogList    = "/log/list"
)

const (
	InvaildGetLogParm   = "Filename and log level must specify one"
	InvalidLogLevel     = "Invalid log level, only support [error, warn, debug, info, read, update, critical]"
	OpenLogFileFailed   = "Failed to open log file"
	GetLogNumFailed     = "Failed to get param num"
	GetEntireParaFailed = "Can't recognized whether the whole log file is requested."
	GetEntireFileFailed = "Failed to get entire log file"
	TailLogFileFailed   = "Failed to tail log file"
	EmptyLogFile        = "log file is empty"
	InvaildLogNum       = ", invalid num param, use default num"
	TooBigNum           = ", param num is too big, use default max num"
	LossNum             = ", can't find num param, use default num"

	DefaultProfPort = 10090
	MaxProfPort     = 10110

	buffSize       = int64(4096)
	maxLogLine     = 10000
	defaultLogLine = 100
)

var (
	ErrIllegalLogLevel = errors.New("illegal log level")
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

func init() {
	http.HandleFunc(APIPathSetLogLevel, SetLogLevelHandler)
	http.HandleFunc(APIPathSetLogMaxSize, SetLogSizeHandler)
	http.HandleFunc(APIPathGetLogConfig, GetLogConfigHandler)
	http.HandleFunc(APIPathGetLogList, GetLogListHandler)
	http.HandleFunc(APIPathGetLog, GetLogHandler)
}

func SetLogLevelHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
	)
	if err = r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	levelStr := r.FormValue(levelKey)
	var level Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = DebugLevel
	case "info", "read", "write":
		level = InfoLevel
	case "warn":
		level = WarnLevel
	case "error":
		level = ErrorLevel
	case "critical":
		level = CriticalLevel
	case "fatal":
		level = FatalLevel
	default:
		err = fmt.Errorf("level only can be set :debug,info,warn,error,critical,read,write,fatal")
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	SetLogLevel(level)
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
	sizeStr := r.FormValue(sizeKey)
	if size, err = strconv.ParseInt(sizeStr, 10, 64); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if err = SetLogMaxSize(size); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	buildSuccessResp(w, fmt.Sprintf("set max log size [%v]MB", size))
}

func GetLogConfigHandler(w http.ResponseWriter, r *http.Request) {
	var cfg string
	GetLogConfig(func(level Level, headRoom, rollingSize, maxUseSize int64) {
		cfg = fmt.Sprintf("log config: level[%v], headRoom [%v]MB, rollingSize [%v]MB, maxUse [%v]MB",
			level, headRoom, rollingSize/1024/1024, maxUseSize)
	})
	buildSuccessResp(w, cfg)
}

func GetLogListHandler(w http.ResponseWriter, r *http.Request) {
	// gLog.dir == LogDir
	_, err := os.Stat(gLog.dir)
	if err != nil {
		var msg string
		if os.IsNotExist(err) {
			msg = fmt.Sprintf("log directory[%v] is not exist", gLog.dir)
		} else {
			msg = fmt.Sprintf("stat log directory[%v] failed, err[%v]", gLog.dir, err)
		}
		buildFailureResp(w, http.StatusInternalServerError, msg)
		return
	}

	logFiles, err := ioutil.ReadDir(gLog.dir)
	if err != nil {
		msg := fmt.Sprintf("Read log directory[%v] failed, err[%v]", gLog.dir, err)
		buildFailureResp(w, http.StatusInternalServerError, msg)
		return
	}
	fileList := make([]string, 0, len(logFiles))
	for _, file := range logFiles {
		fileList = append(fileList, file.Name())
	}

	sendOKReply(w, r, "", fileList)
	return
}

func GetLogHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	fileName := query.Get(fileKey)
	levelStr := query.Get(levelKey)

	if fileName != "" {
		fileName = path.Join(gLog.dir, fileName)
		goto openFile
	} else if levelStr != "" {
		var errStr string
		if fileName, errStr = getLogFileByLevel(levelStr); errStr != "" {
			buildFailureResp(w, http.StatusBadRequest, errStr)
			return
		}
		goto openFile
	} else {
		buildFailureResp(w, http.StatusBadRequest, InvaildGetLogParm)
		return
	}

openFile:
	file, err := os.Open(fileName)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("%s, err is [%v]", OpenLogFileFailed, err))
		return
	}
	defer file.Close()

	entire := 0
	entireStr := query.Get(entireKey)
	if entireStr != "" {
		if entire, err = strconv.Atoi(entireStr); err != nil {
			buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("%s, err is [%v]", GetEntireParaFailed, err))
			return
		}
	}
	if entire > 0 {
		readEntireFile(w, r, file)
	} else {
		numStr := query.Get(numKey)
		readTailNFile(w, r, file, numStr)
	}
	return
}

func getLogFileByLevel(levelStr string) (fileName string, errStr string) {
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
		return "", InvalidLogLevel
	}
	return
}

func readEntireFile(w http.ResponseWriter, r *http.Request, file *os.File) {
	var (
		fileInfo os.FileInfo
		err error
	)
	fileInfo, err = os.Stat(file.Name())
	if os.IsNotExist(err) {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("%s, err is [%v]", GetEntireFileFailed, err))
		return
	}
	if fileInfo.Size() == 0 {
		msg := fmt.Sprintf(": %v, " + EmptyLogFile, file.Name())
		sendOKReply(w, r, msg, "")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(fileInfo.Size(), 10))
	reader := bufio.NewReader(file)
	_, err = reader.WriteTo(w)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("%s, err is [%v]", GetEntireFileFailed, err))
	}
	return
	//var (
	//	readStr string
	//	err error
	//	size int64
	//)
	//
	//for {
	//	readStr, err = reader.ReadString('\n')
	//	size += int64(len(readStr))
	//	if err == io.EOF {
	//		return size, nil
	//	} else if err != nil {
	//		return size, nil
	//	}
	//	_, err = w.Write([]byte(readStr))
	//	if err != nil {
	//		return size, err
	//	}
	//}
}

func readTailNFile(w http.ResponseWriter, r *http.Request, file *os.File, numStr string) {
	var (
		msg string
		num int64
		err error
	)
	if numStr != "" {
		if num, err = strconv.ParseInt(numStr, 10, 64); err != nil {
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
}

func tailn(line int64, file *os.File) (data []string, err error) {
	fileLen, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return
	}

	var dataLen int
	var currNum int64
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
