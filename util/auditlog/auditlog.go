// Copyright 2023 The CubeFS Authors.
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

package auditlog

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	Audit_Module           = "audit"
	FileNameDateFormat     = "20060102150405"
	ShiftedExtension       = ".old"
	DefaultAuditLogBufSize = 0

	F_OK                 = 0
	DefaultCleanInterval = 1 * time.Hour
	DefaultAuditLogSize  = 200 * 1024 * 1024 // 200M
	DefaultHeadRoom      = 50 * 1024         // 50G
	MaxReservedDays      = 7 * 24 * time.Hour
)

const (
	EnableAuditLogReqPath     = "/auditlog/enable"
	DisableAuditLogReqPath    = "/auditlog/disable"
	SetAuditLogBufSizeReqPath = "/auditlog/setbufsize"
)

const auditFullPathUnsupported = "(Audit full path unsupported)"

var DefaultTimeOutUs = [3]uint32{100000, 500000, 1000000}

type ShiftedFile []os.FileInfo

func (f ShiftedFile) Less(i, j int) bool {
	return f[i].ModTime().Before(f[j].ModTime())
}

func (f ShiftedFile) Len() int {
	return len(f)
}

func (f ShiftedFile) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

//type typeInfo struct {
//	typeName  string
//	allCount  uint32
//	failCount uint32
//	maxTime   time.Duration
//	minTime   time.Duration
//	allTimeUs time.Duration
//	timeOut   [MaxTimeoutLevel]uint32
//}

type AuditPrefix struct {
	prefixes []string
}

func NewAuditPrefix(p ...string) *AuditPrefix {
	return &AuditPrefix{
		prefixes: p,
	}
}

func (a *AuditPrefix) String() string {
	builder := strings.Builder{}
	for _, p := range a.prefixes {
		builder.WriteString(p)
		builder.WriteString(", ")
	}
	return builder.String()
}

type Audit struct {
	hostName         string
	ipAddr           string
	logDir           string
	logModule        string
	logMaxSize       int64
	logFileName      string
	logFile          *os.File
	writer           *bufio.Writer
	writerBufSize    int
	prefix           *AuditPrefix
	bufferC          chan string
	stopC            chan struct{}
	resetWriterBuffC chan int
	pid              int
	lock             sync.Mutex
}

var (
	gAdt      *Audit = nil
	gAdtMutex sync.RWMutex
)

func getAddr() (HostName, IPAddr string) {
	hostName, err := os.Hostname()
	if err != nil {
		HostName = "Unknown"
		log.LogWarnf("Get host name failed, replaced by unknown. err(%v)", err)
	} else {
		HostName = hostName
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		IPAddr = "Unknown"
		log.LogWarnf("Get ip address failed, replaced by unknown. err(%v)", err)
	} else {
		var ip_addrs []string
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				ip_addrs = append(ip_addrs, ipnet.IP.String())
			}
		}
		if len(ip_addrs) > 0 {
			IPAddr = strings.Join(ip_addrs, ",")
		} else {
			IPAddr = "Unknown"
			log.LogWarnf("Get ip address failed, replaced by unknown. err(%v)", err)
		}
	}
	return
}

// NOTE: for client http apis
func ResetWriterBuffSize(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = r.ParseForm(); err != nil {
		BuildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	size := int(DefaultAuditLogBufSize)
	if sizeStr := r.FormValue("size"); sizeStr != "" {
		val, err := strconv.Atoi(sizeStr)
		if err != nil {
			err = fmt.Errorf("size error")
			BuildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		size = val
	}

	ResetWriterBufferSize(size)
	BuildSuccessResp(w, "set audit log buffer size success")
}

func DisableAuditLog(w http.ResponseWriter, r *http.Request) {
	StopAudit()
	BuildSuccessResp(w, "disable audit log success")
}

func BuildSuccessResp(w http.ResponseWriter, data interface{}) {
	buildJSONResp(w, http.StatusOK, data, "")
}

func BuildFailureResp(w http.ResponseWriter, code int, msg string) {
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
	w.Write(jsonBody)
}

func (a *Audit) GetInfo() (dir, logModule string, logMaxSize int64) {
	return a.logDir, a.logModule, a.logMaxSize
}

func NewAuditWithPrefix(dir, logModule string, logMaxSize int64, prefix *AuditPrefix) (a *Audit, err error) {
	a, err = NewAudit(dir, logModule, logMaxSize)
	if err != nil {
		return nil, err
	}
	a.prefix = prefix
	return a, nil
}

func NewAudit(dir, logModule string, logMaxSize int64) (*Audit, error) {
	absPath, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	host, ip := getAddr()
	absPath = path.Join(absPath, logModule)
	if !isPathSafe(absPath) {
		return nil, errors.New("invalid file path")
	}
	fi, err := os.Stat(absPath)
	if err != nil {
		os.MkdirAll(absPath, 0o755)
	} else {
		if !fi.IsDir() {
			return nil, errors.New(absPath + " is not a directory")
		}
	}
	_ = os.Chmod(absPath, 0o755)
	logName := path.Join(absPath, Audit_Module) + ".log"
	audit := &Audit{
		hostName:         host,
		ipAddr:           ip,
		logDir:           absPath,
		logModule:        logModule,
		logMaxSize:       logMaxSize,
		logFileName:      logName,
		writerBufSize:    DefaultAuditLogBufSize,
		bufferC:          make(chan string, 1000),
		prefix:           nil,
		stopC:            make(chan struct{}),
		resetWriterBuffC: make(chan int),
		pid:              os.Getpid(),
	}
	err = audit.newWriterSize(audit.writerBufSize)
	if err != nil {
		return nil, err
	}
	go audit.flushAuditLog()
	return audit, nil
}

// NOTE:
// common header:
// [PREFIX] CURRENT_TIME TIME_ZONE
// format for client:
// [COMMON HEADER] IP_ADDR HOSTNAME OP SRC DST(Rename) ERR LATENCY SRC_INODE DST_INODE(Rename)
// format for server(inode):
// [COMMON HEADER] CLIENT_ADDR VOLUME OP ("nil") FULL_PATH ERR LATENCY INODE FILE_SIZE(Trunc)
// format for server(dentry):
// [COMMON HEADER] CLIENT_ADDR VOLUME OP NAME FULL_PATH ERR LATENCY INODE PARENT_INODE
// format for server(transaction):
// [COMMON HEADER] CLIENT_ADDR VOLUME OP TX_ID ("nil") ERR LATENCY TM_ID (0)
func (a *Audit) formatAuditEntry(ipAddr, hostName, op, src, dst string, err error, latency int64, srcInode, dstInode uint64) (entry string) {
	var errStr string
	if err != nil {
		errStr = err.Error()
	} else {
		errStr = "nil"
	}
	curTime := time.Now()
	curTimeStr := curTime.Format("2006-01-02 15:04:05")
	timeZone, _ := curTime.Zone()
	latencyStr := strconv.FormatInt(latency, 10) + " us"
	srcInodeStr := strconv.FormatUint(srcInode, 10)
	dstInodeStr := strconv.FormatUint(dstInode, 10)

	entry = fmt.Sprintf("%s %s, %s, %s, %s, %s, %s, %s, %s, %s, %s",
		curTimeStr, timeZone, ipAddr, hostName, op, src, dst, errStr, latencyStr, srcInodeStr, dstInodeStr)
	return
}

func (a *Audit) LogClientOp(op, src, dst string, err error, latency int64, srcInode, dstInode uint64) {
	a.formatLog(a.ipAddr, a.hostName, op, src, dst, err, latency, srcInode, dstInode)
}

func (a *Audit) LogDentryOp(clientAddr, volume, op, name, fullPath string, err error, latency int64, ino, parentIno uint64) {
	if fullPath == "" {
		fullPath = auditFullPathUnsupported
	}
	a.formatLog(clientAddr, volume, op, name, fullPath, err, latency, ino, parentIno)
}

func (a *Audit) LogInodeOp(clientAddr, volume, op, fullPath string, err error, latency int64, ino uint64, fileSize uint64) {
	if fullPath == "" {
		fullPath = auditFullPathUnsupported
	}
	a.formatLog(clientAddr, volume, op, "nil", fullPath, err, latency, ino, fileSize)
}

func (a *Audit) LogTxOp(clientAddr, volume, op, txId string, err error, latency int64) {
	a.formatLog(clientAddr, volume, op, txId, "nil", err, latency, 0, 0)
}

func (a *Audit) formatLog(ipAddr, hostName, op, src, dst string, err error, latency int64, srcInode, dstInode uint64) {
	if entry := a.formatAuditEntry(ipAddr, hostName, op, src, dst, err, latency, srcInode, dstInode); entry != "" {
		if a.prefix != nil {
			entry = fmt.Sprintf("%s%s", a.prefix.String(), entry)
		}
		a.AddLog(entry)
	}
}

func (a *Audit) ResetWriterBufferSize(size int) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.resetWriterBuffC <- size
}

func (a *Audit) AddLog(content string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	select {
	case a.bufferC <- content:
		return
	default:
		log.LogErrorf("async audit log failed, audit:[%s]", content)
	}
}

// NOTE: global functions

func GetAuditLogInfo() (dir, logModule string, logMaxSize int64, err error) {
	gAdtMutex.RLock()
	defer gAdtMutex.RUnlock()
	if gAdt != nil {
		dir, logModule, logMaxSize = gAdt.GetInfo()
		return
	} else {
		return "", "", 0, errors.New("audit log is not initialized yet")
	}
}

func InitAuditWithPrefix(dir, logModule string, logMaxSize int64, prefix *AuditPrefix) (a *Audit, err error) {
	a, err = InitAudit(dir, logModule, logMaxSize)
	if err != nil {
		return nil, err
	}

	a.prefix = prefix
	return a, nil
}

func InitAudit(dir, logModule string, logMaxSize int64) (*Audit, error) {
	gAdtMutex.Lock()
	defer gAdtMutex.Unlock()
	if gAdt == nil {
		adt, err := NewAudit(dir, logModule, logMaxSize)
		if err != nil {
			return nil, err
		}
		gAdt = adt
	}
	return gAdt, nil
}

func LogClientOp(op, src, dst string, err error, latency int64, srcInode, dstInode uint64) {
	gAdtMutex.RLock()
	defer gAdtMutex.RUnlock()
	if gAdt == nil {
		return
	}
	gAdt.LogClientOp(op, src, dst, err, latency, srcInode, dstInode)
}

func LogDentryOp(clientAddr, volume, op, name, fullPath string, err error, latency int64, ino, parentIno uint64) {
	gAdtMutex.RLock()
	defer gAdtMutex.RUnlock()
	if gAdt == nil {
		return
	}
	gAdt.LogDentryOp(clientAddr, volume, op, name, fullPath, err, latency, ino, parentIno)
}

func LogInodeOp(clientAddr, volume, op, fullPath string, err error, latency int64, ino uint64, fileSize uint64) {
	gAdtMutex.RLock()
	defer gAdtMutex.RUnlock()
	if gAdt == nil {
		return
	}
	gAdt.LogInodeOp(clientAddr, volume, op, fullPath, err, latency, ino, fileSize)
}

func LogTxOp(clientAddr, volume, op, txId string, err error, latency int64) {
	gAdtMutex.RLock()
	defer gAdtMutex.RUnlock()
	if gAdt == nil {
		return
	}
	gAdt.LogTxOp(clientAddr, volume, op, txId, err, latency)
}

func ResetWriterBufferSize(size int) {
	gAdtMutex.Lock()
	defer gAdtMutex.Unlock()
	if gAdt == nil {
		return
	}
	gAdt.ResetWriterBufferSize(size)
}

func AddLog(content string) {
	gAdtMutex.Lock()
	defer gAdtMutex.Unlock()
	if gAdt == nil {
		return
	}
	gAdt.AddLog(content)
}

func StopAudit() {
	gAdtMutex.Lock()
	defer gAdtMutex.Unlock()
	if gAdt == nil {
		return
	}
	gAdt.Stop()
	gAdt = nil
}

// NOTE: implementation details

func (a *Audit) flushAuditLog() {
	cleanTimer := time.NewTimer(DefaultCleanInterval)

	for {
		select {
		case <-a.stopC:
			return
		case bufSize := <-a.resetWriterBuffC:
			a.writerBufSize = bufSize
			a.newWriterSize(bufSize)
		case aLog := <-a.bufferC:
			a.logAudit(aLog)
		case <-cleanTimer.C:
			a.removeLogFile()
			cleanTimer.Reset(DefaultCleanInterval)
		}
	}
}

func (a *Audit) newWriterSize(size int) error {
	a.writerBufSize = size
	if a.writer != nil {
		a.writer.Flush()
	}

	if a.logFile == nil {
		logFile, err := os.OpenFile(a.logFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o666)
		if err != nil {
			log.LogErrorf("newWriterSize failed, logFileName: %s, err: %v\n", a.logFileName, err)
			return fmt.Errorf("OpenLogFile failed, logFileName %s", a.logFileName)
		}

		a.logFile = logFile
		if size <= 0 {
			log.LogDebugf("newWriterSize : buffer for logFileName: %s is disabled", a.logFileName)
			a.writer = bufio.NewWriter(logFile)
		} else {
			a.writer = bufio.NewWriterSize(logFile, size)
		}

	} else {
		_, err := a.logFile.Stat()
		if err == nil {
			if size <= 0 {
				log.LogErrorf("newWriterSize : buffer for logFileName is disabled")
				a.writer = bufio.NewWriter(a.logFile)
			} else {
				a.writer = bufio.NewWriterSize(a.logFile, size)
			}
		} else {
			a.logFile.Close()
			a.logFile = nil
			return a.newWriterSize(size)
		}
	}
	return nil
}

func (a *Audit) removeLogFile() {
	fs := syscall.Statfs_t{}
	if err := syscall.Statfs(a.logDir, &fs); err != nil {
		log.LogErrorf("Get fs stat failed, err: %v", err)
		return
	}
	diskSpaceLeft := int64(fs.Bavail * uint64(fs.Bsize))
	diskSpaceLeft -= DefaultHeadRoom * 1024 * 1024

	fInfos, err := ioutil.ReadDir(a.logDir)
	if err != nil {
		log.LogErrorf("ReadDir failed, logDir: %s, err: %v", a.logDir, err)
		return
	}
	var needDelFiles ShiftedFile
	for _, info := range fInfos {
		if a.shouldDelete(info, diskSpaceLeft, Audit_Module) {
			needDelFiles = append(needDelFiles, info)
		}
	}
	sort.Sort(needDelFiles)
	for _, info := range needDelFiles {
		if err = os.Remove(path.Join(a.logDir, info.Name())); err != nil {
			log.LogErrorf("Remove log file failed, logFileName: %s, err: %v", info.Name(), err)
			continue
		}
		diskSpaceLeft += info.Size()
		if diskSpaceLeft > 0 && time.Since(info.ModTime()) < MaxReservedDays {
			break
		}
	}
}

func (a *Audit) shouldDelete(info os.FileInfo, diskSpaceLeft int64, module string) bool {
	isOldAuditLogFile := info.Mode().IsRegular() && strings.HasSuffix(info.Name(), ShiftedExtension) && strings.HasPrefix(info.Name(), module)
	if diskSpaceLeft <= 0 {
		return isOldAuditLogFile
	}
	return time.Since(info.ModTime()) > MaxReservedDays && isOldAuditLogFile
}

func (a *Audit) Stop() {
	a.lock.Lock()
	defer a.lock.Unlock()
	close(a.stopC)
	a.writer.Flush()
	a.logFile.Close()
}

func (a *Audit) logAudit(content string) error {
	a.shiftFiles()

	fmt.Fprintf(a.writer, "%s\n", content)
	if a.writerBufSize <= 0 {
		a.writer.Flush()
	}

	return nil
}

func (a *Audit) shiftFiles() error {
	fileInfo, err := os.Stat(a.logFileName)
	if err != nil {
		return err
	}

	if fileInfo.Size() < a.logMaxSize {
		return nil
	}

	if syscall.Access(a.logFileName, F_OK) == nil {
		logNewFileName := a.logFileName + "." + time.Now().Format(FileNameDateFormat) + ShiftedExtension

		a.writer.Flush()
		a.logFile.Close()
		a.writer = nil
		a.logFile = nil
		if err = os.Rename(a.logFileName, logNewFileName); err != nil {
			log.LogErrorf("RenameFile failed, logFileName: %s, logNewFileName: %s, err: %v\n",
				a.logFileName, logNewFileName, err)
			return fmt.Errorf("action[shiftFiles] renameFile failed, logFileName %s, logNewFileName %s",
				a.logFileName, logNewFileName)
		}
	}

	// NOTE: try to recycle space when shift file
	a.removeLogFile()

	return a.newWriterSize(a.writerBufSize)
}

func isPathSafe(filePath string) bool {
	safePattern := `^[a-zA-Z0-9\-_/]+$`
	match, _ := regexp.MatchString(safePattern, filePath)
	return match
}
