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

	"github.com/cubefs/cubefs/util/fileutil"
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
		bufferC:          make(chan string, 100000),
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

// NOTE: the format of audit log in here
// common header:
// [PREFIX] CURRENT_TIME TIME_ZONE
func (a *Audit) formatCommonHeader() (str string) {
	curTime := time.Now()
	curTimeStr := curTime.Format("2006-01-02 15:04:05")
	timeZone, _ := curTime.Zone()
	str = fmt.Sprintf("%v %v", curTimeStr, timeZone)
	return
}

// NOTE: meta audit logs
// format for client:
// [COMMON HEADER] IP_ADDR HOSTNAME OP SRC DST(Rename) ERR LATENCY SRC_INODE DST_INODE(Rename)
// format for server(inode):
// [COMMON HEADER] CLIENT_ADDR VOLUME OP ("nil") FULL_PATH ERR LATENCY INODE FILE_SIZE(Trunc)
// format for server(dentry):
// [COMMON HEADER] CLIENT_ADDR VOLUME OP NAME FULL_PATH ERR LATENCY INODE PARENT_INODE
// format for server(transaction):
// [COMMON HEADER] CLIENT_ADDR VOLUME OP TX_ID ("nil") ERR LATENCY TM_ID (0)
func (a *Audit) formatMetaAudit(ipAddr, hostName, op, src, dst string, err error, latency int64, srcInode, dstInode uint64) (entry string) {
	var errStr string
	if err != nil {
		errStr = err.Error()
	} else {
		errStr = "nil"
	}
	latencyStr := strconv.FormatInt(latency, 10) + " us"
	srcInodeStr := strconv.FormatUint(srcInode, 10)
	dstInodeStr := strconv.FormatUint(dstInode, 10)

	entry = fmt.Sprintf("%v, %s, %s, %s, %s, %s, %s, %s, %s, %s",
		a.formatCommonHeader(), ipAddr, hostName, op, src, dst, errStr, latencyStr, srcInodeStr, dstInodeStr)
	return
}

func (a *Audit) LogClientOp(op, src, dst string, err error, latency int64, srcInode, dstInode uint64) {
	a.formatMetaLog(a.ipAddr, a.hostName, op, src, dst, err, latency, srcInode, dstInode)
}

func (a *Audit) LogDentryOp(clientAddr, volume, op, name, fullPath string, err error, latency int64, ino, parentIno uint64) {
	if fullPath == "" {
		fullPath = auditFullPathUnsupported
	}
	a.formatMetaLog(clientAddr, volume, op, name, fullPath, err, latency, ino, parentIno)
}

func (a *Audit) LogInodeOp(clientAddr, volume, op, fullPath string, err error, latency int64, ino uint64, fileSize uint64) {
	if fullPath == "" {
		fullPath = auditFullPathUnsupported
	}
	a.formatMetaLog(clientAddr, volume, op, "nil", fullPath, err, latency, ino, fileSize)
}

func (a *Audit) LogTxOp(clientAddr, volume, op, txId string, err error, latency int64) {
	a.formatMetaLog(clientAddr, volume, op, txId, "nil", err, latency, 0, 0)
}

func (a *Audit) formatMetaLog(ipAddr, hostName, op, src, dst string, err error, latency int64, srcInode, dstInode uint64) {
	if entry := a.formatMetaAudit(ipAddr, hostName, op, src, dst, err, latency, srcInode, dstInode); entry != "" {
		if a.prefix != nil {
			entry = fmt.Sprintf("%s%s", a.prefix.String(), entry)
		}
		a.AddLog(entry)
	}
}

// NOTE: master audit logs
func (a *Audit) formatMasterAudit(op, msg string, err error) (str string) {
	var errStr string
	if err != nil {
		errStr = err.Error()
	} else {
		errStr = "nil"
	}
	str = fmt.Sprintf("%v, %v, %v, ERR: %v", a.formatCommonHeader(), op, msg, errStr)
	return
}

func (a *Audit) formatMasterLog(op, msg string, err error) {
	if entry := a.formatMasterAudit(op, msg, err); entry != "" {
		if a.prefix != nil {
			entry = fmt.Sprintf("%s%s", a.prefix.String(), entry)
		}
		a.AddLog(entry)
	}
}

func (a *Audit) LogResetDpDecommission(status string, src string, disk string, dpId uint64, dst string) {
	status = fmt.Sprintf("Prev: %v", status)
	src = fmt.Sprintf("SrcAddr: %v", src)
	disk = fmt.Sprintf("SrcDisk: %v", disk)
	id := fmt.Sprintf("DpId: %v", dpId)
	dst = fmt.Sprintf("DstAddr: %v", dst)
	message := fmt.Sprintf("%v %v %v %v %v %v", status, "Next: Initial", src, disk, id, dst)
	a.formatMasterLog("RESET_DP_DECOMMISSION", message, nil)
}

func (a *Audit) LogChangeDpDecommission(oldStatus string, status string, src string, disk string, dpId uint64, dst string) {
	oldStatus = fmt.Sprintf("Prev: %v", oldStatus)
	status = fmt.Sprintf("Next: %v", status)
	src = fmt.Sprintf("SrcAddr: %v", src)
	disk = fmt.Sprintf("SrcDisk: %v", disk)
	id := fmt.Sprintf("DpId: %v", dpId)
	dst = fmt.Sprintf("DstAddr: %v", dst)
	message := fmt.Sprintf("%v %v %v %v %v %v", oldStatus, status, src, disk, id, dst)
	a.formatMasterLog("DP_DECOMMISSION_CHANGE", message, nil)
}

func (a *Audit) LogMigrationOp(clientAddr, volume, op, fullPath string, err error, latency int64, ino uint64, from, to uint32) {
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

	entry := fmt.Sprintf("%s %s, %s, %s, %s, %v, %v, %s, %s, from %v, to %v",
		curTimeStr, timeZone, clientAddr, volume, op, ino, fullPath, errStr, latencyStr, from, to)
	if a.prefix != nil {
		entry = fmt.Sprintf("%s%s", a.prefix.String(), entry)
	}
	a.AddLog(entry)
}

func (a *Audit) LogLcNodeOp(op, vol, name, path string, pid, inode, size, writeGen uint64, hasMek bool, from, to uint32, latency int64, err error) {
	var errStr string
	if err != nil {
		errStr = err.Error()
	} else {
		errStr = "nil"
	}
	curTime := time.Now()
	curTimeStr := curTime.Format("2006-01-02 15:04:05")
	timeZone, _ := curTime.Zone()
	latencyStr := strconv.FormatInt(latency, 10) + " ms"

	entry := fmt.Sprintf("%s %s, op: %s, vol: %s, %s, %s, pid: %v, ino: %v, size: %v, %v, %v, from: %v, to: %v, err: %v, %v",
		curTimeStr, timeZone, op, vol, name, path, pid, inode, size, writeGen, hasMek, from, to, errStr, latencyStr)
	if a.prefix != nil {
		entry = fmt.Sprintf("%s%s", a.prefix.String(), entry)
	}
	a.AddLog(entry)
}

func (a *Audit) ResetWriterBufferSize(size int) {
	a.resetWriterBuffC <- size
}

func (a *Audit) AddLog(content string) {
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

func LogMigrationOp(clientAddr, volume, op, fullPath string, err error, latency int64, ino uint64, from, to uint32) {
	gAdtMutex.RLock()
	defer gAdtMutex.RUnlock()
	if gAdt == nil {
		return
	}
	gAdt.LogMigrationOp(clientAddr, volume, op, fullPath, err, latency, ino, from, to)
}

func LogLcNodeOp(op, vol, name, path string, pid, inode, size, writeGen uint64, hasMek bool, from, to uint32, latency int64, err error) {
	gAdtMutex.RLock()
	defer gAdtMutex.RUnlock()
	if gAdt == nil {
		return
	}
	gAdt.LogLcNodeOp(op, vol, name, path, pid, inode, size, writeGen, hasMek, from, to, latency, err)
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
	gAdtMutex.RLock()
	defer gAdtMutex.RUnlock()
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
			// NOTE: shoule we cleanup bufferC?
			if a.writer != nil {
				a.writer.Flush()
			}
			if a.logFile != nil {
				a.logFile.Close()
			}
			a.stopC <- struct{}{}
			return
		case bufSize := <-a.resetWriterBuffC:
			a.writerBufSize = bufSize
			a.newWriterSize(bufSize)
		case aLog := <-a.bufferC:
			err := a.logAudit(aLog)
			if err != nil {
				log.LogErrorf("action[logAudit]: error occured during logging %v", err)
			}
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
	dentries, err := fileutil.ReadDir(a.logDir)
	if err != nil {
		log.LogErrorf("[removeLogFile] ReadDir failed, logDir: %s, err: %v", a.logDir, err)
		return
	}

	oldLogs := make([]string, 0)
	for _, dentry := range dentries {
		if strings.HasPrefix(dentry, Audit_Module) && strings.HasSuffix(dentry, ShiftedExtension) {
			oldLogs = append(oldLogs, dentry)
		}
	}

	if len(oldLogs) == 0 {
		return
	}

	fs, err := fileutil.Statfs(a.logDir)
	if err != nil {
		log.LogErrorf("[removeLogFile] Get fs stat failed, err: %v", err)
		return
	}
	diskSpaceLeft := int64(fs.Bavail * uint64(fs.Bsize))

	sort.Slice(oldLogs, func(i, j int) bool {
		return oldLogs[i] < oldLogs[j]
	})

	for len(oldLogs) != 0 && diskSpaceLeft < DefaultHeadRoom*1024*1024 {
		oldestFile := path.Join(a.logDir, oldLogs[0])
		fileInfo, err := os.Stat(oldestFile)
		if err != nil {
			log.LogErrorf("[removeLogFile] failed to stat file(%v), err(%v)", oldestFile, err)
			return
		}
		if !a.shouldDelete(fileInfo, diskSpaceLeft, Audit_Module) {
			log.LogDebugf("[removeLogFile] cannot delete oldest file(%v)", oldestFile)
			return
		}
		if err = os.Remove(oldestFile); err != nil && !os.IsNotExist(err) {
			log.LogErrorf("[removeLogFile] failed to remove file(%v), err(%v)", oldestFile, err)
			return
		}
		oldLogs = oldLogs[1:]
		stat := fileutil.ConvertStat(fileInfo)
		diskSpaceLeft += stat.Blocks * fileutil.StatBlockSize
	}
}

func (a *Audit) shouldDelete(info os.FileInfo, diskSpaceLeft int64, module string) bool {
	isOldAuditLogFile := info.Mode().IsRegular() && strings.HasSuffix(info.Name(), ShiftedExtension) && strings.HasPrefix(info.Name(), module)
	if diskSpaceLeft <= 0 {
		return isOldAuditLogFile
	}
	return time.Since(info.ModTime()) > MaxReservedDays && isOldAuditLogFile
}

// NOTE: Please call me in single-thread
func (a *Audit) Stop() {
	// NOTE: put a empty value to stop async coroutine
	a.stopC <- struct{}{}
	// NOTE: wait for coroutine stop
	<-a.stopC
	close(a.stopC)
}

func (a *Audit) logAudit(content string) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[logAudit]: failed to log, %v", err)
			log.LogErrorf("action[logAudit]: audit[%v]", content)
		}
	}()
	if err = a.shiftFiles(); err != nil {
		return
	}
	if a.writer == nil {
		err = fmt.Errorf("write is nil, logFileName: %s\n", a.logFileName)
		return
	}
	fmt.Fprintf(a.writer, "%s\n", content)
	if a.writerBufSize <= 0 {
		a.writer.Flush()
	}
	return
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
	safePattern := `^(/|\.{0,2}/|[a-zA-Z0-9_@.-]+\/)+([a-zA-Z0-9_@.-]+|\.{1,2})$`
	match, _ := regexp.MatchString(safePattern, filePath)
	return match
}

func LogMasterOp(op, msg string, err error) {
	gAdtMutex.RLock()
	defer gAdtMutex.RUnlock()
	if gAdt == nil {
		return
	}
	gAdt.formatMasterLog(op, msg, err)
}

func LogDataNodeOp(op, msg string, err error) {
	gAdtMutex.RLock()
	defer gAdtMutex.RUnlock()
	if gAdt == nil {
		return
	}
	gAdt.formatDataNodeLog(op, msg, err)
}

func (a *Audit) formatDataNodeLog(op, msg string, err error) {
	if entry := a.formatDataNodeAudit(op, msg, err); entry != "" {
		if a.prefix != nil {
			entry = fmt.Sprintf("%s%s", a.prefix.String(), entry)
		}
		a.AddLog(entry)
	}
}

func (a *Audit) formatDataNodeAudit(op, msg string, err error) (str string) {
	var errStr string
	if err != nil {
		errStr = err.Error()
	} else {
		errStr = "nil"
	}
	str = fmt.Sprintf("%v, %v, %v, ERR: %v", a.formatCommonHeader(), op, msg, errStr)
	return
}
