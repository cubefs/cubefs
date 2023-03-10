package auditlog

import "C"
import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
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

var DefaultTimeOutUs = [3]uint32{100000, 500000, 1000000}

var re = regexp.MustCompile(`\([0-9]*\)`)

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
	ap := ""
	for _, p := range a.prefixes {
		ap = ap + p + ", "
	}
	return ap
}

type Audit struct {
	volName          string
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

var gAdt *Audit = nil
var AdtMutex sync.RWMutex
var once sync.Once

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

func ResetWriterBuffSize(w http.ResponseWriter, r *http.Request) {
	var (
		err error
	)
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

func EnableAuditLog(w http.ResponseWriter, r *http.Request) {
	var (
		err error
	)
	if err = r.ParseForm(); err != nil {
		BuildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	logPath := r.FormValue("path")
	if logPath == "" {
		err = fmt.Errorf("path cannot be empty")
		BuildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	prefix := r.FormValue("prefix")
	if prefix == "" {
		err = fmt.Errorf("prefix cannot be empty")
		BuildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	logSize := 0

	if logSizeStr := r.FormValue("logsize"); logSizeStr != "" {
		val, err := strconv.Atoi(logSizeStr)
		if err != nil {
			err = fmt.Errorf("logSize error")
			BuildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		logSize = val
	} else {
		logSize = DefaultAuditLogSize
	}

	err, dir, logModule, logMaxSize := GetAuditLogInfo()
	if err != nil {
		_, err = InitAudit(logPath, prefix, int64(logSize))
		if err != nil {
			err = fmt.Errorf("Init audit log fail: %v\n", err)
			BuildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		info := fmt.Sprintf("audit log is initialized with params: logDir(%v) logModule(%v) logMaxSize(%v)",
			logPath, prefix, logSize)
		BuildSuccessResp(w, info)
	} else {
		info := fmt.Sprintf("audit log is already initialized with params: logDir(%v) logModule(%v) logMaxSize(%v)",
			dir, logModule, logMaxSize)
		BuildSuccessResp(w, info)
	}

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

func GetAuditLogInfo() (err error, dir, logModule string, logMaxSize int64) {
	AdtMutex.RLock()
	defer AdtMutex.RUnlock()
	if gAdt != nil {
		return nil, gAdt.logDir, gAdt.logModule, gAdt.logMaxSize
	} else {
		return errors.New("audit log is not initialized yet"), "", "", 0
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
	AdtMutex.Lock()
	defer AdtMutex.Unlock()

	if gAdt != nil {
		return gAdt, nil
	}
	if dir == "" || logModule == "" || logMaxSize <= 0 {
		errInfo := fmt.Sprintf("InitAudit failed with params: dir(%v) logModule(%v) logMaxSize(%v)",
			dir, logModule, logMaxSize)
		return nil, errors.New(errInfo)
	}
	host, ip := getAddr()
	dir = path.Join(dir, logModule)
	fi, err := os.Stat(dir)
	if err != nil {
		os.MkdirAll(dir, 0755)
	} else {
		if !fi.IsDir() {
			return nil, errors.New(dir + " is not a directory")
		}
	}
	_ = os.Chmod(dir, 0766)
	logName := path.Join(dir, Audit_Module) + ".log"
	audit := &Audit{
		hostName:         host,
		ipAddr:           ip,
		logDir:           dir,
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
	gAdt = audit
	go audit.flushAuditLog()
	return audit, nil
}

func formatAuditEntry(op, src, dst string, err error, latency int64, srcInode, dstInode uint64) (entry string) {
	AdtMutex.RLock()
	if gAdt == nil {
		AdtMutex.RUnlock()
		return ""
	}
	ipAddr := gAdt.ipAddr
	hostName := gAdt.hostName
	AdtMutex.RUnlock()

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

func FormatLog(op, src, dst string, err error, latency int64, srcInode, dstInode uint64) {
	if entry := formatAuditEntry(op, src, dst, err, latency, srcInode, dstInode); entry != "" {
		if gAdt.prefix != nil {
			entry = fmt.Sprintf("%s%s", gAdt.prefix.String(), entry)
		}
		AddLog(entry)
	}

}

func ResetWriterBufferSize(size int) {
	AdtMutex.Lock()
	defer AdtMutex.Unlock()
	if gAdt == nil {
		return
	}
	gAdt.resetWriterBuffC <- size
}

func AddLog(content string) {
	AdtMutex.Lock()
	defer AdtMutex.Unlock()
	if gAdt == nil {
		return
	}
	select {
	case gAdt.bufferC <- content:
		return
	default:
		log.LogErrorf("Async audit log failed, audit:[%s]", content)
	}
}

func StopAudit() {
	AdtMutex.Lock()
	defer AdtMutex.Unlock()
	if gAdt == nil {
		return
	}
	gAdt.stop()
	gAdt = nil
}

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
		logFile, err := os.OpenFile(a.logFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.LogErrorf("newWriterSize failed, logFileName: %s, err: %v\n", a.logFileName, err)
			return fmt.Errorf("OpenLogFile failed, logFileName %s\n", a.logFileName)
		}

		a.logFile = logFile
		if size <= 0 {
			log.LogErrorf("newWriterSize : buffer for logFileName is disabled")
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

func (a *Audit) stop() {
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

		if os.Rename(a.logFileName, logNewFileName) != nil {
			log.LogErrorf("RenameFile failed, logFileName: %s, logNewFileName: %s, err: %v\n",
				a.logFileName, logNewFileName, err)
			return fmt.Errorf("RenameFile failed, logFileName %s, logNewFileName %s\n",
				a.logFileName, logNewFileName)
		}
	}

	return a.newWriterSize(a.writerBufSize)
}
