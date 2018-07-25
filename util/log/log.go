package log

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type Level uint8

const (
	DebugLevel  Level = 1
	InfoLevel         = DebugLevel<<1 + 1
	WarnLevel         = InfoLevel<<1 + 1
	ErrorLevel        = WarnLevel<<1 + 1
	FatalLevel        = ErrorLevel<<1 + 1
	ReadLevel         = InfoLevel
	UpdateLevel       = InfoLevel
)

const (
	FileNameDateFormat   = "2006-01-02"
	FileOpt              = os.O_RDWR | os.O_CREATE | os.O_APPEND
	WriterBufferInitSize = 1024 * 1024
	WriterBufferLenLimit = 4 * 1024 * 1024
)

var levelPrefixes = []string{
	"[DEBUG]",
	"[INFO.]",
	"[WARN.]",
	"[ERROR]",
	"[FATAL]",
	"[READ.]",
	"[WRITE]",
}

type flusher interface {
	Flush()
}

type asyncWriter struct {
	file   *os.File
	buffer *bytes.Buffer
	flushC chan bool
	closed bool
	mu     sync.Mutex
}

func (writer *asyncWriter) flushScheduler() {
	var (
		ticker *time.Ticker
	)
	ticker = time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			writer.flushToFile()
		case _, open := <-writer.flushC:
			if !open {
				ticker.Stop()
				return
			}
			writer.flushToFile()
		}
	}
}

func (writer *asyncWriter) Write(p []byte) (n int, err error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	n, err = writer.buffer.Write(p)
	if writer.buffer.Len() > WriterBufferLenLimit {
		select {
		case writer.flushC <- true:
		default:
		}
	}
	return
}

func (writer *asyncWriter) Close() (err error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.closed {
		return
	}
	close(writer.flushC)
	writer.file.Close()
	writer.closed = true
	return
}

func (writer *asyncWriter) Flush() {
	writer.flushToFile()
}

func (writer *asyncWriter) flushToFile() {
	writer.mu.Lock()
	data := writer.buffer.Bytes()
	writer.buffer.Reset()
	writer.mu.Unlock()
	writer.file.Write(data)
	writer.file.Sync()
}

func newAsyncWriter(out *os.File) *asyncWriter {
	w := &asyncWriter{
		file:   out,
		buffer: bytes.NewBuffer(make([]byte, 0, WriterBufferInitSize)),
		flushC: make(chan bool, 1),
	}
	go w.flushScheduler()
	return w
}

type closableLogger struct {
	*log.Logger
	closer io.Closer
}

func (c *closableLogger) SetOutput(w io.WriteCloser) {
	oldCloser := c.closer
	defer oldCloser.Close()
	c.closer = w
	c.Logger.SetOutput(w)
}

func (c *closableLogger) Flush() {
	if c.closer != nil {
		if flusher, is := c.closer.(flusher); is {
			flusher.Flush()
		}
	}
}

func newCloseableLogger(writer io.WriteCloser, prefix string, flag int) *closableLogger {
	return &closableLogger{
		Logger: log.New(writer, prefix, flag),
		closer: writer,
	}
}

type Log struct {
	dir          string
	module       string
	errorLogger  *closableLogger
	warnLogger   *closableLogger
	debugLogger  *closableLogger
	infoLogger   *closableLogger
	readLogger   *closableLogger
	updateLogger *closableLogger
	level        Level
	msgC         chan string
	startTime    time.Time
}

var (
	ErrLogFileName    = "_error.log"
	WarnLogFileName   = "_warn.log"
	InfoLogFileName   = "_info.log"
	DebugLogFileName  = "_debug.log"
	ReadLogFileName   = "_read.log"
	UpdateLogFileName = "_write.log"
)

var gLog *Log = nil

func NewLog(dir, module string, level Level) (*Log, error) {
	gLog = new(Log)
	gLog.dir = dir
	gLog.module = module
	fi, err := os.Stat(dir)
	if err != nil {
		os.MkdirAll(dir, 0755)
	} else {
		if !fi.IsDir() {
			return nil, errors.New(dir + " is not a directory")
		}
	}

	err = gLog.initLog(dir, module, level)
	if err != nil {
		return nil, err
	}
	gLog.startTime = time.Now()
	go gLog.checkLogRotation(dir, module)

	return gLog, nil
}

func (l *Log) initLog(logDir, module string, level Level) error {
	logOpt := log.LstdFlags | log.Lmicroseconds

	getNewLog := func(logFileName string) (newLogger *closableLogger, err error) {
		var (
			fp *os.File
		)
		if fp, err = os.OpenFile(path.Join(logDir, module+logFileName), FileOpt, 0666); err != nil {
			return
		}
		newLogger = newCloseableLogger(newAsyncWriter(fp), "", logOpt)
		return
	}
	var err error
	logHandles := [...]**closableLogger{&l.debugLogger, &l.infoLogger, &l.warnLogger, &l.errorLogger, &l.readLogger, &l.updateLogger}
	logNames := [...]string{DebugLogFileName, InfoLogFileName, WarnLogFileName, ErrLogFileName, ReadLogFileName, UpdateLogFileName}
	for i := range logHandles {
		if *logHandles[i], err = getNewLog(logNames[i]); err != nil {
			return err
		}
	}
	l.level = level
	return nil
}

func (l *Log) SetPrefix(s, level string) string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		line = 0
	}
	short := file
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			break
		}
	}
	file = short
	return level + " " + file + ":" + strconv.Itoa(line) + ": " + s
}

func (l *Log) Flush() {
	loggers := []*closableLogger{
		l.debugLogger,
		l.infoLogger,
		l.warnLogger,
		l.errorLogger,
		l.readLogger,
		l.updateLogger,
	}
	for _, logger := range loggers {
		if logger != nil {
			logger.Flush()
		}
	}
}

func LogWarn(v ...interface{}) {
	if gLog == nil {
		return
	}
	if WarnLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[2])
	gLog.warnLogger.Output(2, s)
}

func LogWarnf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if WarnLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[2])
	gLog.warnLogger.Output(2, s)
}

func LogInfo(v ...interface{}) {
	if gLog == nil {
		return
	}
	if InfoLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[1])
	gLog.infoLogger.Output(2, s)
}

func LogInfof(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if InfoLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[1])
	gLog.infoLogger.Output(2, s)
}

func LogError(v ...interface{}) {
	if gLog == nil {
		return
	}
	if ErrorLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[3])
	gLog.errorLogger.Output(2, s)
}

func LogErrorf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if ErrorLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[3])
	gLog.errorLogger.Print(s)
}

func LogDebug(v ...interface{}) {
	if gLog == nil {
		return
	}
	if DebugLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[0])
	gLog.debugLogger.Print(s)
}

func LogDebugf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if DebugLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[0])
	gLog.debugLogger.Output(2, s)
}

func LogFatal(v ...interface{}) {
	if gLog == nil {
		return
	}
	if FatalLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[4])
	gLog.errorLogger.Output(2, s)
	os.Exit(1)
}

func LogFatalf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if FatalLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[4])
	gLog.errorLogger.Output(2, s)
	os.Exit(1)
}

func LogRead(v ...interface{}) {
	if gLog == nil {
		return
	}
	if ReadLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[5])
	gLog.readLogger.Output(2, s)
}

func LogReadf(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if ReadLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[5])
	gLog.readLogger.Output(2, s)
}

func LogWrite(v ...interface{}) {
	if gLog == nil {
		return
	}
	if UpdateLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintln(v...)
	s = gLog.SetPrefix(s, levelPrefixes[6])
	gLog.updateLogger.Output(2, s)
}

func LogWritef(format string, v ...interface{}) {
	if gLog == nil {
		return
	}
	if UpdateLevel&gLog.level != gLog.level {
		return
	}
	s := fmt.Sprintf(format, v...)
	s = gLog.SetPrefix(s, levelPrefixes[6])
	gLog.updateLogger.Output(2, s)
}

func LogFlush() {
	if gLog != nil {
		gLog.Flush()
	}
}

func (l *Log) checkLogRotation(logDir, module string) {
	for {
		yesterday := time.Now().AddDate(0, 0, -1)
		_, err := os.Stat(logDir + "/" + module + ErrLogFileName + "." + yesterday.Format(FileNameDateFormat))
		if err == nil || time.Now().Day() == l.startTime.Day() {
			time.Sleep(time.Second * 600)
			continue
		}

		setLogRotation := func(logFileName string, setLog *closableLogger) (err error) {
			var (
				logFilePath string
				fp          *os.File
			)
			logFilePath = path.Join(logDir, module+logFileName)
			if err = os.Rename(logFilePath, logFilePath+"."+yesterday.Format(FileNameDateFormat)); err != nil {
				return
			}
			if fp, err = os.OpenFile(logFilePath, FileOpt, 0666); err != nil {
				return
			}
			setLog.SetOutput(newAsyncWriter(fp))
			return
		}

		// Rotate log files
		setLogRotation(DebugLogFileName, l.debugLogger)
		setLogRotation(InfoLogFileName, l.infoLogger)
		setLogRotation(WarnLogFileName, l.warnLogger)
		setLogRotation(ErrLogFileName, l.errorLogger)
		setLogRotation(ReadLogFileName, l.readLogger)
		setLogRotation(UpdateLogFileName, l.updateLogger)
	}
}
