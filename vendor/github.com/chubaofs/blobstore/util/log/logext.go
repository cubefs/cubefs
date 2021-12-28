package log

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type logger struct {
	level     int32
	calldepth int
	writer    atomic.Value
	pool      sync.Pool
}

type logWriter struct {
	io.Writer
}

// New return a logger with default level Linfo.
// output buffer with reused bytes pool.
func New(out io.Writer, calldepth int) Logger {
	l := &logger{
		level:     Linfo,
		calldepth: calldepth,
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
	l.writer.Store(&logWriter{out})
	return l
}

func (l *logger) Output(id string, lvl int, calldepth int, s string) error {
	if int32(lvl) < atomic.LoadInt32(&l.level) || lvl >= maxLevel {
		return nil
	}

	now := time.Now()
	buf := l.pool.Get().(*bytes.Buffer)

	_, file, line, ok := runtime.Caller(calldepth)
	if !ok {
		file = "???"
		line = 0
	}
	buf.Reset()
	l.formatOutput(buf, now, file, line, lvl)
	if id != "" {
		buf.WriteByte('[')
		buf.WriteString(id)
		buf.WriteByte(']')
		buf.WriteByte(' ')
	}
	buf.WriteString(s)
	if len(s) > 0 && s[len(s)-1] != '\n' {
		buf.WriteByte('\n')
	}
	out := l.writer.Load().(io.Writer)
	_, err := out.Write(buf.Bytes())
	l.pool.Put(buf)
	return err
}

// -----------------------------------------

func (l *logger) outputf(lvl int, format string, v []interface{}) {
	l.Output("", lvl, l.calldepth, fmt.Sprintf(format, v...))
}
func (l *logger) output(lvl int, v []interface{}) {
	l.Output("", lvl, l.calldepth, fmt.Sprintln(v...))
}

func (l *logger) Printf(format string, v ...interface{})   { l.outputf(Linfo, format, v) }
func (l *logger) Println(v ...interface{})                 { l.output(Linfo, v) }
func (l *logger) Debugf(format string, v ...interface{})   { l.outputf(Ldebug, format, v) }
func (l *logger) Debug(v ...interface{})                   { l.output(Ldebug, v) }
func (l *logger) Infof(format string, v ...interface{})    { l.outputf(Linfo, format, v) }
func (l *logger) Info(v ...interface{})                    { l.output(Linfo, v) }
func (l *logger) Warnf(format string, v ...interface{})    { l.outputf(Lwarn, format, v) }
func (l *logger) Warn(v ...interface{})                    { l.output(Lwarn, v) }
func (l *logger) Warningf(format string, v ...interface{}) { l.outputf(Lwarn, format, v) }
func (l *logger) Warning(v ...interface{})                 { l.output(Lwarn, v) }
func (l *logger) Errorf(format string, v ...interface{})   { l.outputf(Lerror, format, v) }
func (l *logger) Error(v ...interface{})                   { l.output(Lerror, v) }

func (l *logger) Fatalf(format string, v ...interface{}) {
	l.outputf(Lfatal, format, v)
	os.Exit(1)
}
func (l *logger) Fatal(v ...interface{}) {
	l.output(Lfatal, v)
	os.Exit(1)
}

func (l *logger) Panicf(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	l.outputf(Lpanic, format, v)
	//l.Output("", Lpanic, l.calldepth, s)
	panic(s)
}
func (l *logger) Panic(v ...interface{}) {
	s := fmt.Sprintln(v...)
	l.output(Lpanic, v)
	//l.Output("", Lpanic, l.calldepth, s)
	panic(s)
}

// -----------------------------------------

func (l *logger) GetOutputLevel() int {
	return int(atomic.LoadInt32(&l.level))
}
func (l *logger) SetOutput(w io.Writer) {
	l.writer.Store(&logWriter{w})
}
func (l *logger) SetOutputLevel(lvl int) {
	if lvl >= maxLevel {
		lvl = Lfatal
	}
	atomic.StoreInt32(&l.level, int32(lvl))
}

func (l *logger) formatOutput(buf *bytes.Buffer, t time.Time, file string, line int, lvl int) {
	year, month, day := t.Date()
	itoa(buf, year, 4)
	buf.WriteByte('/')
	itoa(buf, int(month), 2)
	buf.WriteByte('/')
	itoa(buf, day, 2)
	buf.WriteByte(' ')

	hour, min, sec := t.Clock()
	itoa(buf, hour, 2)
	buf.WriteByte(':')
	itoa(buf, min, 2)
	buf.WriteByte(':')
	itoa(buf, sec, 2)
	buf.WriteByte('.')
	itoa(buf, t.Nanosecond()/1e3, 6)
	buf.WriteByte(' ')

	buf.WriteString(levelToStrings[lvl])
	buf.WriteByte(' ')

	buf.WriteString(file)
	buf.WriteByte(':')
	itoa(buf, line, -1)
	buf.WriteByte(' ')
}

// itoa cheap integer to fixed width decimal ASCII.
// a negative width to avoid zero-padding.
// the buffer has enough capacity.
func itoa(buf *bytes.Buffer, i int, width int) {
	var u = uint(i)
	if u == 0 && width <= 1 {
		buf.WriteByte('0')
		return
	}

	// assemble decimal in reverse order
	var b [32]byte
	bp := len(b)
	for ; u > 0 || width > 0; u /= 10 {
		bp--
		width--
		b[bp] = byte(u%10) + '0'
	}

	// avoid slicing b to make an allocation
	buf.Write(b[bp:])
}
