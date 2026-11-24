// Copyright 2022 The CubeFS Authors.
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

package largefile

type LogCloser interface {
	Close() error
	Log([]byte) error
}

// BufferOwned is a line buffer whose ownership is transferred to the logger:
// Buffer returns the line bytes for the logger to consume, Release hands the
// buffer back to its owner and is called by the logger exactly once, after
// the line is written or dropped.
type BufferOwned interface {
	Buffer() []byte
	Release()
}

// OwnedLogCloser is a LogCloser that also accepts owned buffers, for callers
// that pool their line buffers: LogOwned takes the line instead of copying
// it, use the plain Log if the line cannot be given up. Both
// OpenLargeFileLog and OpenLossyAsyncLog return it, so callers can hold one
// interface type and call LogOwned without a type assertion.
type OwnedLogCloser interface {
	LogCloser
	LogOwned(line BufferOwned) error
}

type largeLogger struct {
	l LargeFile
}

func OpenLargeFileLog(cfg Config, rotateNew bool) (r OwnedLogCloser, err error) {
	return openLargeFileLog(cfg, rotateNew, true)
}

func openLargeFileLog(cfg Config, rotateNew bool, asyncWrapper bool) (r OwnedLogCloser, err error) {
	var f LargeFile
	if asyncWrapper {
		f, err = OpenLargeFile(cfg)
	} else {
		f, err = openLargeFile(cfg)
	}
	if err != nil {
		return
	}
	if rotateNew {
		if err = f.Rotate(); err != nil {
			return
		}
	}
	return &largeLogger{l: f}, nil
}

func (r *largeLogger) Log(msg []byte) error { return r.writeLine(msg) }

// LogOwned writes the line and hands the buffer back before returning: the
// synchronous writer has consumed the bytes once Write returns.
func (r *largeLogger) LogOwned(line BufferOwned) error {
	err := r.writeLine(line.Buffer())
	line.Release()
	return err
}

// writeLine terminates the line with a newline. An empty line is written as a
// bare newline, so callers may pass a nil or empty buffer.
func (r *largeLogger) writeLine(msg []byte) error {
	switch {
	case len(msg) == 0:
		msg = []byte{'\n'}
	case msg[len(msg)-1] != '\n':
		msg = append(msg, '\n')
	}
	_, err := r.l.Write(msg)
	return err
}

func (r *largeLogger) Close() (err error) {
	return r.l.Close()
}
