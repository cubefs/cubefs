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

package log

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExtLogger(t *testing.T) {
	l := New(os.Stderr, 3)
	level := l.GetOutputLevel()
	require.Equal(t, Linfo, level)

	l.SetOutputLevel(Ldebug)
	l.Debugf("test logger debugf %s", "blobstore")
	l.Debug("test logger debug ", "blobstore")

	l.SetOutputLevel(Linfo)
	l.Printf("test logger printf %s", "blobstore")
	l.Println("test logger println ", "blobstore")
	l.Infof("test logger infof %s", "blobstore")
	l.Info("test logger info ", "blobstore")

	l.SetOutputLevel(Lwarn)
	l.Warnf("test logger warnf %s", "blobstore")
	l.Warn("test logger warn ", "blobstore")
	l.Warningf("test logger warningf %s", "blobstore")
	l.Warning("test logger warning ", "blobstore")

	l.SetOutputLevel(Lerror)
	l.Errorf("test logger errorf %s", "blobstore")
	l.Error("test logger error ", "blobstore")

	l.Output("it-a-id", Lerror, 1, "application message")

	require.Panics(t, func() {
		l.Panicf("should panic: %s", "panic information")
	})
	require.Panics(t, func() {
		l.Panic("should panic")
	})

	tmpFile := os.TempDir() + "/" + strconv.Itoa(rand.Intn(100000))
	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o755)
	require.NoError(t, err)
	defer os.Remove(tmpFile)
	defer f.Close()
	l.SetOutput(f)

	l.SetOutputLevel(Ldebug)
	l.Debugf("test logger debugf %s", "blobstore")
	l.Debug("test logger debug ", "blobstore")

	l.SetOutputLevel(Linfo)
	l.Infof("test logger infof %s", "blobstore")
	l.Info("test logger info ", "blobstore")

	l.SetOutputLevel(Lwarn)
	l.Warnf("test logger warnf %s", "blobstore")
	l.Warn("test logger warn ", "blobstore")
	l.Warningf("test logger warningf %s", "blobstore")
	l.Warning("test logger warning ", "blobstore")

	l.SetOutputLevel(Lerror)
	l.Errorf("test logger errorf %s", "blobstore")
	l.Error("test logger error ", "blobstore")

	l.Output("it-a-id", Lerror, 1, "application message")
}

func BenchmarkFormatOutput(b *testing.B) {
	l := &logger{}
	buf := new(bytes.Buffer)
	t := time.Now()
	buf.Write(make([]byte, 1<<14))

	b.Helper()
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		buf.Reset()
		l.formatOutput(buf, t, "file-", 10, Lerror)
	}
}

func BenchmarkFormatOutputLine(b *testing.B) {
	l := &logger{}
	buf := new(bytes.Buffer)
	t := time.Now()
	buf.Write(make([]byte, 1<<14))

	b.Helper()
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		buf.Reset()
		l.formatOutput(buf, t, "file-", 0, Lerror)
	}
}

func outputwith(b *testing.B, calldepth int) {
	l := New(ioutil.Discard, 1)
	b.Helper()
	for ii := 0; ii < b.N; ii++ {
		l.Output("id-xxx-id", Linfo, calldepth, "loggingggggggg")
	}
}

func BenchmarkOutputWith0(b *testing.B) { outputwith(b, 0) }
func BenchmarkOutputWith1(b *testing.B) { outputwith(b, 1) }
func BenchmarkOutputWith2(b *testing.B) { outputwith(b, 2) }
func BenchmarkOutputWith3(b *testing.B) { outputwith(b, 3) }
func BenchmarkOutputWith4(b *testing.B) { outputwith(b, 4) }
