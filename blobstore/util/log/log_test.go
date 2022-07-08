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
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

var (
	httpAddr     string
	logLevelPath string
)

func init() {
	path, handler := ChangeDefaultLevelHandler()
	mux := http.NewServeMux()
	mux.HandleFunc(path, handler)
	server := httptest.NewServer(mux)
	httpAddr = server.URL
	logLevelPath = path
}

func TestLoggerUnmarshal(t *testing.T) {
	type withLogLevel struct {
		Level Level  `json:"level" yaml:"level"`
		Other string `json:"other" yaml:"other"`
	}

	for _, cs := range []struct {
		lvlName  string
		hasError bool
		level    Level
	}{
		{"", true, -1},
		{"0", false, Ldebug},
		{"2", false, Lwarn},
		{"-1", false, Level(-1)},
		{`"1"`, true, -1},
		{"-1x", true, -1},
		{`"info"`, false, Linfo},
		{`"InFo"`, false, Linfo},
		{`"InfoX"`, true, -1},
	} {
		str := `{"other":"foo bar", "level": ` + cs.lvlName + `}`
		var data withLogLevel
		err := json.Unmarshal([]byte(str), &data)
		if cs.hasError {
			require.Error(t, err, cs.lvlName)
		} else {
			require.NoError(t, err, cs.lvlName)
			require.Equal(t, cs.level, data.Level, cs.lvlName)
		}
	}

	for _, cs := range []struct {
		lvlName  string
		hasError bool
		level    Level
	}{
		{"", false, Ldebug},
		{"0", true, -1},
		{"2", true, -1},
		{"info", false, Linfo},
		{"InFo", false, Linfo},
		{"InfoX", true, -1},
	} {
		str := `level: ` + cs.lvlName
		var data withLogLevel
		err := yaml.Unmarshal([]byte(str), &data)
		if cs.hasError {
			require.Error(t, err, cs.lvlName)
		} else {
			require.NoError(t, err, cs.lvlName)
			require.Equal(t, cs.level, data.Level, cs.lvlName)
		}
	}
}

func TestLoggerStd(t *testing.T) {
	SetOutputLevel(Ldebug)
	require.Equal(t, Ldebug, GetOutputLevel())
	Debugf("test logger debugf %s", "blobstore")
	Debug("test logger debug ", "blobstore")

	SetOutputLevel(Linfo)
	require.Equal(t, Linfo, GetOutputLevel())
	Printf("test logger printf %s", "blobstore")
	Println("test logger println ", "blobstore")
	Infof("test logger infof %s", "blobstore")
	Info("test logger info ", "blobstore")
	Debugf("test logger debugf %s", "blobstore")

	SetOutputLevel(Lwarn)
	require.Equal(t, Lwarn, GetOutputLevel())
	Warnf("test logger warnf %s", "blobstore")
	Warn("test logger warn ", "blobstore")
	Debugf("test logger debugf %s", "blobstore")

	SetOutputLevel(Lerror)
	require.Equal(t, Lerror, GetOutputLevel())
	Errorf("test logger errorf %s", "blobstore")
	Error("test logger error ", "blobstore")
	Debugf("test logger debugf %s", "blobstore")

	SetOutputLevel(Lpanic)
	require.Equal(t, Lpanic, GetOutputLevel())
	require.Panics(t, func() { Panicf("test logger panicf %s", "blobstore") })
	require.Panics(t, func() { Panic("test logger panic ", "blobstore") })
}

func TestLoggerFile(t *testing.T) {
	tmpFile := os.TempDir() + "/" + strconv.Itoa(rand.Intn(100000))
	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o755)
	require.NoError(t, err)
	defer os.Remove(tmpFile)
	defer f.Close()

	SetOutput(f)
	defer SetOutput(os.Stderr)

	TestLoggerStd(t)
}

func TestLoggerExit(t *testing.T) {
	if os.Getenv("LOGGER_EXIT") == "1" {
		Fatalf("test logger fatalf %s", "blobstore")
		Fatal("test logger fatal %s", "blobstore")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestLoggerExit")
	cmd.Env = append(os.Environ(), "LOGGER_EXIT=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("ran with err %v, want exit status 1", err)
}

func TestLoggerSetLevelHandler(t *testing.T) {
	addr := httpAddr + logLevelPath
	{
		resp, err := http.Get(addr)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
	}
	{
		resp, err := http.Head(addr)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 405, resp.StatusCode)
	}
	{
		resp, err := http.PostForm(addr, url.Values{"level": []string{"nan"}})
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)

		resp, err = http.PostForm(addr, url.Values{"level": []string{"-1"}})
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)

		resp, err = http.PostForm(addr, url.Values{"level": []string{"100"}})
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 400, resp.StatusCode)
	}
	{
		resp, err := http.PostForm(addr, url.Values{"level": []string{"0"}})
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
		require.Equal(t, Ldebug, GetOutputLevel())

		resp, err = http.PostForm(addr, url.Values{"level": []string{"3"}})
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, Lerror, GetOutputLevel())
	}
}
