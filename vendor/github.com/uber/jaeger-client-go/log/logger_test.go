// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogger(t *testing.T) {
	bbLogger := &BytesBufferLogger{}
	for _, logger := range []DebugLogger{StdLogger, NullLogger, bbLogger} {
		logger.Infof("Hi %s", "there")
		logger.Error("Bad wolf")
		logger.Debugf("wolf")
	}
	assert.Equal(t, "INFO: Hi there\nERROR: Bad wolf\nDEBUG: wolf\n", bbLogger.String())
	bbLogger.Flush()
	assert.Empty(t, bbLogger.String())
}

func TestDebugLogAdapter_ReturnSameIfDebugLogger(t *testing.T) {
	assert.Same(t, NullLogger, DebugLogAdapter(NullLogger))
}

func TestDebugLogAdapter_HandleNil(t *testing.T) {
	assert.Nil(t, DebugLogAdapter(nil))
}

type mockLogger struct {
	errorCalled bool
	infoCalled  bool
	msg         string
	args        []interface{}
}

func (i *mockLogger) Error(msg string) {
	i.errorCalled = true
	i.msg = msg
}

func (i *mockLogger) Infof(msg string, args ...interface{}) {
	i.infoCalled = true
	i.msg = msg
	i.args = args
}

func (i *mockLogger) Reset() {
	i.errorCalled = false
	i.infoCalled = false
	i.msg = ""
	i.args = []interface{}{}
}

func TestDebugLogAdapter_adapt(t *testing.T) {
	assert.IsType(t, debugDisabledLogAdapter{}, DebugLogAdapter(&mockLogger{}))
}

func TestDebugLogAdapter_delegation(t *testing.T) {
	infoErrorLogger := &mockLogger{}
	adapted := DebugLogAdapter(infoErrorLogger)
	// DebugLogAdapter logs that debug logging is disabled, so we reset the mock
	infoErrorLogger.Reset()

	adapted.Debugf("msg", "arg1")
	assert.False(t, infoErrorLogger.infoCalled)
	assert.False(t, infoErrorLogger.errorCalled)

	adapted.Infof("Bodo", "Proudfoot")
	assert.True(t, infoErrorLogger.infoCalled)
	assert.Equal(t, "Bodo", infoErrorLogger.msg)
	assert.Equal(t, "Proudfoot", infoErrorLogger.args[0])
	infoErrorLogger.Reset()

	adapted.Error("error")
	assert.True(t, infoErrorLogger.errorCalled)
	assert.Equal(t, "error", infoErrorLogger.msg)
	infoErrorLogger.Reset()

}
