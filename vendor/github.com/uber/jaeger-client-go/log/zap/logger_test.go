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

package zap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestLogger(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	logger := NewLogger(zap.New(core))

	logger.Debugf("test debug %d", 1)
	msg := logs.FilterMessage("test debug 1").TakeAll()
	assert.Len(t, msg, 1)
	assert.Equal(t, msg[0].Level, zap.DebugLevel)

	logger.Infof("test info %d", 2)
	msg = logs.FilterMessage("test info 2").TakeAll()
	assert.Len(t, msg, 1)
	assert.Equal(t, msg[0].Level, zap.InfoLevel)

	logger.Error("test error")
	msg = logs.FilterMessage("test error").TakeAll()
	assert.Len(t, msg, 1)
	assert.Equal(t, msg[0].Level, zap.ErrorLevel)
}
