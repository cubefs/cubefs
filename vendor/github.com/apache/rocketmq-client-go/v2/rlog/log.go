/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package rlog

import (
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/sirupsen/logrus"
)

const (
	LogKeyProducerGroup    = "producerGroup"
	LogKeyConsumerGroup    = "consumerGroup"
	LogKeyTopic            = "topic"
	LogKeyMessageQueue     = "MessageQueue"
	LogKeyUnderlayError    = "underlayError"
	LogKeyBroker           = "broker"
	LogKeyValueChangedFrom = "changedFrom"
	LogKeyValueChangedTo   = "changeTo"
	LogKeyPullRequest      = "PullRequest"
	LogKeyTimeStamp        = "timestamp"
)

type Logger interface {
	Debug(msg string, fields map[string]interface{})
	Info(msg string, fields map[string]interface{})
	Warning(msg string, fields map[string]interface{})
	Error(msg string, fields map[string]interface{})
	Fatal(msg string, fields map[string]interface{})
	Level(level string)
	OutputPath(path string) (err error)
}

func init() {
	r := &defaultLogger{
		logger: logrus.New(),
	}
	level := os.Getenv("ROCKETMQ_GO_LOG_LEVEL")
	switch strings.ToLower(level) {
	case "debug":
		r.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		r.logger.SetLevel(logrus.WarnLevel)
	case "error":
		r.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		r.logger.SetLevel(logrus.FatalLevel)
	default:
		r.logger.SetLevel(logrus.InfoLevel)
	}
	rLog = r
}

var rLog Logger

type defaultLogger struct {
	logger *logrus.Logger
}

func (l *defaultLogger) Debug(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Debug(msg)
}

func (l *defaultLogger) Info(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Info(msg)
}

func (l *defaultLogger) Warning(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Warning(msg)
}

func (l *defaultLogger) Error(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Error(msg)
}

func (l *defaultLogger) Fatal(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	l.logger.WithFields(fields).Fatal(msg)
}

func (l *defaultLogger) Level(level string) {
	switch strings.ToLower(level) {
	case "debug":
		l.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		l.logger.SetLevel(logrus.WarnLevel)
	case "error":
		l.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		l.logger.SetLevel(logrus.FatalLevel)
	default:
		l.logger.SetLevel(logrus.InfoLevel)
	}
}

type Config struct {
	OutputPath    string
	MaxFileSizeMB int
	MaxBackups    int
	MaxAges       int
	Compress      bool
	LocalTime     bool
}

func (c *Config) Logger() *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename:   filepath.ToSlash(c.OutputPath),
		MaxSize:    c.MaxFileSizeMB, // MB
		MaxBackups: c.MaxBackups,
		MaxAge:     c.MaxAges,  // days
		Compress:   c.Compress, // disabled by default
		LocalTime:  c.LocalTime,
	}
}

const defaultLogPath = "/tmp/rocketmq-client.log"

func defaultConfig() Config {
	return Config{
		OutputPath:    defaultLogPath,
		MaxFileSizeMB: 10,
		MaxBackups:    5,
		MaxAges:       3,
		Compress:      false,
		LocalTime:     true,
	}
}

func (l *defaultLogger) Config(conf Config) (err error) {
	l.logger.Out = conf.Logger()
	return
}

func (l *defaultLogger) OutputPath(path string) (err error) {
	config := defaultConfig()
	config.OutputPath = path

	l.logger.Out = config.Logger()
	return
}

// SetLogger use specified logger user customized, in general, we suggest user to replace the default logger with specified
func SetLogger(logger Logger) {
	rLog = logger
}

func SetLogLevel(level string) {
	if level == "" {
		return
	}
	rLog.Level(level)
}

func SetOutputPath(path string) (err error) {
	if "" == path {
		return
	}

	return rLog.OutputPath(path)
}

func Debug(msg string, fields map[string]interface{}) {
	rLog.Debug(msg, fields)
}

func Info(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	rLog.Info(msg, fields)
}

func Warning(msg string, fields map[string]interface{}) {
	if msg == "" && len(fields) == 0 {
		return
	}
	rLog.Warning(msg, fields)
}

func Error(msg string, fields map[string]interface{}) {
	rLog.Error(msg, fields)
}

func Fatal(msg string, fields map[string]interface{}) {
	rLog.Fatal(msg, fields)
}
