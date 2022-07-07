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

package main

import (
	"net/http"
	"os"

	"github.com/cubefs/cubefs/blobstore/common/config"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
	"github.com/cubefs/cubefs/blobstore/common/rpc/example"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// Config service config
type Config struct {
	LogLevel log.Level       `json:"log_level"`
	AuditLog auditlog.Config `json:"auditlog"`

	App struct {
		BindAddr string            `json:"bind_addr"`
		App      example.AppConfig `json:"app"`
	} `json:"app"`
	File struct {
		BindAddr string `json:"bind_addr"`
	} `json:"file"`
	Meta struct {
		BindAddr string `json:"bind_addr"`
	} `json:"meta"`
}

func main() {
	config.Init("f", "", "main.conf")

	var conf Config
	err := config.Load(&conf)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutputLevel(conf.LogLevel)

	os.MkdirAll(conf.AuditLog.LogDir, 0o644)
	lh, lf, _ := auditlog.Open("RPC", &conf.AuditLog)
	defer lf.Close()

	{
		c := conf.Meta
		httpServer := &http.Server{
			Addr:    c.BindAddr,
			Handler: rpc.MiddlewareHandlerWith(example.NewMetaHandler(), lh),
		}

		log.Info("meta server is running at:", c.BindAddr)
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Panic("server exits:", err)
			}
		}()
	}
	{
		c := conf.File
		httpServer := &http.Server{
			Addr:    c.BindAddr,
			Handler: rpc.MiddlewareHandlerWith(example.NewFileHandler(), lh),
		}

		log.Info("file server is running at:", c.BindAddr)
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Panic("server exits:", err)
			}
		}()
	}
	{
		c := conf.App
		httpServer := &http.Server{
			Addr:    c.BindAddr,
			Handler: rpc.MiddlewareHandlerWith(example.NewAppHandler(c.App), lh),
		}

		log.Info("app server is running at:", c.BindAddr)
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Panic("server exits:", err)
			}
		}()
	}

	<-make(chan struct{})
}
