// Copyright 2023 The CubeFS Authors.
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

package dial

import (
	"encoding/json"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type (
	serviceConfig struct {
		cmd.Config

		DialSetting     Setting      `json:"dial_setting"`
		DialConnections []Connection `json:"dial_connections"`
	}
	serviceDial struct {
		closer.Closer
		config serviceConfig
	}

	// Setting dial setting
	Setting struct {
		IntervalS    int  `json:"interval_s"`
		Size         int  `json:"size"`
		N            int  `json:"n"`
		Sequentially bool `json:"sequentially"`
	}

	// Connection dial connection
	Connection struct {
		Setting

		Region     string   `json:"region"`
		Cluster    string   `json:"cluster"`
		IDC        string   `json:"idc"`
		ConsulAddr string   `json:"consul_addr"`
		HostAddrs  []string `json:"host_addrs"`

		api access.API `json:"-"`
	}
)

func (c *serviceConfig) defaulter() {
	defaulter.LessOrEqual(&c.DialSetting.IntervalS, 5*60*1000)
	defaulter.LessOrEqual(&c.DialSetting.Size, 1<<20)
	defaulter.LessOrEqual(&c.DialSetting.N, 10)
	for idx := range c.DialConnections {
		defaulter.LessOrEqual(&c.DialConnections[idx].Setting.IntervalS, c.DialSetting.IntervalS)
		defaulter.LessOrEqual(&c.DialConnections[idx].Setting.Size, c.DialSetting.Size)
		defaulter.LessOrEqual(&c.DialConnections[idx].Setting.N, c.DialSetting.N)
	}
}

func newService(cfg serviceConfig) *serviceDial {
	cfg.defaulter()
	val, _ := json.MarshalIndent(cfg, "    ", "  ")
	log.Infof("load config:\n%s", string(val))
	return &serviceDial{
		Closer: closer.New(),
		config: cfg,
	}
}

func (s *serviceDial) Status(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())
	span.Info("hello.")
	c.Respond()
}

func (s *serviceDial) Start() error {
	conns := make([]Connection, 0, len(s.config.DialConnections))
	for _, conn := range s.config.DialConnections {
		cfg := access.Config{
			ConnMode: access.GeneralConnMode,
			Consul:   access.ConsulConfig{Address: conn.ConsulAddr},
			LogLevel: s.config.LogConf.Level,
		}
		if conn.ConsulAddr == "" {
			cfg.PriorityAddrs = conn.HostAddrs[:]
		}

		api, err := access.New(cfg)
		if err != nil {
			log.Error(err)
			return err
		}

		conn.api = api
		conns = append(conns, conn)
	}

	for _, conn := range conns {
		log.Infof("start %+v", conn)
		s.StartConnection(conn)
	}
	return nil
}

func (s *serviceDial) StartConnection(conn Connection) {
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(conn.IntervalS))
		defer ticker.Stop()
		for {
			select {
			case <-s.Done():
				return
			case <-ticker.C:
				runConnection(conn)
			}
		}
	}()
}
