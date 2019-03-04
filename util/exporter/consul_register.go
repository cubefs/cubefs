// Copyright 2018 The Chubao Authors.
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
package exporter

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/chubaofs/cfs/util/log"
	"github.com/parnurzeal/gorequest"
)

const (
	RegisterPeriod = time.Duration(1) * time.Minute
	RegisterPath   = "/v1/agent/service/register"
)

/**
 * consul register info for prometheus
 * optional for user when set prometheus exporter
 */
type ConsulRegisterInfo struct {
	Name    string
	ID      string
	Address string
	Port    int64
	Tags    []string
}

// get consul id
func GetConsulId(app string, role string, host string, port int64) string {
	return fmt.Sprintf("%s_%s_%s_%d", app, role, host, port)
}

func RegisterConsul(addr, app, role, cluster string, port int64) {
	if len(addr) <= 0 {
		return
	}
	log.LogInfo("consul register enable ", addr)
	ticker := time.NewTicker(RegisterPeriod)
	defer func() {
		if err := recover(); err != nil {
			ticker.Stop()
			log.LogErrorf("RegisterConsul panic,err[%v]", err)
		}
	}()

	go func() {
		for {
			select {
			case <-ticker.C:
				SendRegisterReq(addr, app, role, cluster, port)
			}
		}
	}()
}

// GetLocalIpAddr returns the local IP address.
func GetLocalIpAddr() (ipaddr string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("cannot get local ip")
}

// SendRegisterReq sends the register request.
func SendRegisterReq(addr string, app string, role string, cluster string, port int64) {
	host, err := GetLocalIpAddr()
	if err != nil {
		log.LogErrorf("get local ip error, %v", err.Error())
		return
	}
	id := GetConsulId(app, role, host, port)
	url := addr + RegisterPath
	resp, body, errs := gorequest.New().Put(url).SendMap(ConsulRegisterInfo{
		Name:    app,
		ID:      id,
		Address: host,
		Port:    port,
		Tags: []string{
			"app=" + app,
			"role=" + role,
			"cluster=" + cluster,
		},
	}).End()
	if errs != nil {
		log.LogErrorf("Error on register consul resp: %v, body: %v", body, resp)
	}
}
